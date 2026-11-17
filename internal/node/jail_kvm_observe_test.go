package node

// Root-side observation of jailed QEMUs for the A10 jailed acceptance
// (TestKVMJailAcceptance, TestKVMJailRestart): the durable jail record and
// VM manifest, /proc, the unit cgroup, the QMP peer and the cross-uid probe.
// Lead-owned root throwaway VM only.

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
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

	"golang.org/x/crypto/ssh"
	"golang.org/x/sys/unix"

	"github.com/nathabonfim59/bamboo-raft/internal/node/jailer"
	"github.com/nathabonfim59/bamboo-raft/internal/node/qemu"
	"github.com/nathabonfim59/bamboo-raft/internal/node/storage"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// jailKVMProbeEnv selects the cross-uid probe child (TestKVMJailProbeHelper).
const jailKVMProbeEnv = "BBRAFT_TEST_KVM_JAIL_PROBE"

// jailKVMReadSmall reads a small host file without following a final
// symlink, bounded to 1 MiB.
func jailKVMReadSmall(path string) ([]byte, error) {
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK|syscall.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	raw, err := io.ReadAll(io.LimitReader(f, 1<<20+1))
	if err == nil && len(raw) > 1<<20 {
		err = fmt.Errorf("%s exceeds 1 MiB", path)
	}
	return raw, err
}

func jailKVMLstatID(t *testing.T, path string) jailKVMIdentity {
	t.Helper()
	var st unix.Stat_t
	if err := unix.Lstat(path, &st); err != nil {
		t.Fatalf("lstat %s: %v", path, err)
	}
	return jailKVMIdentity{Dev: st.Dev, Ino: st.Ino}
}

// jailKVMExecFlags returns the rendered ExecStart's flags (each with its one
// value; jailKVMCheckExec already validated the shape).
func jailKVMExecFlags(argv []string) map[string]string {
	flags := map[string]string{}
	for i := 1; i < len(argv); i++ {
		if slices.Contains(jailKVMBoolFlags, argv[i]) || i+1 >= len(argv) {
			flags[argv[i]] = ""
			continue
		}
		flags[argv[i]] = argv[i+1]
		i++
	}
	return flags
}

// jailIO is the unit's rendered jail IO policy (device and rates).
func (a *jailKVMAttempt) jailIO(t *testing.T) jailer.IOLimits {
	t.Helper()
	f := jailKVMExecFlags(a.unit.ExecStart)
	majText, minText, ok := strings.Cut(f["-node.jail-io-device"], ":")
	maj, err1 := strconv.ParseUint(majText, 10, 32)
	minor, err2 := strconv.ParseUint(minText, 10, 32)
	if !ok || err1 != nil || err2 != nil {
		t.Fatalf("rendered -node.jail-io-device %q", f["-node.jail-io-device"])
	}
	rate := func(name string) uint64 {
		v, ok := f[name]
		if !ok {
			return 0
		}
		n, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			t.Fatalf("rendered %s %q: %v", name, v, err)
		}
		return n
	}
	return jailer.IOLimits{Device: jailer.BlockDevice{Major: uint32(maj), Minor: uint32(minor)},
		ReadBPS: rate("-node.jail-io-read-bps"), WriteBPS: rate("-node.jail-io-write-bps"),
		ReadIOPS: rate("-node.jail-io-read-iops"), WriteIOPS: rate("-node.jail-io-write-iops")}
}

// uidRange is the unit's rendered -node.jail-uid-range.
func (a *jailKVMAttempt) uidRange(t *testing.T) (uint32, uint32) {
	t.Helper()
	v := jailKVMExecFlags(a.unit.ExecStart)["-node.jail-uid-range"]
	loText, hiText, ok := strings.Cut(v, "-")
	lo, err1 := strconv.ParseUint(loText, 10, 32)
	hi, err2 := strconv.ParseUint(hiText, 10, 32)
	if !ok || err1 != nil || err2 != nil || lo == 0 || hi < lo {
		t.Fatalf("rendered -node.jail-uid-range %q", v)
	}
	return uint32(lo), uint32(hi)
}

// qemu observes one sandbox's jailed QEMU from its durable jail record
// (<data>/jailer/records/<id>.json), cross-checked with the VM manifest
// (vms/<id>/vm.json), this boot, /proc/<pid>/stat start ticks and the
// membership of exactly <unit>/vms/qemu-<id> (jailKVMCrossCheckQEMU). No
// process is picked from the cgroup listing: kernel vhost workers may live
// there too.
func (a *jailKVMAttempt) qemu(t *testing.T, sandbox string) jailKVMQEMU {
	t.Helper()
	data := a.rendered.DataDir
	// The durable record file is the jailer's {record, ownership}
	// envelope, decoded strictly before any /proc read.
	raw, err := jailKVMReadSmall(filepath.Join(data, "jailer", "records", sandbox+".json"))
	if err != nil {
		t.Fatalf("jail record of %s: %v", sandbox, err)
	}
	rec, err := jailKVMDecodeJailRecord(raw, sandbox)
	if err != nil {
		t.Fatal(err)
	}
	var meta storage.VMMeta
	raw, err = jailKVMReadSmall(filepath.Join(data, "vms", sandbox, "vm.json"))
	if err == nil {
		err = json.Unmarshal(raw, &meta)
	}
	if err != nil {
		t.Fatalf("vm manifest of %s: %v", sandbox, err)
	}
	boot, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		t.Fatal(err)
	}
	pid := strconv.Itoa(rec.PID)
	statRaw, err := os.ReadFile("/proc/" + pid + "/stat")
	if err != nil {
		t.Fatalf("jail record pid %s of %s: %v", pid, sandbox, err)
	}
	start, err := jailKVMStatStartTime(string(statRaw))
	if err != nil {
		t.Fatal(err)
	}
	cg, err := os.ReadFile("/proc/" + pid + "/cgroup")
	if err != nil {
		t.Fatal(err)
	}
	group := filepath.Join(a.cgroupDir(), "vms", "qemu-"+sandbox)
	procs, err := jailKVMReadSmall(filepath.Join(group, "cgroup.procs"))
	if err != nil {
		t.Fatal(err)
	}
	if err := jailKVMCrossCheckQEMU(rec, meta, strings.TrimSpace(string(boot)), start, string(cg), string(procs), a.rendered.Service, sandbox); err != nil {
		t.Fatal(err)
	}
	vmDir := filepath.Join(data, "vms", sandbox)
	q := jailKVMQEMU{
		Sandbox: sandbox, Attempt: rec.Attempt, UID: rec.UID, PID: rec.PID, StartTicks: rec.StartTicks,
		Cgroup: group, Overlay: filepath.Join(vmDir, "disk.qcow2"), ControlDir: filepath.Join(vmDir, storage.ControlDirName),
		JailRoot: filepath.Join(data, "jailer", "jails", sandbox, "root"), Resources: meta.Resources,
	}
	q.OverlayID, q.ControlDirI = jailKVMLstatID(t, q.Overlay), jailKVMLstatID(t, q.ControlDir)
	if q.ControlDirI != (jailKVMIdentity{Dev: rec.ControlDev, Ino: rec.ControlIno}) {
		t.Fatalf("control dir %s is %v, the jail record says %d:%d", q.ControlDir, q.ControlDirI, rec.ControlDev, rec.ControlIno)
	}
	return q
}

// jailKVMNamespaces reads /proc/<pid>/ns/{mnt,pid,ipc}.
func jailKVMNamespaces(pid int) map[string]string {
	out := map[string]string{}
	for _, ns := range []string{"mnt", "pid", "ipc"} {
		if v, err := os.Readlink(fmt.Sprintf("/proc/%d/ns/%s", pid, ns)); err == nil {
			out[ns] = v
		}
	}
	return out
}

// jailKVMProcFDs lists a process's descriptors (bounded) with the readlink
// and the stat through each.
func jailKVMProcFDs(pid int, limit int) ([]jailKVMFD, error) {
	dir := fmt.Sprintf("/proc/%d/fd", pid)
	d, err := os.Open(dir)
	if err != nil {
		return nil, err
	}
	defer d.Close()
	names, err := d.Readdirnames(limit + 1)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	if len(names) > limit {
		return nil, fmt.Errorf("%s lists more than %d descriptors", dir, limit)
	}
	var fds []jailKVMFD
	for _, name := range names {
		n, err := strconv.Atoi(name)
		if err != nil {
			return nil, fmt.Errorf("%s: %q", dir, name)
		}
		link, _ := os.Readlink(filepath.Join(dir, name))
		var st unix.Stat_t
		if err := unix.Stat(filepath.Join(dir, name), &st); err != nil {
			return nil, fmt.Errorf("%s/%s (%s): %w", dir, name, link, err)
		}
		fds = append(fds, jailKVMFD{Num: n, Link: link, ID: jailKVMIdentity{Dev: st.Dev, Ino: st.Ino}, Mode: st.Mode, Rdev: st.Rdev})
	}
	return fds, nil
}

// assertJailed proves one QEMU's confinement inside the actual unit: its
// credentials and seccomp (jailKVMCheckQEMUStatus), private mount, pid and
// ipc namespaces versus the node main, the recorded chroot with a
// read-only root mount, the staged executable inside it, the fixed
// descriptor roles and no setup/config/cgroup/data-root descriptor, the QMP
// peer (pid and uid by SO_PEERCRED; a bounded connect and getsockopt only,
// never a QMP exchange) and the exact cgroup limits. It re-proves the
// process identity (start ticks) last.
func (a *jailKVMAttempt) assertJailed(t *testing.T, q jailKVMQEMU) {
	t.Helper()
	pid := strconv.Itoa(q.PID)
	status, err := os.ReadFile("/proc/" + pid + "/status")
	if err != nil {
		t.Fatal(err)
	}
	lo, hi := a.uidRange(t)
	if err := jailKVMCheckQEMUStatus(string(status), q.UID, lo, hi); err != nil {
		t.Fatalf("%s: %v", q.Sandbox, err)
	}
	mainPID, err := strconv.Atoi(jailKVMShow(t, a.rendered.Service, "MainPID")["MainPID"])
	if err != nil || mainPID <= 0 {
		t.Fatalf("node main pid: %v", err)
	}
	if err := jailKVMCheckNamespaces(jailKVMNamespaces(q.PID), jailKVMNamespaces(mainPID)); err != nil {
		t.Fatalf("%s: %v", q.Sandbox, err)
	}
	var root unix.Stat_t
	if err := unix.Stat("/proc/"+pid+"/root", &root); err != nil {
		t.Fatal(err)
	}
	if got, want := (jailKVMIdentity{Dev: root.Dev, Ino: root.Ino}), jailKVMLstatID(t, q.JailRoot); got != want {
		t.Fatalf("%s: chroot is %v, want the recorded jail root %s %v", q.Sandbox, got, q.JailRoot, want)
	}
	mountinfo, err := os.ReadFile("/proc/" + pid + "/mountinfo")
	if err != nil {
		t.Fatal(err)
	}
	if err := jailKVMRootMountReadOnly(string(mountinfo)); err != nil {
		t.Fatalf("%s: %v", q.Sandbox, err)
	}
	var exe unix.Stat_t
	if err := unix.Stat("/proc/"+pid+"/exe", &exe); err != nil {
		t.Fatal(err)
	}
	binary := jailKVMExecFlags(a.unit.ExecStart)["-node.jail-qemu-binary"]
	if binary == "" {
		binary = "/usr/bin/qemu-system-x86_64"
	}
	if got, want := (jailKVMIdentity{Dev: exe.Dev, Ino: exe.Ino}), jailKVMLstatID(t, filepath.Join(q.JailRoot, binary)); got != want {
		t.Fatalf("%s: exe %v is not the staged %s %v", q.Sandbox, got, binary, want)
	}
	rdev := func(p string) uint64 {
		var st unix.Stat_t
		if err := unix.Stat(p, &st); err != nil {
			t.Fatal(err)
		}
		return st.Rdev
	}
	var cgroupfs unix.Stat_t
	if err := unix.Stat("/sys/fs/cgroup", &cgroupfs); err != nil {
		t.Fatal(err)
	}
	data, version := a.rendered.DataDir, filepath.Join(a.rendered.ImageDir, a.env.imageID)
	forbidden := map[jailKVMIdentity]string{}
	for p, what := range map[string]string{
		data: "data root", filepath.Join(data, "jailer"): "jailer state dir", filepath.Join(data, "jailer", "records"): "jail records dir",
		filepath.Join(data, "jailer", "jails"): "jails dir", filepath.Join(data, "vms", q.Sandbox): "VM dir",
		a.rendered.ConfigDir: "config dir", filepath.Join(a.rendered.ConfigDir, "node.token"): "node token",
		q.ControlDir: "control dir",
	} {
		forbidden[jailKVMLstatID(t, p)] = what
	}
	fds, err := jailKVMProcFDs(q.PID, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if err := jailKVMCheckFDs(fds, jailKVMFDPolicy{
		Rdev: map[int]uint64{qemu.TapFD: rdev("/dev/net/tun"), qemu.VhostFD: rdev("/dev/vhost-net"), qemu.VsockFD: rdev("/dev/vhost-vsock")},
		ID: map[int]jailKVMIdentity{
			qemu.KernelFD: jailKVMLstatID(t, filepath.Join(version, "kernel")), qemu.InitrdFD: jailKVMLstatID(t, filepath.Join(version, "initrd")),
			qemu.BaseDiskFD: jailKVMLstatID(t, filepath.Join(version, "root.qcow2")), qemu.OverlayFD: q.OverlayID,
		},
		ForbiddenIDs: forbidden, ForbiddenDev: cgroupfs.Dev, NullRdev: rdev("/dev/null"), MaxFDs: 1024,
	}); err != nil {
		t.Fatalf("%s: %v", q.Sandbox, err)
	}
	if err := jailKVMQMPPeer(filepath.Join(q.ControlDir, "qmp.sock"), q.PID, q.UID); err != nil {
		t.Fatalf("%s: %v", q.Sandbox, err)
	}
	want, err := jailLimits(q.Resources, a.jailIO(t))
	if err != nil {
		t.Fatal(err)
	}
	files := map[string]string{}
	for _, name := range []string{"cpu.max", "memory.max", "pids.max", "io.max"} {
		if raw, err := jailKVMReadSmall(filepath.Join(q.Cgroup, name)); err == nil {
			files[name] = string(raw)
		}
	}
	if err := jailKVMCheckLimits(files, want); err != nil {
		t.Fatalf("%s: %v", q.Sandbox, err)
	}
	if again := a.qemu(t, q.Sandbox); again.PID != q.PID || again.StartTicks != q.StartTicks {
		t.Fatalf("%s: QEMU changed during the inspection: %+v then %+v", q.Sandbox, q, again)
	}
}

// jailKVMQMPPeer connects to a QMP socket (bounded) and reads only the
// peer credentials: QEMU accepts one monitor client at a time and the node
// holds it, so this connection only waits in the listen backlog and is
// closed without any exchange.
func jailKVMQMPPeer(path string, pid int, uid uint32) error {
	conn, err := (&net.Dialer{Timeout: 5 * time.Second}).Dial("unix", path)
	if err != nil {
		return fmt.Errorf("QMP %s: %w", path, err)
	}
	defer conn.Close()
	raw, err := conn.(*net.UnixConn).SyscallConn()
	if err != nil {
		return err
	}
	var cred *unix.Ucred
	var credErr error
	if err := raw.Control(func(fd uintptr) { cred, credErr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED) }); err != nil {
		return err
	}
	if credErr != nil {
		return fmt.Errorf("QMP %s SO_PEERCRED: %w", path, credErr)
	}
	if int(cred.Pid) != pid || cred.Uid != uid || cred.Gid != uid {
		return fmt.Errorf("QMP %s peer is pid %d uid %d gid %d, want the jailed QEMU %d as %d", path, cred.Pid, cred.Uid, cred.Gid, pid, uid)
	}
	return nil
}

// probeBinary stages a root-owned 0755 copy of this test binary in the
// attempt's stage dir (traversable by every jail uid) for the probe child.
func (a *jailKVMAttempt) probeBinary(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(a.rendered.StageDir, "probe")
	bin := filepath.Join(dir, "node.test")
	if _, err := os.Lstat(bin); err == nil {
		return bin
	}
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	a.mkdirOwned(t, dir, 0o755)
	jailKVMCopyExcl(t, self, bin, 0o755)
	a.own(t, "file", bin)
	return bin
}

// probe runs each case as its target uid: root pins the leaf as an O_PATH
// descriptor (proving it exists and is the expected inode) and passes only
// that descriptor to a re-exec of this test binary through setpriv, which
// drops the uid, gid, groups, bounding, inheritable and ambient sets and
// sets NoNewPrivs. The child verifies its own status and reopens
// /proc/self/fd/3; one bounded Wait collects its bounded output.
func (a *jailKVMAttempt) probe(t *testing.T, cases []jailKVMProbeCase) {
	t.Helper()
	bin := a.probeBinary(t)
	for _, c := range cases {
		t.Run(fmt.Sprintf("%d %s %s", c.As, c.Op, filepath.Base(c.Path)), func(t *testing.T) {
			fd, err := unix.Open(c.Path, unix.O_PATH|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
			if err != nil {
				t.Fatalf("pin %s: %v", c.Path, err)
			}
			leaf := os.NewFile(uintptr(fd), c.Path)
			defer leaf.Close()
			var st unix.Stat_t
			if err := unix.Fstat(fd, &st); err != nil || (jailKVMIdentity{Dev: st.Dev, Ino: st.Ino}) != jailKVMLstatID(t, c.Path) {
				t.Fatalf("pinned %s is not its name's inode: %v", c.Path, err)
			}
			as := strconv.FormatUint(uint64(c.As), 10)
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, "setpriv", "--reuid="+as, "--regid="+as, "--clear-groups",
				"--bounding-set=-all", "--inh-caps=-all", "--ambient-caps=-all", "--no-new-privs",
				"--", bin, "-test.run=^TestKVMJailProbeHelper$", "-test.v", "-test.count=1")
			cmd.Env = []string{jailKVMProbeEnv + "=" + c.Op, "PATH=/usr/bin:/bin"}
			cmd.Dir = "/"
			cmd.ExtraFiles = []*os.File{leaf}
			var out jailKVMBoundedOut
			out.limit = 64 << 10
			cmd.Stdout, cmd.Stderr = &out, &out
			runErr := cmd.Run()
			if out.over {
				t.Fatalf("probe output exceeds %d bytes", out.limit)
			}
			if runErr != nil {
				t.Fatalf("probe child: %v\n%s", runErr, out.buf.String())
			}
			res, err := jailKVMParseProbeOutput(out.buf.String())
			if err != nil {
				t.Fatal(err)
			}
			if err := jailKVMCheckProbeStatus(res.Status, c.As); err != nil {
				t.Fatalf("the probe did not run fully dropped: %v", err)
			}
			if res.Result != c.Want {
				t.Fatalf("uid %d %s %s = %s, want %s", c.As, c.Op, c.Path, res.Result, c.Want)
			}
		})
	}
}

// jailKVMBoundedOut keeps at most limit bytes and records an overflow.
type jailKVMBoundedOut struct {
	buf   bytes.Buffer
	limit int
	over  bool
}

func (b *jailKVMBoundedOut) Write(p []byte) (int, error) {
	if room := b.limit - b.buf.Len(); len(p) > room {
		b.over = true
		b.buf.Write(p[:max(room, 0)])
		return len(p), nil
	}
	return b.buf.Write(p)
}

// TestKVMJailProbeHelper is the cross-uid probe child (setpriv-dropped
// re-exec of the test binary by probe); inert otherwise. It reports its own
// /proc/self/status and the outcome of reopening /proc/self/fd/3 with the
// requested access.
func TestKVMJailProbeHelper(t *testing.T) {
	op := os.Getenv(jailKVMProbeEnv)
	if op == "" {
		t.Skip("re-executed cross-uid probe; only started by the jailed acceptance")
	}
	flags := map[string]int{"reopen-rw": os.O_RDWR, "reopen-r": os.O_RDONLY, "reopen-dir": os.O_RDONLY | syscall.O_DIRECTORY}
	flag, ok := flags[op]
	if !ok {
		t.Fatalf("unknown probe op %q", op)
	}
	status, err := os.ReadFile("/proc/self/status")
	if err != nil {
		t.Fatal(err)
	}
	result := "ok"
	f, err := os.OpenFile("/proc/self/fd/3", flag|syscall.O_CLOEXEC, 0)
	switch {
	case err == nil:
		f.Close()
	case errors.Is(err, syscall.EACCES):
		result = "EACCES"
	case errors.Is(err, syscall.EPERM):
		result = "EPERM"
	default:
		t.Fatalf("reopen: %v", err)
	}
	raw, err := json.Marshal(jailKVMProbeResult{Status: string(status), Result: result})
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(jailKVMProbeMarker + string(raw))
}

// kvmNetAddSSHKeyFresh adds a new (in-memory) client key to id and returns
// its access.
func kvmNetAddSSHKeyFresh(t *testing.T, tp *kvmNetTopology, c nodev1.NodeClient, id string) *nodev1.SSHAccess {
	t.Helper()
	_ = tp
	_, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := ssh.NewSignerFromKey(private)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	return kvmNetAddSSHKey(ctx, t, c, id, strings.TrimSpace(string(ssh.MarshalAuthorizedKey(signer.PublicKey()))))
}
