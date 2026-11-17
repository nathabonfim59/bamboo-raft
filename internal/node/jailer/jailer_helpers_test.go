package jailer

import (
	"context"
	"encoding/binary"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"
	"uuid"

	"golang.org/x/sys/unix"
)

// jailerFakeModeEnv selects the unprivileged fake helper in a re-executed
// test binary. The real helper needs root; the fake follows the same fd and
// release protocol without namespaces, mounts or credential changes.
const jailerFakeModeEnv = "BBRAFT_JAILER_FAKE_HELPER"

// Fake helper behaviours after the release byte.
const (
	jailerModeExec       = "exec"        // exec the staged binary (success)
	jailerModeExecHost   = "exec-host"   // exec the host binary, not the staged one
	jailerModeDie        = "die"         // exit before exec without a message
	jailerModeCloseAlive = "close-alive" // close the status pipe, stay alive
	jailerModeSetupError = "setup-error" // report a setup error and exit
	jailerModeHang       = "hang"        // keep the status pipe open
	// jailerModeGateHang stays alive even when the release gate closes
	// without the byte, standing in for a helper whose death is delayed.
	jailerModeGateHang = "gate-hang"
	// jailerModeExit0 exits 0 before exec without a message: a helper that
	// returns cleanly, which the trusted helper never does.
	jailerModeExit0 = "exit0"
	// jailerModeDieSignal kills itself with SIGKILL before exec.
	jailerModeDieSignal = "die-signal"
)

func TestMain(m *testing.M) {
	if os.Getenv(jailerRlimitProbeEnv) == "1" {
		os.Exit(jailerRlimitProbe())
	}
	if len(os.Args) > 1 && os.Args[1] == HelperArg && os.Getenv(jailerFakeModeEnv) != "" {
		os.Exit(jailerFakeHelper())
	}
	if len(os.Args) > 1 && os.Args[1] == AdmitHelperArg && os.Getenv(jailerFakeModeEnv) != "" {
		os.Exit(jailerFakeAdmit())
	}
	// The privileged suite re-executes this binary as the real helper; it
	// only returns on a setup error, which it already reported on the
	// status pipe.
	if os.Getenv(jailerProbeEnv) == "1" {
		os.Exit(jailerProbe(os.Args[1:]))
	}
	RunHelperIfSelected()
	os.Exit(m.Run())
}

func jailerFakeHelper() int {
	mode := os.Getenv(jailerFakeModeEnv)
	if len(os.Args) != 5 {
		return 90
	}
	var fds [3]int
	for i := range fds {
		fd, err := strconv.Atoi(os.Args[i+2])
		if err != nil {
			return 91
		}
		fds[i] = fd
	}
	unix.CloseOnExec(fds[2])
	status := os.NewFile(uintptr(fds[2]), "status")
	var cfg helperConfig
	if err := json.UnmarshalRead(os.NewFile(uintptr(fds[0]), "config"), &cfg); err != nil {
		fmt.Fprint(status, "decode config: ", err)
		return 92
	}
	var gate [1]byte
	if n, err := os.NewFile(uintptr(fds[1]), "release").Read(gate[:]); n != 1 || err != nil || gate[0] != 1 {
		if mode == jailerModeGateHang {
			for {
				unix.Pause()
			}
		}
		fmt.Fprint(status, "launch cancelled before setup")
		return 93
	}
	switch mode {
	case jailerModeExec:
		err := unix.Exec(filepath.Join(cfg.Root, cfg.Binary), []string{cfg.Binary, "3600"}, nil)
		fmt.Fprint(status, "exec: ", err)
		return 94
	case jailerModeExecHost:
		err := unix.Exec(cfg.Binary, []string{cfg.Binary, "3600"}, nil)
		fmt.Fprint(status, "exec: ", err)
		return 94
	case jailerModeDie:
		return 3
	case jailerModeExit0:
		return 0
	case jailerModeDieSignal:
		_ = unix.Kill(os.Getpid(), unix.SIGKILL)
		for {
			unix.Pause()
		}
	case jailerModeCloseAlive:
		status.Close()
		for {
			unix.Pause()
		}
	case jailerModeSetupError:
		fmt.Fprint(status, "injected setup failure")
		return 1
	case jailerModeHang:
		for {
			unix.Pause()
		}
	}
	return 95
}

// jailerFakeCgroups is an in-memory cgroupOps. Identities are distinct
// inodes; membership is the attached pids that are still running, which is
// what cgroup.procs reports for a real group.
type jailerFakeCgroups struct {
	mu     sync.Mutex
	next   uint64
	groups map[string]*jailerFakeGroup
	fail   map[string]error // operation -> injected error
	calls  []string
}

type jailerFakeGroup struct {
	identity inodeIdentity
	limits   Limits
	pids     []int
}

func newJailerFakeCgroups() *jailerFakeCgroups {
	return &jailerFakeCgroups{next: 1000, groups: make(map[string]*jailerFakeGroup), fail: make(map[string]error)}
}

func (c *jailerFakeCgroups) owned(id string, owned inodeIdentity) (*jailerFakeGroup, error) {
	g, ok := c.groups[id]
	if !ok {
		return nil, fs.ErrNotExist
	}
	if !owned.valid() || !g.identity.equal(owned) {
		return nil, fmt.Errorf("jailer: cgroup %s was replaced; preserving it", id)
	}
	return g, nil
}

func (c *jailerFakeCgroups) Prepare(id string, limits Limits) (inodeIdentity, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, "prepare "+id)
	if err := c.fail["mkdir"]; err != nil {
		return inodeIdentity{}, err
	}
	if _, exists := c.groups[id]; exists {
		return inodeIdentity{}, fmt.Errorf("jailer: cgroup %s already exists", id)
	}
	c.next++
	g := &jailerFakeGroup{identity: inodeIdentity{Dev: 99, Ino: c.next}}
	c.groups[id] = g
	if err := c.fail["limits"]; err != nil {
		return g.identity, err
	}
	g.limits = limits
	return g.identity, nil
}

// OpenOwned stands in for the group directory fd. The fake cannot be a
// CLONE_INTO_CGROUP target, so jailerEnv's command hook drops UseCgroupFD
// and Joined records the membership the kernel would have created at birth.
func (c *jailerFakeCgroups) OpenOwned(id string, owned inodeIdentity) (*os.File, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, "open "+id)
	if _, err := c.owned(id, owned); err != nil {
		return nil, err
	}
	if err := c.fail["open"]; err != nil {
		return nil, err
	}
	return os.Open(os.DevNull)
}

// Joined records the birth membership first, as the kernel already did,
// then reports an injected verification failure.
func (c *jailerFakeCgroups) Joined(id string, pid int, owned inodeIdentity) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, "joined "+id)
	g, err := c.owned(id, owned)
	if err != nil {
		return err
	}
	g.pids = append(g.pids, pid)
	return c.fail["attach"]
}

func (c *jailerFakeCgroups) Empty(id string, owned inodeIdentity) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	g, err := c.owned(id, owned)
	if errors.Is(err, fs.ErrNotExist) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	for _, pid := range g.pids {
		identity, err := readProcIdentity(pid)
		if err == nil && !procDead(identity.state) {
			return false, nil
		}
	}
	return true, nil
}

func (c *jailerFakeCgroups) Kill(id string, owned inodeIdentity) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, "kill "+id)
	g, err := c.owned(id, owned)
	if err != nil {
		return err
	}
	for _, pid := range g.pids {
		_ = unix.Kill(pid, unix.SIGKILL)
	}
	return nil
}

func (c *jailerFakeCgroups) Remove(id string, owned inodeIdentity) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, "remove "+id)
	if _, err := c.owned(id, owned); errors.Is(err, fs.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	if err := c.fail["remove"]; err != nil {
		return err
	}
	delete(c.groups, id)
	return nil
}

func (c *jailerFakeCgroups) List() ([]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	names := make([]string, 0, len(c.groups))
	for id := range c.groups {
		names = append(names, cgroupName(id))
	}
	slices.Sort(names)
	return names, nil
}

func (c *jailerFakeCgroups) Close() error { return nil }

func (c *jailerFakeCgroups) has(id string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.groups[id]
	return ok
}

func (c *jailerFakeCgroups) attached(id string) []int {
	c.mu.Lock()
	defer c.mu.Unlock()
	if g, ok := c.groups[id]; ok {
		return slices.Clone(g.pids)
	}
	return nil
}

// jailerTestBinary is a trusted root-owned dynamic executable staged as the
// fake QEMU. The fake helper execs its staged copy with "3600".
const jailerTestBinary = "/usr/bin/sleep"

func jailerNeedBinary(t *testing.T) {
	t.Helper()
	info, err := os.Stat(jailerTestBinary)
	if err != nil {
		t.Skipf("no %s: %v", jailerTestBinary, err)
	}
	if st, ok := info.Sys().(*syscall.Stat_t); !ok || st.Uid != 0 {
		t.Skipf("%s is not root-owned", jailerTestBinary)
	}
}

// jailerStages records every launch boundary reached and fails the one
// named by failAt with err.
type jailerStages struct {
	mu      sync.Mutex
	reached []launchStage
	failAt  launchStage
	err     error
	on      map[launchStage]func()
}

func (s *jailerStages) hook(stage launchStage) error {
	s.mu.Lock()
	s.reached = append(s.reached, stage)
	fn := s.on[stage]
	fail := stage == s.failAt
	s.mu.Unlock()
	if fn != nil {
		fn()
	}
	if fail {
		return s.err
	}
	return nil
}

// last returns the last launch boundary reached. The reclaim stage belongs
// to the release a failed launch performs, so it is skipped here; tests that
// care about reclaim inspect reached directly.
func (s *jailerStages) last() launchStage {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := len(s.reached) - 1; i >= 0; i-- {
		if s.reached[i] != stageReclaim {
			return s.reached[i]
		}
	}
	return ""
}

type jailerEnv struct {
	cfg      Config
	cgroups  *jailerFakeCgroups
	stages   *jailerStages
	mode     string
	pidfd    func(int) (int, error)
	tapFlags uint16
	jailOp   func(jailOp, string) error
	chowned  []string
	mu       sync.Mutex
	// reclaimed records every reclaim call; onReclaim, when set, decides
	// its result.
	reclaimed []Record
	onReclaim func(context.Context, Record) error
	// toolOwner owns the tool children of the Manager jailerToolOpen made.
	toolOwner *jailerToolOwner
}

func newJailerEnv(t *testing.T) *jailerEnv {
	t.Helper()
	base := t.TempDir()
	cfg := Config{
		StateDir:     filepath.Join(base, "state"),
		CgroupParent: filepath.Join(base, "cgroup"),
		UIDs:         UIDRange{Min: 50000, Max: 50002},
		Log:          slog.New(slog.DiscardHandler),
	}
	if err := os.MkdirAll(cfg.CgroupParent, 0o700); err != nil {
		t.Fatal(err)
	}
	e := &jailerEnv{
		cfg: cfg, cgroups: newJailerFakeCgroups(), stages: &jailerStages{}, mode: jailerModeExec,
		tapFlags: unix.IFF_TAP | unix.IFF_NO_PI | unix.IFF_VNET_HDR,
	}
	e.cfg.Reclaim = e.reclaim
	return e
}

// reclaim is the env's recording reclaim policy. It runs under the
// Manager lock, so it observes state only through the filesystem.
func (e *jailerEnv) reclaim(ctx context.Context, record Record) error {
	e.mu.Lock()
	e.reclaimed = append(e.reclaimed, record)
	hook := e.onReclaim
	e.mu.Unlock()
	if hook != nil {
		return hook(ctx, record)
	}
	return nil
}

// reclaims returns the records the policy has seen.
func (e *jailerEnv) reclaims() []Record {
	e.mu.Lock()
	defer e.mu.Unlock()
	return slices.Clone(e.reclaimed)
}

// jailerNoReclaim is the policy for tests whose records own no caller
// resource that needs returning.
func jailerNoReclaim(context.Context, Record) error { return nil }

// jailerAttempt returns a fresh attempt identity.
func jailerAttempt() string { return uuid.NewV7().String() }

// jailerRefOf returns id's current attempt reference, or a reference no
// record matches when the id has none.
func jailerRefOf(m *Manager, id string) Ref {
	if record, ok := m.Record(id); ok {
		return record.Ref()
	}
	return Ref{SandboxID: id, Attempt: jailerAttempt()}
}

// open creates a Manager on the env's state dir with the fake cgroups and
// test hooks. The fake cgroup state survives a Close/open "restart".
func (e *jailerEnv) open(t *testing.T) *Manager {
	t.Helper()
	m, err := New(e.cfg)
	if err != nil {
		t.Fatal(err)
	}
	_ = m.cgroups.Close()
	m.cgroups = e.cgroups
	m.hooks = managerHooks{
		stage: e.stages.hook,
		command: func(cmd *exec.Cmd) {
			cmd.Env = []string{jailerFakeModeEnv + "=" + e.mode}
			// No namespaces (unprivileged) and no CLONE_INTO_CGROUP into the
			// fake group; keep the clone pidfd unless a test forces the
			// pidfd_open path.
			pidfd := cmd.SysProcAttr.PidFD
			if e.pidfd != nil {
				pidfd = nil
			}
			cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL, PidFD: pidfd}
		},
		pidfdOpen: e.pidfd,
		chown: func(f *os.File, uid int) error {
			e.mu.Lock()
			defer e.mu.Unlock()
			e.chowned = append(e.chowned, fmt.Sprintf("%s:%d", filepath.Base(f.Name()), uid))
			return nil
		},
		devices: func() (privateDevices, error) {
			null, err := fixedDevice("/dev/null")
			return privateDevices{KVM: unix.Mkdev(10, 232), Random: unix.Mkdev(1, 9), Null: null}, err
		},
		deviceRdev: func(p string) (uint64, error) {
			role, ok := jailerRoleHost(p)
			if !ok {
				return 0, fmt.Errorf("unexpected role device %s", p)
			}
			return fixedDevice(jailerDevice(role))
		},
		tapFlags: func(*os.File) (uint16, error) { return e.tapFlags, nil },
		// open_tree needs CAP_SYS_ADMIN: stand in with a fresh descriptor
		// of the same inode, which the fake helper never mounts.
		detachMount: func(f *os.File) (*os.File, error) {
			return os.Open(fdPath(int(f.Fd())))
		},
		jailOp: e.jailOp,
	}
	return m
}

func jailerOpen(t *testing.T, name string, flag int) *os.File {
	t.Helper()
	f, err := os.OpenFile(name, flag, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })
	return f
}

// jailerDevice maps each device role to the distinct unprivileged character
// device that stands in for it; jailerEnv's platform seam reports the same
// mapping as the role's host device.
func jailerDevice(role string) string {
	switch role {
	case TAPName:
		return "/dev/null"
	case VhostNetName:
		return "/dev/zero"
	case VhostVsockName:
		return "/dev/full"
	}
	panic("no device role " + role)
}

// jailerRoleHost maps a role's host device path to its role.
func jailerRoleHost(host string) (string, bool) {
	switch host {
	case "/dev/net/tun":
		return TAPName, true
	case "/dev/vhost-net":
		return VhostNetName, true
	case "/dev/vhost-vsock":
		return VhostVsockName, true
	}
	return "", false
}

// jailerSpec builds a valid spec: fds 3 tap, 4 vhost-net, 5 kernel,
// 6 initrd, 7 vhost-vsock, then the disks; see jailerDevice.
func jailerSpec(t *testing.T, id string, disks ...[]byte) LaunchSpec {
	t.Helper()
	dir := t.TempDir()
	if len(disks) == 0 {
		disks = [][]byte{nil} // one empty raw disk
	}
	write := func(name string, content []byte) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, content, 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	files := []Descriptor{
		{Name: TAPName, File: jailerOpen(t, jailerDevice(TAPName), os.O_RDWR)},
		{Name: VhostNetName, File: jailerOpen(t, jailerDevice(VhostNetName), os.O_RDWR)},
		{Name: KernelName, File: jailerOpen(t, write("kernel", []byte("kernel")), os.O_RDONLY)},
		{Name: InitrdName, File: jailerOpen(t, write("initrd", []byte("initrd")), os.O_RDONLY)},
		{Name: VhostVsockName, File: jailerOpen(t, jailerDevice(VhostVsockName), os.O_RDWR)},
	}
	var names []string
	for i, content := range disks {
		name := fmt.Sprintf("disk%d", i)
		flag := os.O_RDONLY
		if i == len(disks)-1 {
			flag = os.O_RDWR
		}
		files = append(files, Descriptor{Name: name, File: jailerOpen(t, write(name, content), flag)})
		names = append(names, name)
	}
	if err := os.Mkdir(filepath.Join(dir, "control"), 0o700); err != nil {
		t.Fatal(err)
	}
	spec := LaunchSpec{
		SandboxID:  id,
		Attempt:    jailerAttempt(),
		Binary:     jailerTestBinary,
		Files:      files,
		Disks:      names,
		ControlDir: jailerOpen(t, filepath.Join(dir, "control"), os.O_RDONLY),
		SerialLog:  jailerOpen(t, write("serial.log", nil), os.O_RDWR),
		Limits: Limits{
			CPUQuotaPercent: 100,
			MemoryBytes:     64 << 20,
			Pids:            16,
			IO:              IOLimits{Device: BlockDevice{Major: 8, Minor: 1}, ReadBPS: 1024},
		},
	}
	qcow := make([]bool, len(disks))
	for i, content := range disks {
		qcow[i] = len(content) >= 4 && [4]byte(content[:4]) == qcowMagic
	}
	spec.Args = jailerArgs(spec, qcow)
	return spec
}

// jailerArgs renders the supported argv form for spec.
func jailerArgs(spec LaunchSpec, qcow []bool) []string {
	id := spec.SandboxID
	args := []string{
		"-name", "guest=" + id + ",process=" + id,
		"-machine", "q35,accel=kvm",
		"-cpu", "qemu64,+vmx,enforce",
		"-nodefaults", "-no-user-config", "-no-reboot",
		"-m", "64M", "-smp", "1",
		"-kernel", fdPath(spec.fdOf(KernelName)),
		"-initrd", fdPath(spec.fdOf(InitrdName)),
		"-append", "console=ttyS0 quiet",
	}
	node := ""
	for i, name := range spec.Disks {
		driver := "raw"
		if qcow[i] {
			driver = "qcow2"
		}
		value := fmt.Sprintf("driver=%s,node-name=disk%d,file.driver=file,file.node-name=file%d,file.filename=%s,file.cache.direct=off,file.cache.no-flush=off",
			driver, i, i, fdPath(spec.fdOf(name)))
		if qcow[i] {
			value += ",lazy-refcounts=off"
			if i > 0 {
				value += ",backing=" + node
			}
		}
		if i < len(spec.Disks)-1 {
			value += ",read-only=on"
		}
		node = fmt.Sprintf("disk%d", i)
		args = append(args, "-blockdev", value)
	}
	return append(args,
		"-device", "virtio-blk-pci,drive="+node+",write-cache=on,werror=stop,rerror=stop",
		"-netdev", "tap,id=net0,fd=3,vhost=on,vhostfd=4",
		// Direct kernel boot needs no option ROM: romfile= disables it, so
		// no distribution firmware file has to be staged.
		"-device", "virtio-net-pci,netdev=net0,mac=02:00:00:00:00:01,romfile=",
		"-device", "vhost-vsock-pci,guest-cid=3,vhostfd="+strconv.Itoa(spec.fdOf(VhostVsockName)),
		"-object", "rng-random,filename=/dev/urandom,id=rng0",
		"-device", "virtio-rng-pci,rng=rng0",
		"-device", "virtio-balloon-pci,free-page-reporting=on",
		"-device", "vmgenid",
		"-chardev", "file,id=serial0,path=/serial.log,append=on",
		"-serial", "chardev:serial0",
		"-qmp", "unix:/control/qmp.sock,server=on,wait=off",
		"-display", "none",
		"-sandbox", SandboxPolicy,
	)
}

// jailerQCOW returns a qcow2 v3 header. backing gives a stored backing name.
func jailerQCOW(backing string, incompatible uint64) []byte {
	raw := make([]byte, 512)
	copy(raw, qcowMagic[:])
	binary.BigEndian.PutUint32(raw[4:8], 3)
	if backing != "" {
		binary.BigEndian.PutUint64(raw[8:16], 256)
		binary.BigEndian.PutUint32(raw[16:20], uint32(len(backing)))
		copy(raw[256:], backing)
	}
	binary.BigEndian.PutUint64(raw[72:80], incompatible)
	binary.BigEndian.PutUint32(raw[100:104], qcowHeaderBytes)
	return raw
}

// jailerOpenFDs counts this process's open descriptors.
func jailerOpenFDs(t *testing.T) int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	return len(entries)
}

func jailerExists(t *testing.T, root string, name string) bool {
	t.Helper()
	_, err := os.Lstat(path.Join(root, name))
	if errors.Is(err, fs.ErrNotExist) {
		return false
	}
	if err != nil {
		t.Fatal(err)
	}
	return true
}

// jailerJoinTimeout bounds every reaper join.
const jailerJoinTimeout = 20 * time.Second

// jailerOwner is the single owner of one Child's sole Cmd.Wait. It is
// registered the moment Start returns a non-nil Child, error included, so a
// failing assertion can never abandon a live process: its cleanup kills
// through the Child handle (never a raw pid), joins the reaper with a bound
// and releases. Tests consume the recorded Wait result instead of waiting.
type jailerOwner struct {
	child *Child
	done  chan struct{}
	err   error
	// strict makes cleanup fail the test when the final Release fails.
	strict bool
	// via, when set, names the Manager that owns the reservation after the
	// test deliberately closed the Child's own Manager (a restart). Only
	// then is the Child's ErrClosed expected, and the reservation is
	// released through via instead.
	via func() *Manager
}

func jailerOwn(t *testing.T, child *Child) *jailerOwner {
	t.Helper()
	o := &jailerOwner{child: child, done: make(chan struct{})}
	go func() {
		o.err = child.Cmd.Wait()
		close(o.done)
	}()
	t.Cleanup(func() {
		_ = child.Kill()
		select {
		case <-o.done:
		case <-time.After(jailerJoinTimeout):
			t.Errorf("child %d was not reaped within %v", child.Cmd.Process.Pid, jailerJoinTimeout)
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), jailerJoinTimeout)
		defer cancel()
		err := child.Release(ctx)
		if errors.Is(err, ErrClosed) && o.via != nil {
			if m := o.via(); m != nil {
				err = m.Release(ctx, child.Identity().SandboxID)
			}
		}
		if err != nil && o.strict {
			t.Errorf("cleanup Release of child %d: %v", child.Cmd.Process.Pid, err)
		}
	})
	return o
}

// jailerStart starts spec and registers an owner for any returned Child.
func jailerStart(t *testing.T, m *Manager, ctx context.Context, spec LaunchSpec) (*Child, *jailerOwner, error) {
	t.Helper()
	child, err := m.Start(ctx, spec)
	if child == nil {
		return nil, nil, err
	}
	return child, jailerOwn(t, child), err
}

// wait returns the recorded result of the sole Wait, bounded.
func (o *jailerOwner) wait(t *testing.T) error {
	t.Helper()
	select {
	case <-o.done:
		return o.err
	case <-time.After(jailerJoinTimeout):
		t.Fatalf("child %d did not exit within %v", o.child.Cmd.Process.Pid, jailerJoinTimeout)
		return nil
	}
}

// waitKilled requires the recorded Wait result to be a SIGKILL.
func (o *jailerOwner) waitKilled(t *testing.T) {
	t.Helper()
	err := o.wait(t)
	exitErr, ok := errors.AsType[*exec.ExitError](err)
	if !ok {
		t.Fatalf("Wait = %v, want a killed process", err)
	}
	if ws, ok := exitErr.Sys().(syscall.WaitStatus); !ok || !ws.Signaled() || ws.Signal() != syscall.SIGKILL {
		t.Fatalf("child exit = %v, want SIGKILL", exitErr)
	}
}
