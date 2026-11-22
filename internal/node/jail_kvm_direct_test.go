package node

// The A10 Direct helper: a test-only real jailed Manager served on a unix
// socket under the SAME rendered unit (only ExecStart and Environment are
// replaced by the drop-in). It mirrors bbraft-node's jail path step by step
// except the cmd-only proofs it cannot run (realJailHost: the root
// supervisor, delegation, account and IO-device proofs); TestKVMJailAcceptance
// /Link covers those through the production main.

import (
	"cmp"
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
	"uuid"

	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/nathabonfim59/bamboo-raft/internal/node/jailer"
	nodenet "github.com/nathabonfim59/bamboo-raft/internal/node/net"
	"github.com/nathabonfim59/bamboo-raft/internal/node/qemu"
	"github.com/nathabonfim59/bamboo-raft/internal/nodelink"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// jailKVMDirectShutdown bounds the Manager shutdown inside the unit's
// TimeoutStopSec=30s (see jailKVMDirectServeDefaults).
const jailKVMDirectShutdown = 20 * time.Second

// jailKVMProbeHostFacts mirrors bbraft-node's host facts: runtime.NumCPU,
// MemTotal in MiB and the available GiB (Bavail) of the filesystem holding
// dataDir (its deepest existing ancestor).
func jailKVMProbeHostFacts(dataDir string) (jailKVMHostFacts, error) {
	raw, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return jailKVMHostFacts{}, err
	}
	var memKB uint64
	for line := range strings.Lines(string(raw)) {
		if v, ok := strings.CutPrefix(line, "MemTotal:"); ok {
			f := strings.Fields(v)
			if len(f) != 2 || f[1] != "kB" {
				return jailKVMHostFacts{}, fmt.Errorf("malformed MemTotal %q", line)
			}
			if memKB, err = strconv.ParseUint(f[0], 10, 64); err != nil {
				return jailKVMHostFacts{}, err
			}
		}
	}
	p := filepath.Clean(dataDir)
	var st syscall.Statfs_t
	for {
		err := syscall.Statfs(p, &st)
		if err == nil {
			break
		}
		if !errors.Is(err, os.ErrNotExist) || filepath.Dir(p) == p {
			return jailKVMHostFacts{}, fmt.Errorf("statfs %s: %w", p, err)
		}
		p = filepath.Dir(p)
	}
	if st.Bsize <= 0 {
		return jailKVMHostFacts{}, fmt.Errorf("statfs %s: block size %d", p, st.Bsize)
	}
	disk := st.Bavail / (1 << 30 / uint64(st.Bsize))
	return jailKVMHostFacts{VCPUs: uint64(max(runtime.NumCPU(), 0)), MemoryMiB: memKB / 1024, DiskGiB: disk}, nil
}

// directExec stages a root-owned 0755 copy of this test binary in the
// attempt's stage dir and writes direct.json (root 0600, owned) with the
// rendered ExecStart flags verbatim; the plan is checked here first, so a
// rendered flag the helper cannot map fails before anything is installed.
func (a *jailKVMAttempt) directExec(t *testing.T) *jailKVMDirect {
	t.Helper()
	r := a.rendered
	cfg := jailKVMDirectConfig{Flags: jailKVMExecFlags(a.unit.ExecStart), Socket: a.directSocket()}
	facts, err := jailKVMProbeHostFacts(r.DataDir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := jailKVMDirectPlan(cfg, facts); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	cfgPath := filepath.Join(r.ConfigDir, "direct.json")
	jailKVMWriteExcl(t, cfgPath, raw, 0o600)
	a.own(t, "file", cfgPath)
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(r.StageDir, "direct")
	a.mkdirOwned(t, dir, 0o755)
	bin := filepath.Join(dir, "node.test")
	jailKVMCopyExcl(t, self, bin, 0o755)
	a.own(t, "file", bin)
	return &jailKVMDirect{
		Exec: []string{bin, "-test.run=^TestKVMJailDirectHelper$", "-test.v", "-test.timeout=0"},
		Env:  map[string]string{jailKVMHelperEnv: cfgPath},
	}
}

// jailKVMDialDirect dials the Direct helper's socket and proves it with a
// bounded Info, without failing the test (the teardown recovery uses it).
func jailKVMDialDirect(ctx context.Context, path string) (nodev1.NodeClient, func() error, error) {
	conn, err := grpc.NewClient("passthrough:///node",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", path)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(nodelink.MaxResponseBytes)),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("dial %s: %w", path, err)
	}
	client := nodev1.NewNodeClient(conn)
	ictx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if _, err := client.Info(ictx, &nodev1.InfoRequest{}); err != nil {
		return nil, nil, errors.Join(fmt.Errorf("direct Info: %w", err), conn.Close())
	}
	return client, conn.Close, nil
}

// dialDirect connects to the Direct helper, records the client and its
// closer for the teardown and installs the same dial as reconnect.
func (a *jailKVMAttempt) dialDirect(t *testing.T) nodev1.NodeClient {
	t.Helper()
	connect := func(ctx context.Context) (nodev1.NodeClient, error) {
		client, closeConn, err := jailKVMDialDirect(ctx, a.directSocket())
		if err != nil {
			return nil, err
		}
		a.mu.Lock()
		a.client = client
		a.conns = append(a.conns, closeConn)
		a.mu.Unlock()
		return client, nil
	}
	a.mu.Lock()
	a.reconnect = connect
	a.mu.Unlock()
	client, err := connect(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	return client
}

// jailKVMDirectCgroup is the test copy of bbraft-node's one cgroup mutation
// (prepareJailCgroupAs, owner root): prove the service cgroup directory
// (no-follow, root-owned, not group/other writable), bind that inode, enable
// the controllers, create vms and prove it the same way. Like production it
// creates vms with 0755, which the unit's UMask=0077 makes 0700 (root-only).
func jailKVMDirectCgroup(ctx context.Context, jailCgroup string) error {
	service := filepath.Dir(jailCgroup)
	prove := func(info os.FileInfo) error {
		st, ok := info.Sys().(*syscall.Stat_t)
		if !info.IsDir() || !ok || st.Uid != 0 || info.Mode().Perm()&0o022 != 0 {
			return fmt.Errorf("jail cgroup: %s is not a root-owned directory only root can write", info.Name())
		}
		return nil
	}
	named, err := os.Lstat(service)
	if err != nil {
		return fmt.Errorf("jail cgroup: %w", err)
	}
	if err := prove(named); err != nil {
		return err
	}
	root, err := os.OpenRoot(service)
	if err != nil {
		return fmt.Errorf("jail cgroup: %w", err)
	}
	defer root.Close()
	if opened, err := root.Stat("."); err != nil || !os.SameFile(named, opened) {
		return fmt.Errorf("jail cgroup: %s changed while it was opened", service)
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("jail cgroup: %w", context.Cause(ctx))
	}
	control, err := root.OpenFile("cgroup.subtree_control", os.O_WRONLY|os.O_TRUNC|syscall.O_NOFOLLOW, 0)
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
	if err := root.Mkdir("vms", 0o755); err != nil && !errors.Is(err, os.ErrExist) {
		return fmt.Errorf("jail cgroup: %w", err)
	}
	vms, err := root.Lstat("vms")
	if err != nil {
		return fmt.Errorf("jail cgroup: %w", err)
	}
	return prove(vms)
}

// jailKVMDirectListen listens on the Direct socket and returns its cleanup.
// An existing path is refused, never adopted. The listener's own unlink on
// Close is disabled at once (a Go unix listener removes its original
// pathname on Close, even if that name is now a replacement; grpc's Stop
// closes it too), so the only removal is the cleanup's: Close, then remove
// the path only while it is still the socket inode created here. The
// identity is captured before the chmod to 0600 and re-proven after it.
func jailKVMDirectListen(path string) (net.Listener, func() error, error) {
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		return nil, nil, fmt.Errorf("direct socket %s already exists (%v)", path, err)
	}
	l, err := net.Listen("unix", path)
	if err != nil {
		return nil, nil, err
	}
	l.(*net.UnixListener).SetUnlinkOnClose(false)
	var created unix.Stat_t
	idErr := unix.Lstat(path, &created)
	cleanup := func() error {
		closeErr := l.Close()
		if errors.Is(closeErr, net.ErrClosed) {
			closeErr = nil
		}
		var now unix.Stat_t
		if idErr == nil && unix.Lstat(path, &now) == nil && now.Dev == created.Dev && now.Ino == created.Ino {
			return errors.Join(closeErr, os.Remove(path))
		}
		return closeErr
	}
	if idErr != nil || created.Mode&unix.S_IFMT != unix.S_IFSOCK {
		return nil, nil, errors.Join(fmt.Errorf("direct socket %s is not the socket just created (%v)", path, idErr), cleanup())
	}
	var after unix.Stat_t
	if err := errors.Join(os.Chmod(path, 0o600), unix.Lstat(path, &after)); err != nil ||
		after.Dev != created.Dev || after.Ino != created.Ino || after.Mode&unix.S_IFMT != unix.S_IFSOCK || after.Mode&0o7777 != 0o600 {
		return nil, nil, errors.Join(fmt.Errorf("direct socket %s: chmod 0600 not proven on the created inode (%v)", path, err), cleanup())
	}
	return l, cleanup, nil
}

// TestKVMJailDirectHelper is the Direct subtest's service main, started by
// systemd from the drop-in's ExecStart; inert otherwise. It reads
// direct.json (jailKVMReadDirectConfig, root 0600) and the host facts, maps
// every rendered flag (jailKVMDirectPlan), then runs bbraft-node's jail path
// in order: CPU resolution, the cgroup mutation, reclaimer, jailer,
// admission, the jailed CPU probe with its nested fallback and evidence
// equality, nodenet, node.New (its tool self-check and local-CID probe),
// the socket listen and only then "bbraft-node ready". SIGTERM/SIGINT stop
// it within the unit's 30s (jailKVMDirectServe: 3s graceful, then Stop and
// a 2s GracefulStop join, a 2s join of the sole Serve waiter; then a 20s
// Manager shutdown), then the network, jailer and reclaimer close, last in,
// first out.
func TestKVMJailDirectHelper(t *testing.T) {
	cfgPath := os.Getenv(jailKVMHelperEnv)
	if cfgPath == "" {
		t.Skip("re-executed Direct helper; only started by TestKVMJailAcceptance/Direct")
	}
	ctx, stopSignals := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stopSignals()
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	cfg, err := jailKVMReadDirectConfig(cfgPath, 0)
	if err != nil {
		t.Fatal(err)
	}
	dataDir := cfg.Flags["-node.data-dir"]
	facts, err := jailKVMProbeHostFacts(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	s, err := jailKVMDirectPlan(cfg, facts)
	if err != nil {
		t.Fatal(err)
	}
	log.Info("capacity resolved", "vcpus", s.Capacity.GetVcpus(), "memory_mib", s.Capacity.GetMemoryMib(), "disk_gib", s.Capacity.GetDiskGib())
	var host qemu.HostCPU
	if s.Nested != qemu.NestedOff {
		if host, err = qemu.DetectHostCPU(""); err != nil {
			if s.Nested == qemu.NestedOn {
				t.Fatalf("cannot detect host CPU for -node.cpu-nested=on: %v", err)
			}
			log.Warn("cannot detect host CPU; nested virtualization stays off", "err", err)
			host = qemu.HostCPU{}
		}
	}
	cpu, err := qemu.ResolveCPU(s.CPUModel, s.Nested, host)
	if err != nil {
		t.Fatal(err)
	}
	log.Info("cpu resolved", "cpu_option", cpu.String(), "model", cpu.Model, "mode", s.Nested.String())
	if err := jailKVMDirectCgroup(ctx, s.Cgroup); err != nil {
		t.Fatal(err)
	}
	rc, err := NewJailReclaimer(ctx, s.DataDir, s.ImageDir)
	if err != nil {
		t.Fatalf("jail reclaimer: %v", err)
	}
	defer func() {
		if err := rc.Close(); err != nil {
			t.Errorf("jail reclaimer: close: %v", err)
		}
	}()
	jm, err := jailer.New(jailer.Config{
		StateDir: filepath.Join(s.DataDir, "jailer"), CgroupParent: s.Cgroup, UIDs: s.UIDs,
		LibraryDirs: s.LibraryDirs, FirmwareFiles: s.Firmware, Reclaim: rc.Reclaim, Log: log,
	})
	if err != nil {
		t.Fatalf("jailer: %v", err)
	}
	defer func() {
		if err := jm.Close(); err != nil {
			t.Errorf("jailer: close: %v", err)
		}
	}()
	if _, err := jm.Admit(ctx, jailer.AdmitSpec{Attempt: uuid.NewV7().String(), Limits: jailer.Limits{
		CPUQuotaPercent: 100, MemoryBytes: 256 << 20, Pids: 64, IO: s.IO,
	}}); err != nil {
		t.Fatalf("jail admission: %v", err)
	}
	log.Info("jail admitted", "uid_min", s.UIDs.Min, "uid_max", s.UIDs.Max, "cgroup", s.Cgroup)
	probe := func() (CPUEvidence, error) {
		return ProbeJailedCPU(ctx, jm, rc, JailProbeOptions{
			Binary: s.QEMUBinary, Option: cpu, Host: host,
			Limits: jailer.Limits{CPUQuotaPercent: 100, MemoryBytes: 512 << 20, Pids: 64, IO: s.IO},
		})
	}
	ev, err := probe()
	if toggles, dropped, ok := qemu.NestedFallback(cpu, err); ok {
		log.Warn("host lacks some nested VMX features; guests keep nested virtualization without them",
			"dropped", strings.Join(dropped, ","), "cpu_option_refused", cpu.String())
		cpu.Features = append(slices.Clip(cpu.Features), toggles...)
		ev, err = probe()
	}
	if err != nil {
		t.Fatalf("enforced CPU %s cannot be realized: %v", cpu.String(), err)
	}
	if ev.Option.String() != cpu.String() || ev.Host != host {
		t.Fatalf("jailed CPU probe evidence %s does not match the requested %s", ev.Option.String(), cpu.String())
	}
	log.Info("cpu realized", "cpu_option", cpu.String(), "status", ev.Status, "qemu", ev.QEMUVersion, "pid", ev.PID, "jailed", true)
	network, err := nodenet.New(s.Network, nodenet.Deps{Logger: log})
	if err != nil {
		t.Fatalf("node networking: %v", err)
	}
	defer func() {
		if err := network.Close(); err != nil {
			t.Errorf("node networking: close: %v", err)
		}
	}()
	if err := network.CheckHost(ctx); err != nil {
		t.Fatalf("node networking: check host: %v", err)
	}
	log.Info("vm networking ready", "network_mode", "tap", "bridge", s.Network.Bridge, "prefix", s.Network.Prefix)
	// The object store the way bbraft-node gets it: -s3.* resolved from the
	// unit's BBRAFT_S3_* environment (the role-rendered EnvironmentFile and
	// credential), reaching the bucket through the netns forwarder.
	store, err := jailKVMDirectObjectStore(ctx)
	if err != nil {
		t.Fatal(err)
	}
	log.Info("object store configured")
	admitted := host
	mgr, err := New(ctx, Config{
		NodeID: s.NodeID, DataDir: s.DataDir, ImageDir: s.ImageDir, DefaultImage: s.DefaultImage,
		CPUModel: s.CPUModel, Capacity: s.Capacity, HostVCPUs: s.HostVCPUs, DiskOvercommit: 1,
		BootTimeout: s.BootTimeout, Log: log, Nested: s.Nested, CPUFeatures: cpu.Features,
		Jailer: jm, QemuVersion: ev.QEMUVersion, HostCPU: &admitted, JailBinary: s.QEMUBinary,
		JailToolBinary: s.ToolBinary, JailIO: s.IO, Network: network,
		Store: store, ChunkCacheBytes: int64(s.ChunkCacheBytes), Phase2: ev.Phase2,
	})
	if err != nil {
		t.Fatalf("node: %v", err)
	}
	defer func() {
		sctx, cancel := context.WithTimeout(context.Background(), jailKVMDirectShutdown)
		defer cancel()
		if err := mgr.Shutdown(sctx); err != nil {
			t.Errorf("node: shutdown: %v", err)
		}
	}()
	lis, cleanupSocket, err := jailKVMDirectListen(s.Socket)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := cleanupSocket(); err != nil {
			t.Errorf("direct socket: %v", err)
		}
	}()
	srv := grpc.NewServer(nodelink.ServerOptions()...)
	mgr.Register(srv)
	if err := jailKVMDirectServe(ctx, srv, lis, func() {
		log.Info("bbraft-node ready", "node_id", s.NodeID, "socket", s.Socket,
			"vcpus", s.Capacity.GetVcpus(), "memory_mib", s.Capacity.GetMemoryMib(), "disk_gib", s.Capacity.GetDiskGib())
	}, jailKVMDirectServeDefaults()); err != nil {
		t.Error(err)
	}
}

// jailKVMServer is what jailKVMDirectServe needs of a grpc.Server.
type jailKVMServer interface {
	Serve(net.Listener) error
	GracefulStop()
	Stop()
}

// jailKVMServeBudget bounds the Direct stop.
type jailKVMServeBudget struct{ Graceful, GracefulJoin, ServeJoin time.Duration }

// The Direct stop budget: with the 20s Manager shutdown,
// 3+2+2+20 = 27s inside the unit's TimeoutStopSec=30s.
const (
	jailKVMDirectGraceful     = 3 * time.Second
	jailKVMDirectGracefulJoin = 2 * time.Second
	jailKVMDirectServeJoin    = 2 * time.Second
)

// jailKVMDirectServeDefaults returns the Direct stop budget.
func jailKVMDirectServeDefaults() jailKVMServeBudget {
	return jailKVMServeBudget{Graceful: jailKVMDirectGraceful, GracefulJoin: jailKVMDirectGracefulJoin, ServeJoin: jailKVMDirectServeJoin}
}

// jailKVMDirectServe serves lis until ctx ends and owns both goroutines it
// starts. ready runs once Serve is started. An early Serve end (no stop
// signal) is an error and the server is stopped. On ctx: GracefulStop runs
// bounded by b.Graceful; past it Stop runs and GracefulStop is joined
// (b.GracefulJoin); then the sole receive of Serve's result is bounded by
// b.ServeJoin. grpc.ErrServerStopped is a clean end.
func jailKVMDirectServe(ctx context.Context, srv jailKVMServer, lis net.Listener, ready func(), b jailKVMServeBudget) error {
	served := make(chan error, 1)
	go func() { served <- srv.Serve(lis) }()
	ready()
	select {
	case err := <-served:
		srv.Stop()
		return fmt.Errorf("direct serve ended before a stop signal: %w", cmp.Or(err, errors.New("no error")))
	case <-ctx.Done():
	}
	var errs []error
	graceful := make(chan struct{})
	go func() {
		srv.GracefulStop()
		close(graceful)
	}()
	select {
	case <-graceful:
	case <-time.After(b.Graceful):
		srv.Stop()
		select {
		case <-graceful:
		case <-time.After(b.GracefulJoin):
			errs = append(errs, fmt.Errorf("direct GracefulStop did not return within %v of Stop", b.GracefulJoin))
		}
	}
	select {
	case err := <-served:
		if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			errs = append(errs, fmt.Errorf("direct serve: %w", err))
		}
	case <-time.After(b.ServeJoin):
		errs = append(errs, fmt.Errorf("direct serve did not return within %v of the stop", b.ServeJoin))
	}
	return errors.Join(errs...)
}
