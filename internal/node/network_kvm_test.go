package node

// Real-TAP and real-SSH acceptance for node networking.
//
// This file is test-only. It owns no production launcher, lifecycle or cleanup
// code. The node and QEMU always run as the unprivileged review user; the root
// test process is only a throwaway-VM supervisor for network namespaces and
// OpenSSH clients. It never starts QEMU itself.
//
// Prerequisites (a throwaway test VM only). Compile as the normal VM user
// first; sudo only the setup and the already-built test binary, never
// `sudo go test` (secure_path loses the pinned Go toolchain and would build
// the binary as root):
//
//	go test -c -o /tmp/bbraft-node-network.test ./internal/node
//	sudo env BBRAFT_TEST_VM_SETUP=1 BBRAFT_TEST_KVM_NET_SETUP=1 \
//	  sh internal/node/network-test-setup.sh
//	sudo env BBRAFT_TEST_KVM_NET=1 BBRAFT_TEST_KVM=1 \
//	  BBRAFT_TEST_IMAGE_DIR=/path/to/images/out \
//	  BBRAFT_TEST_REQUIRE_ALL=1 \
//	  /tmp/bbraft-node-network.test -test.run '^TestKVMNetwork' \
//	  -test.v -test.count=1 -test.timeout 60m
//
// Optional M3 test-only fallback (throwaway VM lifetime only; never the final
// pinned-v1 gate): add BBRAFT_TEST_KVM_NET_UNPINNED=1 to the last command. It
// selects only a fully consistent -unpinned-test image; without the opt-in a
// collection holding only that fallback is refused.
//
// A normal run skips every test here. Once BBRAFT_TEST_KVM_NET=1 is set, a
// missing prerequisite is a failure rather than a skip: this is the
// authoritative A8 gate.
//
// The containment boundary is deliberately narrow:
//
//   - ip netns exec nodeNS unshare --pid --fork --mount-proc --kill-child=KILL
//     -- setpriv --reuid=... --regid=... --init-groups
//     --inh-caps=+net_admin --ambient-caps=+net_admin
//     --bounding-set=-all,+net_admin --pdeathsig keep -- TESTBIN ...
//
// Ubuntu 24.04 ships util-linux 2.39.3. Verified in util-linux v2.39.3:
// unshare.c has no --forward-signals and blocks SIGTERM/SIGINT in the unshare
// parent, so this test does not use that flag. Normal shutdown is an EOF on
// the parent-owned stdin pipe, which the nonroot helper observes before any
// host I/O and uses to cancel and shut the manager down. Only if that bounded
// wait fails does the supervisor call the managed exec.Cmd.Process.Kill and
// Wait; that is a single Go os.Process handle, never a second manually managed
// pidfd, so it cannot race the one reaper.
//
// setpriv --pdeathsig=keep is required in util-linux 2.39.3: unshare
// --kill-child installs PDEATHSIG before exec, but a UID/GID change clears it,
// and setpriv restores it after dropping credentials only when asked.
//
// --kill-child=KILL and PID-namespace teardown are containment backstops, not
// proof of cleanup. The test only deletes a network namespace after observing
// `ip netns pids` empty. If that observation fails, it leaves the namespaces
// in place for inspection and fails loudly instead of deleting a namespace
// while a process may still live in it. The guest CID allocator is production
// code owned elsewhere; this test uses the Manager defaults and never invents
// a CID workaround.

import (
	"bytes"
	"cmp"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json/v2"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/netip"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/ids"
	nodenet "github.com/nathabonfim59/bamboo-raft/internal/node/net"
	"github.com/nathabonfim59/bamboo-raft/internal/node/qemu"
	"github.com/nathabonfim59/bamboo-raft/internal/node/storage"
	"github.com/nathabonfim59/bamboo-raft/internal/nodelink"
	"github.com/nathabonfim59/bamboo-raft/internal/nodetest"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

const (
	// kvmNetGateEnv is the authoritative gate. Set it only inside a throwaway
	// VM; every other prerequisite is then a failure instead of a skip.
	kvmNetGateEnv = "BBRAFT_TEST_KVM_NET"
	// kvmNetHelperEnv and kvmNetContainmentEnv select the re-executed helpers.
	kvmNetHelperEnv      = "BBRAFT_TEST_KVM_NET_HELPER"
	kvmNetContainmentEnv = "BBRAFT_TEST_KVM_NET_CONTAINMENT"
	// kvmNetProbeEnv selects the tiny TCP probe re-executed in the client
	// namespace.
	kvmNetProbeEnv = "BBRAFT_TEST_KVM_NET_PROBE"
	// kvmNetUIDEnv and kvmNetGIDEnv may override the SUDO_UID/SUDO_GID the
	// node and clients run as. Production never uses them.
	kvmNetUIDEnv = "BBRAFT_TEST_KVM_NET_UID"
	kvmNetGIDEnv = "BBRAFT_TEST_KVM_NET_GID"
	// kvmNetUnpinnedOptInEnv is the explicit, VM-only M3 fallback opt-in. It
	// never applies to the final pinned-v1 gate.
	kvmNetUnpinnedOptInEnv = "BBRAFT_TEST_KVM_NET_UNPINNED"

	kvmNetBridge       = "bbraft0"
	kvmNetPrefixText   = "10.42.0.0/16"
	kvmNetGatewayText  = "10.42.0.1"
	kvmNetNodeAddrText = "203.0.113.1"
	kvmNetClientText   = "203.0.113.2"
	kvmNetIngressIface = "up0"
	kvmNetSSHPortMin   = 30000
	kvmNetSSHPortMax   = 30010

	// capNetAdminBit is CAP_NET_ADMIN in the kernel capability bit numbering.
	capNetAdminBit = 12
)

// kvmNetNodeConfig is the JSON passed to the re-executed node helper.
type kvmNetNodeConfig struct {
	DataDir      string `json:"dataDir"`
	ImageDir     string `json:"imageDir"`
	ImageID      string `json:"imageId"`
	StateDir     string `json:"stateDir"`
	DirectSocket string `json:"directSocket,omitempty"`
	HubSocket    string `json:"hubSocket,omitempty"`
	ReadyFile    string `json:"readyFile"`
	NodeID       string `json:"nodeId"`
	Token        string `json:"token"`
}

// kvmNetContainmentConfig is the JSON passed to the containment helper.
type kvmNetContainmentConfig struct {
	ReadyFile string `json:"readyFile"`
}

// kvmNetProbeConfig is the JSON passed to the TCP probe helper.
type kvmNetProbeConfig struct {
	Address    string `json:"address"`
	ResultFile string `json:"resultFile"`
}

// kvmNetReady is published atomically by both helpers after their setup. It
// proves the helper runs as the expected nonroot identity, with the temporary
// ambient NET_ADMIN capability, in the node network namespace.
type kvmNetReady struct {
	ProxyAddr  string `json:"proxyAddr,omitempty"`
	UID        int    `json:"uid"`
	GID        int    `json:"gid"`
	CapEff     uint64 `json:"capEff"`
	CapAmb     uint64 `json:"capAmb"`
	NetNSInode uint64 `json:"netNsInode"`
	// SleeperPID is set only by the containment helper: the guest pid of its
	// one owned sleeper, which proves a live descendant before forced kill.
	SleeperPID int `json:"sleeperPid,omitempty"`
}

// kvmNetProbeResult is the bounded, typed result of one client-side TCP probe.
// Only connected, refused and timeout describe a completed dial; invalid and
// other are probe failures the acceptance assertions must reject.
type kvmNetProbeResult struct {
	OK    bool   `json:"ok"`
	Class string `json:"class"`
	Error string `json:"error,omitempty"`
}

const (
	kvmNetProbeConnected = "connected"
	kvmNetProbeRefused   = "refused"
	kvmNetProbeTimeout   = "timeout"
	kvmNetProbeInvalid   = "invalid"
	kvmNetProbeOther     = "other"
)

// kvmNetDialFailure is the only non-nil error kvmNetTopology.tcpProbe returns
// for an expected unreachable endpoint. Every other probe outcome is fatal.
type kvmNetDialFailure struct {
	Class  string
	Detail string
}

func (e *kvmNetDialFailure) Error() string {
	return fmt.Sprintf("tcp %s: %s", e.Class, e.Detail)
}

// TestKVMNetworkAcceptance is the authoritative A8 real-network gate. It runs
// the frozen Phase 1 conformance groups and the real SSH/scp/sftp lifecycle
// over both the direct node client and nodelink.
func TestKVMNetworkAcceptance(t *testing.T) {
	imageDir, imageID := kvmNetRequireAcceptance(t)
	for _, tc := range []struct {
		name string
		link bool
	}{
		{name: "Direct"},
		{name: "Link", link: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			suffix := "d"
			if tc.link {
				suffix = "l"
			}
			tp := kvmNetNewTopology(t, suffix)
			tp.createNamespaces(t, true)
			cfg := tp.nodeConfig(imageDir, imageID, tc.link)
			var client nodev1.NodeClient
			if tc.link {
				wait := tp.startHub(t, cfg.HubSocket, cfg.NodeID)
				tp.startNodeHelper(t, cfg)
				client = wait(t)
			} else {
				tp.startNodeHelper(t, cfg)
				client = tp.dialDirect(t, cfg.DirectSocket)
			}
			tp.client = client

			// A nested subtest is deliberate: nodetest registers the shared
			// VM's cleanup on this t, so it must finish before the resource
			// assertions below.
			t.Run("AllPhase1", func(t *testing.T) {
				nodetest.Run(t, client, nodetest.Options{
					Resources:   &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: 10},
					BootTimeout: 3 * time.Minute,
					FenceWatch:  3 * time.Minute,
					Phase1:      nodetest.AllPhase1,
				})
			})
			tp.sshAcceptance(t, client, cfg.StateDir)
			tp.assertNoSandboxResources(t, cfg.StateDir)
		})
	}
}

// TestKVMNetworkContainment proves the process boundary before any QEMU
// acceptance: the nonroot helper reaches readiness with ambient NET_ADMIN in
// the owned node namespace, then exits on stdin EOF; a second helper is killed
// by the managed os.Process handle and the PID namespace leaves no process
// behind. It needs no image.
func TestKVMNetworkContainment(t *testing.T) {
	kvmNetRequireBase(t)
	for _, tc := range []struct {
		name  string
		force bool
	}{
		{name: "EOF"},
		{name: "ForcedKill", force: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			suffix := "ce"
			if tc.force {
				suffix = "cf"
			}
			tp := kvmNetNewTopology(t, suffix)
			tp.createNamespaces(t, false)
			raw, err := json.Marshal(kvmNetContainmentConfig{
				ReadyFile: filepath.Join(tp.sockDir, "containment-ready.json"),
			})
			if err != nil {
				t.Fatal(err)
			}
			var cfg kvmNetContainmentConfig
			if err := json.Unmarshal(raw, &cfg); err != nil {
				t.Fatal(err)
			}
			p := tp.startContained(t,
				"TestKVMNetworkContainmentHelper",
				kvmNetContainmentEnv, string(raw), cfg.ReadyFile)
			ready := p.waitReady(t, cfg.ReadyFile)
			tp.assertHelperIdentity(t, ready)
			if ready.SleeperPID <= 0 {
				t.Fatalf("containment helper did not own a live sleeper at readiness")
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			pids, err := kvmNetNamespacePids(ctx, tp.nodeNS)
			cancel()
			if err != nil {
				t.Fatalf("list namespace pids at readiness: %v", err)
			}
			// unshare PID 1, the helper, and its sleeper are all alive in the
			// owned namespace. The follower count is independent evidence that
			// the forced kill has a real descendant to tear down.
			if len(pids) < 3 {
				t.Fatalf("namespace has %d pids at readiness, want unshare, helper and sleeper: %v", len(pids), pids)
			}
			if tc.force {
				p.forceKill(t)
			} else if !p.stop(t, false) {
				t.Fatal("helper did not stop on stdin EOF")
			}
			// t.Cleanup still verifies the namespace is empty and deletes it.
		})
	}
}

// TestKVMNetworkNamespaceCollision is the gated collision regression: a
// pre-existing empty namespace with the test's chosen name must survive the
// supervisor's cleanup. It fails against a cleanup that deletes by name alone.
func TestKVMNetworkNamespaceCollision(t *testing.T) {
	kvmNetRequireBase(t)
	tp := kvmNetNewTopology(t, "nc")
	kvmNetRun(t, "ip", "netns", "add", tp.nodeNS)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = kvmNetCommandOutput(ctx, "ip", "netns", "del", tp.nodeNS)
	})
	if tp.addOwnedNamespace(t, tp.nodeNS) {
		t.Fatalf("adopted a pre-existing namespace this attempt did not create")
	}
	// Close must have nothing to delete: the collision was never recorded.
	tp.Close()
	if _, err := os.Stat(filepath.Join(tp.nsRunDir, tp.nodeNS)); err != nil {
		t.Fatalf("cleanup deleted an unowned empty namespace: %v", err)
	}
}

// TestKVMNetworkNodeHelper is inert unless the root supervisor starts it in a
// namespace. It registers the EOF watcher before any manager or host I/O.
func TestKVMNetworkNodeHelper(t *testing.T) {
	raw := os.Getenv(kvmNetHelperEnv)
	if raw == "" {
		t.Skip("re-executed node helper; only run by TestKVMNetworkAcceptance")
	}
	var cfg kvmNetNodeConfig
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		t.Fatalf("decode helper config: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Register the parent-owned stdin EOF watcher before New, CheckHost or
	// any other host I/O. A parent crash closes this pipe too.
	go func() {
		_, _ = io.Copy(io.Discard, os.Stdin)
		cancel()
	}()
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	netMgr, err := nodenet.New(nodenet.Config{
		Bridge:       kvmNetBridge,
		Prefix:       netip.MustParsePrefix(kvmNetPrefixText),
		Gateway:      netip.MustParseAddr(kvmNetGatewayText),
		Resolvers:    []netip.Addr{netip.MustParseAddr("1.1.1.1")},
		IngressAddr:  netip.MustParseAddr(kvmNetNodeAddrText),
		IngressIface: kvmNetIngressIface,
		SSHPorts: nodenet.PortRange{
			Min: kvmNetSSHPortMin,
			Max: kvmNetSSHPortMax,
		},
		StateDir: cfg.StateDir,
	}, nodenet.Deps{Logger: log})
	if err != nil {
		t.Fatalf("nodenet.New: %v", err)
	}
	defer func() {
		if err := netMgr.Close(); err != nil {
			t.Errorf("nodenet.Close: %v", err)
		}
	}()
	if err := qemu.CheckHost(ctx, log); err != nil {
		t.Fatalf("qemu.CheckHost: %v", err)
	}
	if err := netMgr.CheckHost(ctx); err != nil {
		t.Fatalf("nodenet.CheckHost: %v", err)
	}
	// Manager.New owns the conservative restore/prune path; do not run an
	// additional Reconcile here against the same fresh state.

	cpuModel, allowHostCPU := kvmCPUOverride()
	mgr, err := New(ctx, Config{
		NodeID:       cfg.NodeID,
		DataDir:      cfg.DataDir,
		ImageDir:     cfg.ImageDir,
		DefaultImage: cfg.ImageID,
		CPUModel:     cpuModel,
		AllowHostCPU: allowHostCPU,
		Capacity:     &nodev1.Resources{Vcpus: 16, MemoryMib: 32 << 10, DiskGib: 200},
		BootTimeout:  3 * time.Minute,
		Network:      netMgr,
		Log:          log,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() {
		sctx, scancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer scancel()
		if err := mgr.Shutdown(sctx); err != nil {
			t.Errorf("Manager.Shutdown: %v", err)
		}
	}()

	srv := grpc.NewServer(nodelink.ServerOptions()...)
	mgr.Register(srv)
	ready := kvmNetSelfReady(t)
	switch {
	case cfg.DirectSocket != "":
		lis, err := net.Listen("unix", cfg.DirectSocket)
		if err != nil {
			t.Fatalf("listen direct socket: %v", err)
		}
		go func() { _ = srv.Serve(lis) }()
		defer srv.Stop()
		kvmNetWriteReady(t, cfg.ReadyFile, ready)
		<-ctx.Done()

	case cfg.HubSocket != "":
		shim, err := kvmNetNewShim(cfg.HubSocket)
		if err != nil {
			t.Fatalf("nodelink shim: %v", err)
		}
		defer func() {
			if cerr := shim.Close(); cerr != nil {
				t.Logf("nodelink shim close: %v", cerr)
			}
		}()
		serveDone := make(chan error, 1)
		go func() {
			serveDone <- nodelink.Serve(ctx, nodelink.DialConfig{
				Addr:       shim.Addr(),
				NodeID:     cfg.NodeID,
				Token:      cfg.Token,
				Log:        log,
				MinBackoff: 50 * time.Millisecond,
				MaxBackoff: 500 * time.Millisecond,
			}, srv)
		}()
		ready.ProxyAddr = shim.Addr()
		kvmNetWriteReady(t, cfg.ReadyFile, ready)
		<-ctx.Done()
		_ = shim.Close()
		select {
		case serr := <-serveDone:
			if serr != nil && ctx.Err() == nil {
				t.Errorf("nodelink.Serve: %v", serr)
			}
		case <-time.After(10 * time.Second):
			t.Error("nodelink.Serve did not stop after cancel")
		}

	default:
		t.Fatal("helper config has neither a direct nor a link socket")
	}
}

// TestKVMNetworkContainmentHelper is inert unless the containment supervisor
// starts it. It proves identity and waits for EOF or death.
func TestKVMNetworkContainmentHelper(t *testing.T) {
	raw := os.Getenv(kvmNetContainmentEnv)
	if raw == "" {
		t.Skip("re-executed containment helper; only run by TestKVMNetworkContainment")
	}
	var cfg kvmNetContainmentConfig
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		t.Fatalf("decode containment config: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// No manager or host I/O before this watcher is registered.
	go func() {
		_, _ = io.Copy(io.Discard, os.Stdin)
		cancel()
	}()
	// One owned descendant and exactly one Wait owner for it. The parent
	// observes it as an extra namespace pid before the forced unshare kill, and
	// normal EOF terminates it through the same managed os.Process handle.
	sleeper := exec.Command("sleep", "300")
	if err := sleeper.Start(); err != nil {
		t.Fatalf("start containment sleeper: %v", err)
	}
	sleeperDone := make(chan error, 1)
	go func() { sleeperDone <- sleeper.Wait() }()
	ready := kvmNetSelfReady(t)
	ready.SleeperPID = sleeper.Process.Pid
	kvmNetWriteReady(t, cfg.ReadyFile, ready)
	select {
	case <-ctx.Done():
		if err := sleeper.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
			t.Errorf("stop containment sleeper: %v", err)
		}
		select {
		case <-sleeperDone: // the single Wait owner
		case <-time.After(10 * time.Second):
			t.Fatalf("containment sleeper did not exit within 10s after managed Process.Kill")
		}
	case err := <-sleeperDone:
		t.Fatalf("containment sleeper exited before EOF: %v", err)
	}
}

// TestKVMNetworkTCPProbe is the tiny client-side reachability probe. It writes
// its result instead of failing so the parent can treat both reachable and
// unreachable as expected outcomes.
func TestKVMNetworkTCPProbe(t *testing.T) {
	raw := os.Getenv(kvmNetProbeEnv)
	if raw == "" {
		t.Skip("re-executed TCP probe; only run by TestKVMNetworkAcceptance")
	}
	var cfg kvmNetProbeConfig
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		t.Fatalf("decode probe config: %v", err)
	}
	var result kvmNetProbeResult
	if err := kvmNetValidateProbeAddress(cfg.Address); err != nil {
		result = kvmNetProbeResult{Class: kvmNetProbeInvalid, Error: err.Error()}
	} else {
		conn, err := net.DialTimeout("tcp", cfg.Address, 3*time.Second)
		switch {
		case err == nil:
			_ = conn.Close()
			result = kvmNetProbeResult{OK: true, Class: kvmNetProbeConnected}
		default:
			result = kvmNetProbeResult{Class: kvmNetProbeClass(err), Error: err.Error()}
		}
	}
	rawResult, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("encode probe result: %v", err)
	}
	if err := os.WriteFile(cfg.ResultFile, rawResult, 0o600); err != nil {
		t.Fatalf("write probe result: %v", err)
	}
}

func kvmNetValidateProbeAddress(addr string) error {
	ap, err := netip.ParseAddrPort(addr)
	if err != nil {
		return fmt.Errorf("address %q: %w", addr, err)
	}
	if !ap.Addr().IsValid() || ap.Addr().IsUnspecified() || ap.Port() == 0 {
		return fmt.Errorf("address %q is not a usable unicast endpoint", addr)
	}
	return nil
}

func kvmNetProbeClass(err error) string {
	switch {
	case err == nil:
		return kvmNetProbeConnected
	case errors.Is(err, syscall.ECONNREFUSED):
		return kvmNetProbeRefused
	case errors.Is(err, os.ErrDeadlineExceeded), errors.Is(err, syscall.ETIMEDOUT), os.IsTimeout(err):
		return kvmNetProbeTimeout
	}
	if netErr, ok := errors.AsType[net.Error](err); ok && netErr.Timeout() {
		return kvmNetProbeTimeout
	}
	return kvmNetProbeOther
}

func kvmNetDecodeProbeResult(raw []byte) (kvmNetProbeResult, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return kvmNetProbeResult{}, errors.New("empty probe result")
	}
	var result kvmNetProbeResult
	if err := json.Unmarshal(raw, &result); err != nil {
		return kvmNetProbeResult{}, fmt.Errorf("decode probe result: %w", err)
	}
	switch result.Class {
	case kvmNetProbeConnected:
		if !result.OK {
			return kvmNetProbeResult{}, errors.New("connected probe result is not ok")
		}
	case kvmNetProbeRefused, kvmNetProbeTimeout, kvmNetProbeInvalid, kvmNetProbeOther:
		if result.OK {
			return kvmNetProbeResult{}, fmt.Errorf("failed probe class %q is marked ok", result.Class)
		}
	default:
		return kvmNetProbeResult{}, fmt.Errorf("unknown probe class %q", result.Class)
	}
	if !result.OK && result.Error == "" {
		return kvmNetProbeResult{}, errors.New("failed probe result has no error detail")
	}
	return result, nil
}

// kvmNetTopology owns one throwaway topology and every child it starts.
type kvmNetTopology struct {
	t   *testing.T
	uid int
	gid int

	baseDir string
	sockDir string
	dataDir string
	workDir string

	nodeNS   string
	clientNS string

	// ownedNS records only namespaces this attempt created, pinned to the
	// inode observed immediately after `ip netns add`. Cleanup never acts on a
	// name absent here, and never deletes a name whose inode changed.
	ownedNS  map[string]uint64
	nsRunDir string

	helper         *kvmNetProcess
	client         nodev1.NodeClient
	closeTransport func()
	tracked        map[string]struct{}

	closeOnce sync.Once
}

func kvmNetNewTopology(t *testing.T, name string) *kvmNetTopology {
	t.Helper()
	uid, gid := kvmNetTestUser(t)
	tp := &kvmNetTopology{
		t:        t,
		uid:      uid,
		gid:      gid,
		nodeNS:   fmt.Sprintf("bbn%dn%s", os.Getpid(), name),
		ownedNS:  map[string]uint64{},
		nsRunDir: "/var/run/netns",
		tracked:  map[string]struct{}{},
	}
	t.Cleanup(tp.Close)
	tp.baseDir = kvmNetMkdirTemp(t, os.TempDir(), "bbnet-base-"+name+"-", uid, gid)
	tp.sockDir = kvmNetMkdirTemp(t, "/tmp", "bbnet-sock-"+name+"-", uid, gid)
	tp.dataDir = kvmNetMkdirTemp(t, os.TempDir(), "bbnet-data-"+name+"-", uid, gid)
	tp.workDir = kvmNetMkdirTemp(t, "/tmp", "bbnet-work-"+name+"-", uid, gid)
	tp.clientNS = tp.nodeNS + "c"
	for _, dir := range []string{tp.baseDir, tp.sockDir, tp.dataDir, tp.workDir} {
		kvmNetAssertNodeAccess(t, dir, uid, gid, true)
	}
	return tp
}

// helperExecutable returns a copy of this test binary that the unprivileged
// node helper can execute. `go test` may keep its binary below a root-only
// temporary directory when the supervisor itself runs under sudo; copying into
// the node-owned topology directory removes that accidental privilege
// coupling. The binary content is unchanged.
func (tp *kvmNetTopology) helperExecutable(t *testing.T) string {
	t.Helper()
	dst := filepath.Join(tp.baseDir, "helper.test")
	if info, err := os.Stat(dst); err == nil && info.Mode().IsRegular() {
		return dst
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("test executable: %v", err)
	}
	exe, err = filepath.EvalSymlinks(exe)
	if err != nil {
		t.Fatalf("resolve test executable: %v", err)
	}
	in, err := os.Open(exe)
	if err != nil {
		t.Fatalf("open test executable: %v", err)
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o755)
	if err != nil {
		t.Fatalf("create helper executable: %v", err)
	}
	_, copyErr := io.Copy(out, in)
	closeErr := out.Close()
	if copyErr != nil {
		t.Fatalf("copy helper executable: %v", copyErr)
	}
	if closeErr != nil {
		t.Fatalf("close helper executable: %v", closeErr)
	}
	if err := os.Chmod(dst, 0o755); err != nil {
		t.Fatalf("chmod helper executable: %v", err)
	}
	if err := os.Chown(dst, tp.uid, tp.gid); err != nil {
		t.Fatalf("chown helper executable: %v", err)
	}
	kvmNetAssertNodeAccess(t, dst, tp.uid, tp.gid, false)
	return dst
}

func kvmNetAssertNodeAccess(t *testing.T, path string, uid, gid int, write bool) {
	t.Helper()
	script := `test -r "$1" && test -x "$1"`
	if write {
		script = `test -w "$1" && test -x "$1"`
	}
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	out, err := kvmNetCommandOutput(ctx, "setpriv",
		"--reuid="+strconv.Itoa(uid),
		"--regid="+strconv.Itoa(gid),
		"--init-groups",
		"--", "sh", "-c", script, "sh", path,
	)
	if err != nil {
		t.Fatalf("node user %d:%d cannot access %s: %v: %s", uid, gid, path, err, out)
	}
}

func kvmNetMkdirTemp(t *testing.T, parent, pattern string, uid, gid int) string {
	t.Helper()
	dir, err := os.MkdirTemp(parent, pattern)
	if err != nil {
		t.Fatalf("create %s: %v", pattern, err)
	}
	if err := os.Chmod(dir, 0o755); err != nil {
		_ = os.RemoveAll(dir)
		t.Fatalf("chmod %s: %v", dir, err)
	}
	if err := os.Chown(dir, uid, gid); err != nil {
		_ = os.RemoveAll(dir)
		t.Fatalf("chown %s: %v", dir, err)
	}
	return dir
}

func (tp *kvmNetTopology) nodeConfig(imageDir, imageID string, link bool) kvmNetNodeConfig {
	cfg := kvmNetNodeConfig{
		DataDir:   tp.dataDir,
		ImageDir:  imageDir,
		ImageID:   imageID,
		StateDir:  filepath.Join(tp.dataDir, "net"),
		ReadyFile: filepath.Join(tp.sockDir, "node-ready.json"),
		NodeID:    "kvm-net-test",
		Token:     "test-token",
	}
	if link {
		cfg.HubSocket = filepath.Join(tp.sockDir, "hub.sock")
	} else {
		cfg.DirectSocket = filepath.Join(tp.sockDir, "node.sock")
	}
	return cfg
}

// addOwnedNamespace creates name only when the path is absent, and records its
// inode immediately after a successful add. A pre-existing name is reported and
// left untouched: this attempt does not own it and cleanup must never delete
// it, even when it is empty.
func (tp *kvmNetTopology) addOwnedNamespace(t *testing.T, name string) bool {
	t.Helper()
	path := filepath.Join(tp.nsRunDir, name)
	if _, err := os.Stat(path); err == nil {
		t.Logf("namespace %s already exists; refusing to adopt or delete it", name)
		return false
	} else if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stat namespace %s: %v", name, err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	out, err := kvmNetCommandOutput(ctx, "ip", "netns", "add", name)
	if err != nil {
		t.Logf("ip netns add %s: %v: %s", name, err, out)
		return false
	}
	inode, err := kvmNetInode(path)
	if err != nil {
		// Record the name with an unknown identity so cleanup refuses to delete
		// it; the failure still points at the namespace for inspection.
		tp.ownedNS[name] = 0
		t.Fatalf("namespace %s was created but its identity could not be pinned: %v", name, err)
	}
	tp.ownedNS[name] = inode
	return true
}

func (tp *kvmNetTopology) createNamespaces(t *testing.T, withNetwork bool) {
	t.Helper()
	if !tp.addOwnedNamespace(t, tp.nodeNS) {
		t.Fatalf("node namespace %s already exists or could not be created; refusing to adopt or delete it", tp.nodeNS)
	}
	if !withNetwork {
		return
	}
	if !tp.addOwnedNamespace(t, tp.clientNS) {
		t.Fatalf("client namespace %s already exists or could not be created; refusing to adopt or delete it", tp.clientNS)
	}

	kvmNetRun(t, "ip", "netns", "exec", tp.nodeNS,
		"ip", "link", "add", "up0", "type", "veth", "peer", "name", "up1")
	kvmNetRun(t, "ip", "netns", "exec", tp.nodeNS,
		"ip", "link", "set", "up1", "netns", tp.clientNS)
	kvmNetRun(t, "ip", "netns", "exec", tp.nodeNS,
		"ip", "addr", "add", kvmNetNodeAddrText+"/24", "dev", "up0")
	kvmNetRun(t, "ip", "netns", "exec", tp.nodeNS, "ip", "link", "set", "up0", "up")
	kvmNetRun(t, "ip", "netns", "exec", tp.nodeNS, "ip", "link", "set", "lo", "up")
	kvmNetRun(t, "ip", "netns", "exec", tp.clientNS,
		"ip", "addr", "add", kvmNetClientText+"/24", "dev", "up1")
	kvmNetRun(t, "ip", "netns", "exec", tp.clientNS, "ip", "link", "set", "up1", "up")
	kvmNetRun(t, "ip", "netns", "exec", tp.clientNS, "ip", "link", "set", "lo", "up")
	kvmNetRun(t, "ip", "netns", "exec", tp.nodeNS,
		"sh", "-c", "echo 1 > /proc/sys/net/ipv4/ip_forward")
}

func (tp *kvmNetTopology) startHub(t *testing.T, path, nodeID string) func(*testing.T) nodev1.NodeClient {
	t.Helper()
	lis, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("listen hub socket: %v", err)
	}
	if err := os.Chown(path, tp.uid, tp.gid); err != nil {
		_ = lis.Close()
		t.Fatalf("chown hub socket: %v", err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		_ = lis.Close()
		t.Fatalf("chmod hub socket: %v", err)
	}
	hub := nodelink.NewHub(func(context.Context, nodelink.Hello) error { return nil }, slog.New(slog.DiscardHandler))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = hub.Serve(ctx, lis)
		close(done)
	}()
	tp.closeTransport = func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			tp.t.Errorf("nodelink hub did not finish its cleanup within 10s")
		}
	}
	return func(t *testing.T) nodev1.NodeClient {
		t.Helper()
		wctx, wcancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer wcancel()
		link, err := hub.Wait(wctx, nodeID)
		if err != nil {
			t.Fatalf("node never connected through nodelink: %v\n%s", err, tp.helperTail())
		}
		return link.Client
	}
}

func (tp *kvmNetTopology) startNodeHelper(t *testing.T, cfg kvmNetNodeConfig) {
	t.Helper()
	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("encode helper config: %v", err)
	}
	p := tp.startContained(t, "TestKVMNetworkNodeHelper", kvmNetHelperEnv, string(raw), cfg.ReadyFile)
	ready := p.waitReady(t, cfg.ReadyFile)
	tp.assertHelperIdentity(t, ready)
	if cfg.HubSocket != "" && ready.ProxyAddr == "" {
		t.Fatalf("link helper reported no shim address")
	}
	if cfg.DirectSocket != "" && ready.ProxyAddr != "" {
		t.Fatalf("direct helper reported a link shim address %q", ready.ProxyAddr)
	}
}

func (tp *kvmNetTopology) startContained(t *testing.T, testName, envName, envValue, readyPath string) *kvmNetProcess {
	t.Helper()
	exe := tp.helperExecutable(t)
	logPath := filepath.Join(tp.baseDir, testName+".log")
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatalf("open helper log: %v", err)
	}
	stdinR, stdinW, err := os.Pipe()
	if err != nil {
		_ = logFile.Close()
		t.Fatalf("helper stdin pipe: %v", err)
	}
	args := []string{
		"netns", "exec", tp.nodeNS,
		"unshare", "--pid", "--fork", "--mount-proc", "--kill-child=KILL", "--",
		"setpriv",
		"--reuid=" + strconv.Itoa(tp.uid),
		"--regid=" + strconv.Itoa(tp.gid),
		"--init-groups",
		"--inh-caps=+net_admin",
		"--ambient-caps=+net_admin",
		"--bounding-set=-all,+net_admin",
		"--pdeathsig", "keep",
		"--",
		exe, "-test.run=^" + testName + "$", "-test.v",
	}
	cmd := exec.Command("ip", args...)
	cmd.Dir = tp.baseDir
	cmd.Env = kvmNetEnvWith(os.Environ(), envName, envValue)
	cmd.Stdin = stdinR
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		_ = stdinR.Close()
		_ = stdinW.Close()
		_ = logFile.Close()
		t.Fatalf("start contained helper: %v", err)
	}
	// Drop the parent's duplicate read end; only the helper keeps one. EOF is
	// produced by closing the write end in closeStdin, so an extra read end
	// cannot delay it.
	_ = stdinR.Close()

	p := &kvmNetProcess{
		name:      testName,
		cmd:       cmd,
		stdinW:    stdinW,
		logFile:   logFile,
		logPath:   logPath,
		readyPath: readyPath,
		done:      make(chan struct{}),
	}
	tp.helper = p
	go func() {
		err := cmd.Wait()
		p.mu.Lock()
		p.waitErr = err
		p.mu.Unlock()
		close(p.done)
	}()
	return p
}

func (tp *kvmNetTopology) dialDirect(t *testing.T, path string) nodev1.NodeClient {
	t.Helper()
	conn, err := grpc.NewClient(
		"passthrough:///node",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", path)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(nodelink.MaxResponseBytes)),
	)
	if err != nil {
		t.Fatalf("dial direct node socket: %v", err)
	}
	tp.closeTransport = func() { _ = conn.Close() }
	client := nodev1.NewNodeClient(conn)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	if _, err := client.Info(ctx, &nodev1.InfoRequest{}); err != nil {
		t.Fatalf("direct node Info: %v", err)
	}
	return client
}

// kvmNetValidateIdentity enforces the exact temporary privilege boundary: the
// helper is the review uid/gid, and both effective and ambient capability sets
// are NET_ADMIN alone. Merely containing NET_ADMIN is not enough.
func kvmNetValidateIdentity(ready kvmNetReady, uid, gid int) error {
	if uid <= 0 || gid <= 0 {
		return fmt.Errorf("expected helper identity %d:%d is not nonroot", uid, gid)
	}
	if ready.UID != uid || ready.GID != gid {
		return fmt.Errorf("helper identity = %d:%d, want nonroot %d:%d", ready.UID, ready.GID, uid, gid)
	}
	want := uint64(1) << capNetAdminBit
	if ready.CapEff != want {
		return fmt.Errorf("helper effective capabilities %#x, want exactly CAP_NET_ADMIN %#x", ready.CapEff, want)
	}
	if ready.CapAmb != want {
		return fmt.Errorf("helper ambient capabilities %#x, want exactly CAP_NET_ADMIN %#x", ready.CapAmb, want)
	}
	return nil
}

func (tp *kvmNetTopology) assertHelperIdentity(t *testing.T, ready kvmNetReady) {
	t.Helper()
	if err := kvmNetValidateIdentity(ready, tp.uid, tp.gid); err != nil {
		t.Fatalf("%v", err)
	}
	info, err := os.Stat(filepath.Join(tp.nsRunDir, tp.nodeNS))
	if err != nil {
		t.Fatalf("stat node namespace: %v", err)
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatalf("node namespace stat has unexpected type %T", info.Sys())
	}
	if uint64(st.Ino) != ready.NetNSInode {
		t.Fatalf("helper is in netns inode %d, want node namespace %d", ready.NetNSInode, st.Ino)
	}
}

func (tp *kvmNetTopology) helperTail() string {
	if tp.helper == nil {
		return ""
	}
	return tp.helper.logTail()
}

func (tp *kvmNetTopology) track(id string) {
	tp.tracked[id] = struct{}{}
}

func (tp *kvmNetTopology) untrack(id string) {
	delete(tp.tracked, id)
}

func (tp *kvmNetTopology) Close() {
	tp.closeOnce.Do(func() {
		// Best effort while the node may still answer. The test body normally
		// deletes its own VM; nodetest cleans up its nested VMs before this.
		if tp.client != nil && len(tp.tracked) > 0 {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			for id := range tp.tracked {
				if _, err := tp.client.DeleteVM(ctx, &nodev1.DeleteVMRequest{SandboxId: id}); err != nil {
					tp.t.Errorf("cleanup DeleteVM(%s): %v", id, err)
				}
			}
			cancel()
		}
		if tp.closeTransport != nil {
			tp.closeTransport()
			tp.closeTransport = nil
		}
		proven := true
		if tp.helper != nil {
			proven = tp.helper.stop(tp.t, false)
		}
		if !proven {
			tp.t.Logf("leaving namespaces %s and %s for inspection: helper stop was not proven", tp.nodeNS, tp.clientNS)
			return
		}
		if !tp.deleteNamespaces(tp.t) {
			return
		}
		for _, dir := range []string{tp.baseDir, tp.sockDir, tp.dataDir, tp.workDir} {
			if dir == "" {
				continue
			}
			if err := os.RemoveAll(dir); err != nil {
				tp.t.Logf("remove %s: %v", dir, err)
			}
		}
	})
}

// kvmNetMayDeleteNamespace is the pure ownership check cleanup uses. A name is
// deletable only when this attempt recorded it and the observed inode still
// matches; a pre-existing collision or a reused name is never deleted.
func kvmNetMayDeleteNamespace(owned map[string]uint64, name string, observed uint64) (bool, string) {
	inode, ok := owned[name]
	if !ok {
		return false, "not created by this attempt"
	}
	if inode == 0 {
		return false, "identity was never pinned"
	}
	if inode != observed {
		return false, fmt.Sprintf("inode changed from %d to %d", inode, observed)
	}
	return true, ""
}

// deleteNamespaces acts only on namespaces this attempt created and pinned.
// It observes `ip netns pids` empty before deleting; a nonempty or changed
// namespace is left in place. kill-child alone is never treated as proof.
func (tp *kvmNetTopology) deleteNamespaces(t *testing.T) bool {
	t.Helper()
	names := make([]string, 0, len(tp.ownedNS))
	for name := range tp.ownedNS {
		names = append(names, name)
	}
	slices.Sort(names)
	for _, name := range names {
		path := filepath.Join(tp.nsRunDir, name)
		info, err := os.Stat(path)
		if errors.Is(err, os.ErrNotExist) {
			t.Logf("owned namespace %s is already absent", name)
			continue
		}
		if err != nil {
			t.Errorf("stat namespace %s: %v", name, err)
			return false
		}
		st, ok := info.Sys().(*syscall.Stat_t)
		if !ok {
			t.Errorf("namespace %s stat has unexpected type %T", name, info.Sys())
			return false
		}
		if may, reason := kvmNetMayDeleteNamespace(tp.ownedNS, name, uint64(st.Ino)); !may {
			t.Errorf("refusing to delete namespace %s: %s", name, reason)
			return false
		}
		deadline := time.Now().Add(5 * time.Second)
		for {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			pids, err := kvmNetNamespacePids(ctx, name)
			cancel()
			if err != nil {
				t.Errorf("ip netns pids %s: %v", name, err)
				return false
			}
			if len(pids) == 0 {
				break
			}
			if time.Now().After(deadline) {
				t.Errorf("namespace %s still has pids %v; leaving it for inspection", name, pids)
				for _, pid := range pids {
					cmdline, _ := os.ReadFile(filepath.Join("/proc", pid, "cmdline"))
					t.Logf("namespace %s pid %s: %s", name, pid, strings.ReplaceAll(string(cmdline), "\x00", " "))
				}
				return false
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	for _, name := range names {
		path := filepath.Join(tp.nsRunDir, name)
		info, err := os.Stat(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			t.Errorf("stat namespace %s before delete: %v", name, err)
			return false
		}
		st, ok := info.Sys().(*syscall.Stat_t)
		if !ok {
			t.Errorf("namespace %s pre-delete stat has unexpected type %T", name, info.Sys())
			return false
		}
		// Re-check immediately before deletion: polling other namespaces may
		// have let this name be removed and recreated since the first pass.
		if may, reason := kvmNetMayDeleteNamespace(tp.ownedNS, name, uint64(st.Ino)); !may {
			t.Errorf("refusing to delete namespace %s: %s", name, reason)
			return false
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		out, err := kvmNetCommandOutput(ctx, "ip", "netns", "del", name)
		cancel()
		if err != nil {
			t.Errorf("ip netns del %s: %v: %s", name, err, out)
			return false
		}
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("namespace %s survived deletion (stat: %v)", name, err)
			return false
		}
	}
	return true
}

func (tp *kvmNetTopology) assertNoSandboxResources(t *testing.T, stateDir string) {
	t.Helper()
	entries, err := os.ReadDir(stateDir)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("read network state dir: %v", err)
	}
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".json") {
			t.Errorf("network allocation state file %q remains after delete", entry.Name())
		}
	}
	out := kvmNetRun(t, "ip", "netns", "exec", tp.nodeNS, "ip", "link", "show", "type", "tun")
	if strings.TrimSpace(out) != "" {
		t.Errorf("TAP links remain after delete:\n%s", out)
	}
}

// kvmNetProcess owns one managed exec.Cmd. Exactly one goroutine calls Wait;
// every other path only observes done.
type kvmNetProcess struct {
	name      string
	cmd       *exec.Cmd
	stdinW    *os.File
	logFile   *os.File
	logPath   string
	readyPath string

	closeStdinOnce sync.Once
	stopOnce       sync.Once
	stopResult     bool
	forced         bool

	mu      sync.Mutex
	waitErr error
	done    chan struct{}
}

func (p *kvmNetProcess) closeStdin() {
	p.closeStdinOnce.Do(func() {
		if p.stdinW != nil {
			_ = p.stdinW.Close()
		}
	})
}

func (p *kvmNetProcess) waitFor(d time.Duration) bool {
	if d <= 0 {
		select {
		case <-p.done:
			return true
		default:
			return false
		}
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-p.done:
		return true
	case <-timer.C:
		return false
	}
}

func (p *kvmNetProcess) err() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.waitErr
}

// stop implements the normal path: EOF on stdin, then a bounded Wait. Only
// when that fails does it use the managed os.Process handle to kill and Wait.
// It returns true only after the one reaper observed the child exit.
func (p *kvmNetProcess) stop(t *testing.T, allowForce bool) bool {
	t.Helper()
	p.stopOnce.Do(func() {
		p.closeStdin()
		if p.waitFor(30 * time.Second) {
			p.stopResult = true
			if !p.forced {
				if err := p.err(); err != nil {
					t.Errorf("helper %s exited with error: %v\n%s", p.name, err, p.logTail())
				}
			}
			return
		}
		t.Logf("helper %s did not exit on stdin EOF; forcing managed Process.Kill", p.name)
		if err := p.cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
			t.Errorf("helper %s: Process.Kill: %v", p.name, err)
		}
		if !p.waitFor(10 * time.Second) {
			t.Errorf("helper %s survived managed Process.Kill; leaving namespaces for inspection", p.name)
			return
		}
		p.forced = true
		if !allowForce {
			t.Errorf("helper %s needed forced termination; stdin EOF did not stop it\n%s", p.name, p.logTail())
		}
		p.stopResult = true
	})
	if p.logFile != nil && p.stopResult {
		_ = p.logFile.Close()
	}
	return p.stopResult
}

// forceKill is the deliberate containment test path: no EOF, just the managed
// os.Process handle and the one reaper.
func (p *kvmNetProcess) forceKill(t *testing.T) {
	t.Helper()
	if p.waitFor(0) {
		t.Fatalf("helper %s exited before the forced-kill path", p.name)
	}
	if err := p.cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
		t.Fatalf("helper %s: Process.Kill: %v", p.name, err)
	}
	if !p.waitFor(15 * time.Second) {
		t.Fatalf("helper %s survived Process.Kill\n%s", p.name, p.logTail())
	}
	p.forced = true
}

func (p *kvmNetProcess) waitReady(t *testing.T, path string) kvmNetReady {
	t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	for {
		raw, err := os.ReadFile(path)
		if err == nil {
			var ready kvmNetReady
			if err := json.Unmarshal(raw, &ready); err != nil {
				t.Fatalf("decode helper ready file %s: %v", path, err)
			}
			return ready
		}
		if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("read helper ready file %s: %v", path, err)
		}
		if p.waitFor(0) {
			t.Fatalf("helper %s exited before readiness: %v\n%s", p.name, p.err(), p.logTail())
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for helper %s readiness at %s\n%s", p.name, path, p.logTail())
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// logTail returns a fixed diagnostic window from the helper log. The log file
// itself is not size-capped; this read is bounded so a helper that keeps
// writing cannot turn a failure report into an unbounded ReadAll. The section
// length is fixed after Stat, so later growth is ignored rather than followed.
func (p *kvmNetProcess) logTail() string {
	f, err := os.Open(p.logPath)
	if err != nil {
		return ""
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return ""
	}
	const window int64 = 64 << 10
	offset := max(0, info.Size()-window)
	raw, err := io.ReadAll(io.NewSectionReader(f, offset, window))
	if err != nil {
		return ""
	}
	return string(raw)
}

// kvmNetShim forwards the node's TCP nodelink dial to the parent's unix
// socket. nodelink hardcodes tcp; this shim changes no bytes and no protocol.
type kvmNetShim struct {
	lis    net.Listener
	target string

	mu      sync.Mutex
	closed  bool
	conns   map[net.Conn]struct{}
	wg      sync.WaitGroup
	once    sync.Once
	closeEr error
}

func kvmNetNewShim(target string) (*kvmNetShim, error) {
	lis, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("listen shim: %w", err)
	}
	s := &kvmNetShim{lis: lis, target: target, conns: map[net.Conn]struct{}{}}
	s.wg.Go(s.accept)
	return s, nil
}

func (s *kvmNetShim) Addr() string { return s.lis.Addr().String() }

func (s *kvmNetShim) accept() {
	for {
		conn, err := s.lis.Accept()
		if err != nil {
			return
		}
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			_ = conn.Close()
			return
		}
		s.conns[conn] = struct{}{}
		s.wg.Go(func() { s.serve(conn) })
		s.mu.Unlock()
	}
}

func (s *kvmNetShim) serve(client net.Conn) {
	defer client.Close()
	upstream, err := net.Dial("unix", s.target)
	if err != nil {
		return
	}
	defer upstream.Close()

	one := make(chan struct{}, 2)
	var copyers sync.WaitGroup
	copyers.Go(func() {
		_, _ = io.Copy(upstream, client)
		one <- struct{}{}
	})
	copyers.Go(func() {
		_, _ = io.Copy(client, upstream)
		one <- struct{}{}
	})
	<-one
	_ = client.Close()
	_ = upstream.Close()
	copyers.Wait()
}

func (s *kvmNetShim) Close() error {
	s.once.Do(func() {
		s.mu.Lock()
		s.closed = true
		conns := make([]net.Conn, 0, len(s.conns))
		for conn := range s.conns {
			conns = append(conns, conn)
		}
		listener := s.lis
		s.mu.Unlock()
		if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			s.closeEr = errors.Join(s.closeEr, fmt.Errorf("close shim listener: %w", err))
		}
		for _, conn := range conns {
			if err := conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				s.closeEr = errors.Join(s.closeEr, fmt.Errorf("close shim conn: %w", err))
			}
		}
		done := make(chan struct{})
		go func() {
			s.wg.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			s.closeEr = errors.Join(s.closeEr, errors.New("shim close timed out waiting for copiers"))
		}
	})
	return s.closeEr
}

func kvmNetSelfReady(t *testing.T) kvmNetReady {
	t.Helper()
	capEff, capAmb, err := kvmNetCaps()
	if err != nil {
		t.Fatalf("read helper capabilities: %v", err)
	}
	ino, err := kvmNetInode("/proc/self/ns/net")
	if err != nil {
		t.Fatalf("read helper netns inode: %v", err)
	}
	return kvmNetReady{
		UID:        os.Getuid(),
		GID:        os.Getgid(),
		CapEff:     capEff,
		CapAmb:     capAmb,
		NetNSInode: ino,
	}
}

func kvmNetCaps() (effective, ambient uint64, err error) {
	raw, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0, 0, err
	}
	var foundEff, foundAmb bool
	for line := range strings.SplitSeq(string(raw), "\n") {
		switch {
		case strings.HasPrefix(line, "CapEff:"):
			effective, err = strconv.ParseUint(strings.TrimSpace(strings.TrimPrefix(line, "CapEff:")), 16, 64)
			foundEff = true
		case strings.HasPrefix(line, "CapAmb:"):
			ambient, err = strconv.ParseUint(strings.TrimSpace(strings.TrimPrefix(line, "CapAmb:")), 16, 64)
			foundAmb = true
		}
		if err != nil {
			return 0, 0, err
		}
	}
	if !foundEff || !foundAmb {
		return 0, 0, errors.New("CapEff or CapAmb missing from /proc/self/status")
	}
	return effective, ambient, nil
}

func kvmNetInode(path string) (uint64, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, fmt.Errorf("stat %s: unexpected type %T", path, info.Sys())
	}
	return uint64(st.Ino), nil
}

func kvmNetWriteReady(t *testing.T, path string, ready kvmNetReady) {
	t.Helper()
	raw, err := json.Marshal(ready)
	if err != nil {
		t.Fatalf("encode ready file: %v", err)
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		t.Fatalf("write ready temp: %v", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		t.Fatalf("publish ready file: %v", err)
	}
}

func kvmNetEnvWith(env []string, key, value string) []string {
	out := make([]string, 0, len(env)+1)
	for _, entry := range env {
		if strings.HasPrefix(entry, key+"=") {
			continue
		}
		out = append(out, entry)
	}
	return append(out, key+"="+value)
}

// --- supervisor gate and image selection -----------------------------------

func kvmNetRequireBase(t *testing.T) {
	t.Helper()
	if os.Getenv(kvmNetGateEnv) != "1" {
		t.Skipf("set %s=1 to run the throwaway-VM TAP/SSH acceptance tests", kvmNetGateEnv)
	}
	if os.Geteuid() != 0 {
		t.Fatalf("%s=1 requires root: run it through sudo in a throwaway VM; the node and QEMU still run nonroot", kvmNetGateEnv)
	}
	for _, binary := range []string{"ip", "nft", "unshare", "setpriv"} {
		if _, err := exec.LookPath(binary); err != nil {
			t.Fatalf("%s is required: %v (run internal/node/network-test-setup.sh)", binary, err)
		}
	}
	for _, probe := range []struct {
		binary string
		want   string
	}{
		{"unshare", "--kill-child"},
		{"setpriv", "--pdeathsig"},
	} {
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		out, err := kvmNetCommandOutput(ctx, probe.binary, "--help")
		cancel()
		if err != nil {
			t.Fatalf("%s --help: %v", probe.binary, err)
		}
		if !bytes.Contains(out, []byte(probe.want)) {
			t.Fatalf("%s does not support %s; util-linux 2.39.3 is required", probe.binary, probe.want)
		}
	}
	_, _ = kvmNetTestUser(t)
}

func kvmNetRequireAcceptance(t *testing.T) (imageDir, imageID string) {
	t.Helper()
	kvmNetRequireBase(t)
	if os.Getenv("BBRAFT_TEST_KVM") != "1" {
		t.Fatalf("%s=1 requires BBRAFT_TEST_KVM=1", kvmNetGateEnv)
	}
	// BBRAFT_TEST_IMAGE_DIR is the image collection root (for example
	// images/out), matching storage.VerifyImages; the picker below enumerates
	// its version subdirectories.
	imageDir = os.Getenv("BBRAFT_TEST_IMAGE_DIR")
	if imageDir == "" {
		t.Fatalf("%s=1 requires BBRAFT_TEST_IMAGE_DIR", kvmNetGateEnv)
	}
	info, err := os.Stat(imageDir)
	if err != nil || !info.IsDir() {
		t.Fatalf("BBRAFT_TEST_IMAGE_DIR %q: %v", imageDir, err)
	}
	for _, binary := range []string{"ssh", "scp", "sftp"} {
		if _, err := exec.LookPath(binary); err != nil {
			t.Fatalf("%s is required: %v (run internal/node/network-test-setup.sh)", binary, err)
		}
	}
	for _, device := range []string{"/dev/kvm", "/dev/vhost-vsock", "/dev/net/tun", "/dev/vhost-net"} {
		info, err := os.Stat(device)
		if err != nil {
			t.Fatalf("%s is required: %v", device, err)
		}
		if info.Mode()&os.ModeCharDevice == 0 {
			t.Fatalf("%s is not a character device", device)
		}
	}
	uid, gid := kvmNetTestUser(t)
	kvmNetAssertNodeAccess(t, imageDir, uid, gid, false)
	allowUnpinned, err := kvmNetUnpinnedOptIn()
	if err != nil {
		t.Fatalf("%v", err)
	}
	return imageDir, kvmNetPickImage(t, imageDir, allowUnpinned)
}

const kvmNetTestImageSuffix = "-unpinned-test"

// kvmNetImageManifest is the subset of the node-facing manifest this selector
// owns. storage.VerifyImages still validates artifacts and hashes.
type kvmNetImageManifest struct {
	ID           string `json:"id"`
	Distribution struct {
		Snapshot string `json:"snapshot"`
		Mirror   string `json:"mirror"`
	} `json:"distribution"`
}

type kvmNetImageKind int

const (
	kvmNetImagePinned kvmNetImageKind = iota
	kvmNetImageFallback
)

// kvmNetClassifyImage returns pinned for an ordinary manifest and fallback for
// a fully consistent test-only image. Any partial marker set is an error, never
// a silently skipped candidate. Detection covers all three contracted markers
// so a hand-edited mirror-only manifest is rejected as partial too.
func kvmNetClassifyImage(manifest kvmNetImageManifest) (kvmNetImageKind, error) {
	marked := strings.Contains(manifest.ID, "unpinned-test") ||
		manifest.Distribution.Snapshot == "unpinned" ||
		manifest.Distribution.Mirror != ""
	if !marked {
		return kvmNetImagePinned, nil
	}
	if !storage.ValidImageID(manifest.ID) {
		return kvmNetImagePinned, fmt.Errorf("test-marked image id %q is not a valid image id", manifest.ID)
	}
	if !strings.HasSuffix(manifest.ID, kvmNetTestImageSuffix) ||
		strings.Count(manifest.ID, "unpinned-test") != 1 {
		return kvmNetImagePinned, fmt.Errorf("test-marked image id %q does not carry exactly one terminal %s suffix", manifest.ID, kvmNetTestImageSuffix)
	}
	base := strings.TrimSuffix(manifest.ID, kvmNetTestImageSuffix)
	if base == "" || !storage.ValidImageID(base) {
		return kvmNetImagePinned, fmt.Errorf("test-marked image id %q has an invalid base %q", manifest.ID, base)
	}
	if manifest.Distribution.Snapshot != "unpinned" {
		return kvmNetImagePinned, fmt.Errorf("test-marked image %q must declare distribution.snapshot=unpinned", manifest.ID)
	}
	if err := kvmNetValidateTestMirror(manifest.Distribution.Mirror); err != nil {
		return kvmNetImagePinned, fmt.Errorf("test-marked image %q: %w", manifest.ID, err)
	}
	return kvmNetImageFallback, nil
}

func kvmNetValidateTestMirror(mirror string) error {
	if strings.TrimSpace(mirror) == "" {
		return errors.New("distribution.mirror is empty")
	}
	parsed, err := url.Parse(mirror)
	if err != nil {
		return fmt.Errorf("distribution.mirror %q: %w", mirror, err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return fmt.Errorf("distribution.mirror %q is not http(s)", mirror)
	}
	if parsed.Host == "" {
		return fmt.Errorf("distribution.mirror %q has no host", mirror)
	}
	return nil
}

// kvmNetSelectImage selects a pinned image by default, or a consistent
// test-only fallback only with allowUnpinned. Inconsistent marked manifests
// abort before a pinned alternative can hide them.
func kvmNetSelectImage(manifests []kvmNetImageManifest, allowUnpinned bool) (kvmNetImageKind, string, error) {
	if len(manifests) == 0 {
		return kvmNetImagePinned, "", errors.New("image collection is empty")
	}
	var pinned, fallback []string
	for _, manifest := range manifests {
		kind, err := kvmNetClassifyImage(manifest)
		if err != nil {
			return kvmNetImagePinned, "", err
		}
		switch kind {
		case kvmNetImageFallback:
			fallback = append(fallback, manifest.ID)
		default:
			pinned = append(pinned, manifest.ID)
		}
	}
	slices.Sort(pinned)
	slices.Sort(fallback)
	if allowUnpinned && len(fallback) > 0 {
		return kvmNetImageFallback, fallback[0], nil
	}
	if len(pinned) > 0 {
		return kvmNetImagePinned, pinned[0], nil
	}
	if len(fallback) > 0 {
		return kvmNetImagePinned, "", fmt.Errorf("image collection holds only test-only fallback %q; set %s=1 (throwaway VM lifetime only) to select it", fallback[0], kvmNetUnpinnedOptInEnv)
	}
	return kvmNetImagePinned, "", errors.New("image collection holds no usable image")
}

func kvmNetUnpinnedOptIn() (bool, error) {
	switch value := os.Getenv(kvmNetUnpinnedOptInEnv); value {
	case "":
		return false, nil
	case "1":
		return true, nil
	default:
		return false, fmt.Errorf("%s must be unset or 1, got %q", kvmNetUnpinnedOptInEnv, value)
	}
}

func kvmNetPickImage(t *testing.T, dir string, allowUnpinned bool) string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read image dir: %v", err)
	}
	manifests := make([]kvmNetImageManifest, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(dir, entry.Name(), "manifest.json"))
		if err != nil {
			t.Fatalf("image %s manifest: %v", entry.Name(), err)
		}
		var manifest kvmNetImageManifest
		if err := json.Unmarshal(raw, &manifest); err != nil {
			t.Fatalf("decode image %s manifest: %v", entry.Name(), err)
		}
		manifests = append(manifests, manifest)
	}
	kind, id, err := kvmNetSelectImage(manifests, allowUnpinned)
	if err != nil {
		t.Fatalf("select image from %s: %v", dir, err)
	}
	if kind == kvmNetImageFallback {
		t.Logf("selected TEST-ONLY unpinned fallback image %q (throwaway VM lifetime only; %s=1)", id, kvmNetUnpinnedOptInEnv)
	} else {
		t.Logf("selected pinned image %q", id)
	}
	return id
}

func kvmNetTestUser(t *testing.T) (uid, gid int) {
	t.Helper()
	uidText := cmp.Or(os.Getenv(kvmNetUIDEnv), os.Getenv("SUDO_UID"))
	gidText := cmp.Or(os.Getenv(kvmNetGIDEnv), os.Getenv("SUDO_GID"))
	if uidText == "" || gidText == "" {
		t.Fatalf("cannot determine the unprivileged node user: run via sudo (SUDO_UID/SUDO_GID) or set %s/%s", kvmNetUIDEnv, kvmNetGIDEnv)
	}
	uid, err := strconv.Atoi(uidText)
	if err != nil || uid <= 0 {
		t.Fatalf("invalid unprivileged uid %q", uidText)
	}
	gid, err = strconv.Atoi(gidText)
	if err != nil || gid <= 0 {
		t.Fatalf("invalid unprivileged gid %q: the node and QEMU must not run with gid 0", gidText)
	}
	return uid, gid
}

// --- real SSH/scp/sftp flow -------------------------------------------------

func (tp *kvmNetTopology) sshAcceptance(t *testing.T, client nodev1.NodeClient, stateDir string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
	defer cancel()

	id := ids.Sandbox()
	tp.track(id)
	vm, err := client.CreateVM(ctx, &nodev1.CreateVMRequest{
		SandboxId: id,
		Resources: &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: 10},
	})
	if err != nil {
		t.Fatalf("CreateVM: %v", err)
	}
	if vm.State != nodev1.VMState_VM_STATE_READY {
		t.Fatalf("CreateVM returned %s, want READY", vm.State)
	}
	deleted := false
	defer func() {
		if deleted {
			return
		}
		dctx, dcancel := context.WithTimeout(context.Background(), time.Minute)
		defer dcancel()
		if _, err := client.DeleteVM(dctx, &nodev1.DeleteVMRequest{SandboxId: id}); err == nil {
			tp.untrack(id)
		}
	}()

	keyPath, publicKey := kvmNetSSHKey(t, tp.workDir, tp.uid, tp.gid)
	access := kvmNetAddSSHKey(ctx, t, client, id, publicKey)
	host := access.Host
	port := strconv.Itoa(int(access.Port))
	addr := net.JoinHostPort(host, port)
	oldKnown, oldKey := kvmNetWriteKnownHosts(t, tp.workDir, tp.uid, tp.gid, "old", host, access.Port, access.HostKey)

	tp.waitReachable(t, addr, true, 2*time.Minute)
	if got := tp.runSSH(t, keyPath, oldKnown, port, access.User, host, "printf 'ssh-ok\\n'"); got != "ssh-ok\n" {
		t.Fatalf("ssh command output = %q", got)
	}
	tp.runSSH(t, keyPath, oldKnown, port, access.User, host, "printf 'marker\\n' > /home/user/kvm-net-marker")

	localSCP := filepath.Join(tp.workDir, "scp-default.txt")
	kvmNetWriteClientFile(t, localSCP, []byte("scp-default\n"), tp.uid, tp.gid)
	tp.runSCP(t, false, keyPath, oldKnown, port, access.User, host, localSCP, "/home/user/scp-default.txt")
	if got := tp.runSSH(t, keyPath, oldKnown, port, access.User, host, "cat /home/user/scp-default.txt"); got != "scp-default\n" {
		t.Fatalf("scp (sftp mode) content = %q", got)
	}

	localLegacy := filepath.Join(tp.workDir, "scp-legacy.txt")
	kvmNetWriteClientFile(t, localLegacy, []byte("scp-legacy\n"), tp.uid, tp.gid)
	tp.runSCP(t, true, keyPath, oldKnown, port, access.User, host, localLegacy, "/home/user/scp-legacy.txt")
	if got := tp.runSSH(t, keyPath, oldKnown, port, access.User, host, "cat /home/user/scp-legacy.txt"); got != "scp-legacy\n" {
		t.Fatalf("scp -O content = %q", got)
	}

	localSFTP := filepath.Join(tp.workDir, "sftp.txt")
	remoteGot := filepath.Join(tp.workDir, "sftp-got.txt")
	kvmNetWriteClientFile(t, localSFTP, []byte("sftp\n"), tp.uid, tp.gid)
	tp.runSFTP(t, keyPath, oldKnown, port, access.User, host, fmt.Sprintf("put %s /home/user/sftp.txt\n", localSFTP))
	tp.runSFTP(t, keyPath, oldKnown, port, access.User, host, fmt.Sprintf("get /home/user/sftp.txt %s\n", remoteGot))
	if raw, err := os.ReadFile(remoteGot); err != nil || string(raw) != "sftp\n" {
		t.Fatalf("sftp round trip = %q, %v", raw, err)
	}

	stopped, err := client.StopVM(ctx, &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 10})
	if err != nil {
		t.Fatalf("StopVM: %v", err)
	}
	if stopped.State != nodev1.VMState_VM_STATE_STOPPED {
		t.Fatalf("StopVM returned %s, want STOPPED", stopped.State)
	}
	tp.waitReachable(t, addr, false, 5*time.Second)

	started, err := client.StartVM(ctx, &nodev1.StartVMRequest{SandboxId: id})
	if err != nil {
		t.Fatalf("StartVM: %v", err)
	}
	if started.State != nodev1.VMState_VM_STATE_READY {
		t.Fatalf("StartVM returned %s, want READY", started.State)
	}
	access2 := kvmNetAddSSHKey(ctx, t, client, id, publicKey)
	if access2.User != access.User {
		t.Fatalf("SSH user rotated from %q to %q", access.User, access2.User)
	}
	if access2.Host != access.Host || access2.Port != access.Port {
		t.Logf("SSH endpoint changed across stop/start: %s:%d -> %s:%d", access.Host, access.Port, access2.Host, access2.Port)
	}
	newKnown, newKey := kvmNetWriteKnownHosts(t, tp.workDir, tp.uid, tp.gid, "new", access2.Host, access2.Port, access2.HostKey)
	if oldKey == newKey {
		t.Fatalf("SSH host key did not rotate across stop/start: %s", oldKey)
	}
	// Pin the OLD public key at the NEW returned endpoint. Reusing the old
	// known_hosts file would fail merely because a changed port has no entry,
	// which would not exercise host-key rejection.
	mismatchKnown, _ := kvmNetWriteKnownHosts(t, tp.workDir, tp.uid, tp.gid,
		"old-key-new-endpoint", access2.Host, access2.Port, access.HostKey)
	addr2 := net.JoinHostPort(access2.Host, strconv.Itoa(int(access2.Port)))
	tp.waitReachable(t, addr2, true, 2*time.Minute)

	stdout, stderr, err := tp.sshMaybe(t, keyPath, mismatchKnown, strconv.Itoa(int(access2.Port)), access2.User, access2.Host, "true")
	combined := strings.ToLower(stdout + stderr)
	if err == nil {
		t.Fatalf("ssh accepted the previous pinned host key after resume")
	}
	if !strings.Contains(combined, "identification has changed") && !strings.Contains(combined, "host key verification failed") {
		t.Fatalf("ssh with the previous pinned host key failed for the wrong reason: %v\n%s%s", err, stdout, stderr)
	}
	if got := tp.runSSH(t, keyPath, newKnown, strconv.Itoa(int(access2.Port)), access2.User, access2.Host, "cat /home/user/kvm-net-marker"); got != "marker\n" {
		t.Fatalf("pre-stop file did not survive resume: %q", got)
	}

	if _, err := client.DeleteVM(ctx, &nodev1.DeleteVMRequest{SandboxId: id}); err != nil {
		t.Fatalf("DeleteVM: %v", err)
	}
	deleted = true
	tp.untrack(id)
	tp.waitReachable(t, addr2, false, 5*time.Second)
	if _, err := os.Stat(filepath.Join(stateDir, id+".json")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("allocation state for %s remains after delete: %v", id, err)
	}
}

func kvmNetSSHKey(t *testing.T, dir string, uid, gid int) (path, authorized string) {
	t.Helper()
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate SSH key: %v", err)
	}
	signer, err := ssh.NewSignerFromKey(privateKey)
	if err != nil {
		t.Fatalf("SSH signer: %v", err)
	}
	authorized = strings.TrimSpace(string(ssh.MarshalAuthorizedKey(signer.PublicKey())))
	block, err := ssh.MarshalPrivateKey(privateKey, "bbraft-kvm-net")
	if err != nil {
		t.Fatalf("marshal SSH private key: %v", err)
	}
	path = filepath.Join(dir, "id_ed25519")
	kvmNetWriteClientFile(t, path, pem.EncodeToMemory(block), uid, gid)
	return path, authorized
}

func kvmNetAddSSHKey(ctx context.Context, t *testing.T, client nodev1.NodeClient, id, publicKey string) *nodev1.SSHAccess {
	t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	for {
		access, err := client.AddSSHKey(ctx, &nodev1.AddSSHKeyRequest{SandboxId: id, PublicKey: publicKey})
		if err == nil {
			if access.GetUser() == "" || access.GetHost() == "" || access.GetPort() == 0 || access.GetPort() > 65535 {
				t.Fatalf("AddSSHKey returned incomplete access %v", access)
			}
			host, err := netip.ParseAddr(access.GetHost())
			if err != nil || !host.IsValid() || host.IsUnspecified() {
				t.Fatalf("AddSSHKey host %q is not a usable address", access.GetHost())
			}
			return access
		}
		if status.Code(err) != codes.Unavailable {
			t.Fatalf("AddSSHKey: %v", err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("AddSSHKey stayed unavailable: %v", err)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("AddSSHKey: %v", context.Cause(ctx))
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// kvmNetKnownHostsEntry builds the exact known_hosts entry for one endpoint
// from the API-returned OpenSSH public key. It is the single construction used
// by all positive and negative pinning cases.
func kvmNetKnownHostsEntry(host string, port uint32, hostKey string) (line, canonical string, err error) {
	if host == "" || port == 0 || port > 65535 {
		return "", "", fmt.Errorf("endpoint %q:%d is not usable", host, port)
	}
	fields := strings.Fields(hostKey)
	if len(fields) < 2 {
		return "", "", fmt.Errorf("host key %q is not an OpenSSH public key", hostKey)
	}
	key, _, _, _, err := ssh.ParseAuthorizedKey([]byte(fields[0] + " " + fields[1] + "\n"))
	if err != nil {
		return "", "", fmt.Errorf("parse host key %q: %w", hostKey, err)
	}
	canonical = strings.TrimSpace(string(ssh.MarshalAuthorizedKey(key)))
	pattern := host
	if port != 22 {
		pattern = "[" + host + "]:" + strconv.Itoa(int(port))
	}
	return pattern + " " + canonical, canonical, nil
}

func kvmNetWriteKnownHosts(t *testing.T, dir string, uid, gid int, name, host string, port uint32, hostKey string) (path, canonical string) {
	t.Helper()
	line, canonical, err := kvmNetKnownHostsEntry(host, port, hostKey)
	if err != nil {
		t.Fatalf("%v", err)
	}
	path = filepath.Join(dir, "known_hosts."+name)
	kvmNetWriteClientFile(t, path, []byte(line+"\n"), uid, gid)
	return path, canonical
}

func kvmNetWriteClientFile(t *testing.T, path string, data []byte, uid, gid int) {
	t.Helper()
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	if err := os.Chown(path, uid, gid); err != nil {
		t.Fatalf("chown %s: %v", path, err)
	}
}

func (tp *kvmNetTopology) runSSH(t *testing.T, keyPath, knownHosts, port, user, host, command string) string {
	t.Helper()
	stdout, stderr, err := tp.sshMaybe(t, keyPath, knownHosts, port, user, host, command)
	if err != nil {
		t.Fatalf("ssh %s@%s: %v\nstdout:\n%s\nstderr:\n%s", user, host, err, stdout, stderr)
	}
	return stdout
}

func (tp *kvmNetTopology) sshMaybe(t *testing.T, keyPath, knownHosts, port, user, host, command string) (string, string, error) {
	t.Helper()
	args := []string{
		"-F", "/dev/null",
		"-i", keyPath,
		"-o", "IdentitiesOnly=yes",
		"-o", "BatchMode=yes",
		"-o", "StrictHostKeyChecking=yes",
		"-o", "UserKnownHostsFile=" + knownHosts,
		"-o", "GlobalKnownHostsFile=/dev/null",
		"-o", "ConnectTimeout=10",
		"-o", "PasswordAuthentication=no",
		"-o", "KbdInteractiveAuthentication=no",
		"-o", "UpdateHostKeys=no",
		"-p", port,
		user + "@" + host,
		command,
	}
	return tp.runAsNode(t, "ssh", args, nil)
}

func (tp *kvmNetTopology) runSCP(t *testing.T, legacy bool, keyPath, knownHosts, port, user, host, local, remote string) {
	t.Helper()
	args := []string{"-F", "/dev/null"}
	if legacy {
		args = append(args, "-O")
	}
	args = append(args,
		"-i", keyPath,
		"-o", "IdentitiesOnly=yes",
		"-o", "BatchMode=yes",
		"-o", "StrictHostKeyChecking=yes",
		"-o", "UserKnownHostsFile="+knownHosts,
		"-o", "GlobalKnownHostsFile=/dev/null",
		"-o", "ConnectTimeout=10",
		"-P", port,
		local,
		user+"@"+host+":"+remote,
	)
	if _, _, err := tp.runAsNode(t, "scp", args, nil); err != nil {
		t.Fatalf("scp %s -> %s: %v", local, remote, err)
	}
}

func (tp *kvmNetTopology) runSFTP(t *testing.T, keyPath, knownHosts, port, user, host, batch string) {
	t.Helper()
	args := []string{
		"-F", "/dev/null",
		"-i", keyPath,
		"-o", "IdentitiesOnly=yes",
		"-o", "BatchMode=yes",
		"-o", "StrictHostKeyChecking=yes",
		"-o", "UserKnownHostsFile=" + knownHosts,
		"-o", "GlobalKnownHostsFile=/dev/null",
		"-o", "ConnectTimeout=10",
		"-P", port,
		"-b", "-",
		user + "@" + host,
	}
	if _, stderr, err := tp.runAsNode(t, "sftp", args, []byte(batch)); err != nil {
		t.Fatalf("sftp %s@%s: %v\n%s", user, host, err, stderr)
	}
}

func (tp *kvmNetTopology) runAsNode(t *testing.T, name string, args []string, stdin []byte) (string, string, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	full := []string{
		"netns", "exec", tp.clientNS,
		"setpriv",
		"--reuid=" + strconv.Itoa(tp.uid),
		"--regid=" + strconv.Itoa(tp.gid),
		"--init-groups",
		"--",
		name,
	}
	full = append(full, args...)
	cmd := exec.CommandContext(ctx, "ip", full...)
	cmd.Env = kvmNetEnvWith(os.Environ(), "HOME", tp.workDir)
	if stdin != nil {
		cmd.Stdin = bytes.NewReader(stdin)
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if ctx.Err() != nil {
		err = errors.Join(err, context.Cause(ctx))
	}
	return stdout.String(), stderr.String(), err
}

// waitReachable polls the endpoint through a fresh TCP probe in the client
// namespace. Unreachable is only accepted after a failing probe, never a stale
// success.
func (tp *kvmNetTopology) waitReachable(t *testing.T, addr string, want bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last error
	for {
		err := tp.tcpProbe(t, addr)
		if want && err == nil {
			return
		}
		if !want && err != nil {
			return
		}
		last = err
		if time.Now().After(deadline) {
			t.Fatalf("endpoint %s reachable=%t not achieved (last: %v)", addr, want, last)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func (tp *kvmNetTopology) tcpProbe(t *testing.T, addr string) error {
	t.Helper()
	if err := kvmNetValidateProbeAddress(addr); err != nil {
		t.Fatalf("invalid TCP probe address: %v", err)
	}
	exe := tp.helperExecutable(t)
	resultFile := filepath.Join(tp.workDir, "tcp-probe-"+strconv.FormatInt(time.Now().UnixNano(), 10)+".txt")
	raw, err := json.Marshal(kvmNetProbeConfig{Address: addr, ResultFile: resultFile})
	if err != nil {
		t.Fatalf("encode probe config: %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "ip", "netns", "exec", tp.clientNS,
		"setpriv",
		"--reuid="+strconv.Itoa(tp.uid),
		"--regid="+strconv.Itoa(tp.gid),
		"--init-groups",
		"--",
		exe, "-test.run=^TestKVMNetworkTCPProbe$",
	)
	cmd.Env = kvmNetEnvWith(os.Environ(), kvmNetProbeEnv, string(raw))
	out, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("TCP probe helper timed out: %v\n%s", context.Cause(ctx), out)
	}
	if err != nil {
		t.Fatalf("TCP probe helper failed: %v\n%s", err, out)
	}
	rawResult, err := os.ReadFile(resultFile)
	if err != nil {
		t.Fatalf("read TCP probe result: %v", err)
	}
	result, err := kvmNetDecodeProbeResult(rawResult)
	if err != nil {
		t.Fatalf("decode TCP probe result %q: %v", rawResult, err)
	}
	switch {
	case result.OK:
		return nil
	case result.Class == kvmNetProbeRefused, result.Class == kvmNetProbeTimeout:
		return &kvmNetDialFailure{Class: result.Class, Detail: result.Error}
	default:
		t.Fatalf("TCP probe did not complete a dial: class %q: %s", result.Class, result.Error)
		return nil
	}
}

func kvmNetCommandOutput(ctx context.Context, name string, args ...string) ([]byte, error) {
	return exec.CommandContext(ctx, name, args...).CombinedOutput()
}

func kvmNetNamespacePids(ctx context.Context, name string) ([]string, error) {
	out, err := kvmNetCommandOutput(ctx, "ip", "netns", "pids", name)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", err, out)
	}
	return strings.Fields(string(out)), nil
}

func kvmNetRun(t *testing.T, name string, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	out, err := kvmNetCommandOutput(ctx, name, args...)
	if err != nil {
		t.Fatalf("%s %s: %v: %s", name, strings.Join(args, " "), err, out)
	}
	return string(out)
}

// --- nonprivileged regressions for the corrections --------------------------

func TestKVMNetworkNamespaceOwnership(t *testing.T) {
	owned := map[string]uint64{"owned": 42}
	for _, tc := range []struct {
		name     string
		observed uint64
		want     bool
	}{
		{name: "owned matching inode", observed: 42, want: true},
		{name: "unowned pre-existing name", observed: 7, want: false},
		{name: "owned inode changed", observed: 43, want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			name := "owned"
			if tc.name == "unowned pre-existing name" {
				name = "pre-existing"
			}
			may, reason := kvmNetMayDeleteNamespace(owned, name, tc.observed)
			if may != tc.want {
				t.Fatalf("mayDelete(%s) = %t (%s), want %t", name, may, reason, tc.want)
			}
			if !may && reason == "" {
				t.Fatalf("refused deletion of %s without a reason", name)
			}
		})
	}
	zero := map[string]uint64{"zero": 0}
	if may, _ := kvmNetMayDeleteNamespace(zero, "zero", 0); may {
		t.Fatal("namespace with unpinned identity was deletable")
	}
}

func TestKVMNetworkKnownHosts(t *testing.T) {
	oldKey := nodetest.TestPublicKey(t, "old")
	newKey := nodetest.TestPublicKey(t, "new")
	oldLine, oldCanonical, err := kvmNetKnownHostsEntry("203.0.113.1", 30000, oldKey)
	if err != nil {
		t.Fatalf("old known_hosts entry: %v", err)
	}
	if !strings.HasPrefix(oldLine, "[203.0.113.1]:30000 ssh-ed25519 ") {
		t.Fatalf("old known_hosts line = %q", oldLine)
	}
	oldAtNewPort, canonicalAtNewPort, err := kvmNetKnownHostsEntry("203.0.113.1", 30001, oldKey)
	if err != nil {
		t.Fatalf("old key at new endpoint: %v", err)
	}
	if !strings.HasPrefix(oldAtNewPort, "[203.0.113.1]:30001 ssh-ed25519 ") {
		t.Fatalf("old key at a changed port = %q", oldAtNewPort)
	}
	if canonicalAtNewPort != oldCanonical {
		t.Fatalf("endpoint change altered the pinned key: %q != %q", canonicalAtNewPort, oldCanonical)
	}
	newAtNewPort, newCanonical, err := kvmNetKnownHostsEntry("203.0.113.1", 30001, newKey)
	if err != nil {
		t.Fatalf("new key at new endpoint: %v", err)
	}
	if oldAtNewPort == newAtNewPort {
		t.Fatal("old and new host keys produced the same pinned entry at the new endpoint")
	}
	if newCanonical == oldCanonical {
		t.Fatal("test host keys are not distinct")
	}
	for _, bad := range []struct {
		name    string
		host    string
		port    uint32
		hostKey string
	}{
		{name: "empty key", host: "203.0.113.1", port: 30000},
		{name: "garbage key", host: "203.0.113.1", port: 30000, hostKey: "not a key"},
		{name: "zero port", host: "203.0.113.1", hostKey: oldKey},
		{name: "empty host", port: 30000, hostKey: oldKey},
	} {
		t.Run(bad.name, func(t *testing.T) {
			if _, _, err := kvmNetKnownHostsEntry(bad.host, bad.port, bad.hostKey); err == nil {
				t.Fatal("invalid known_hosts inputs were accepted")
			}
		})
	}
}

func TestKVMNetworkIdentityBoundary(t *testing.T) {
	want := uint64(1) << capNetAdminBit
	base := kvmNetReady{UID: 1000, GID: 1000, CapEff: want, CapAmb: want}
	if err := kvmNetValidateIdentity(base, 1000, 1000); err != nil {
		t.Fatalf("valid identity rejected: %v", err)
	}
	for _, tc := range []struct {
		name  string
		ready kvmNetReady
		uid   int
		gid   int
	}{
		{
			name: "extra effective cap",
			ready: func() kvmNetReady {
				ready := base
				ready.CapEff |= 1 << 13
				return ready
			}(),
			uid: 1000, gid: 1000,
		},
		{
			name: "extra ambient cap",
			ready: func() kvmNetReady {
				ready := base
				ready.CapAmb |= 1 << 13
				return ready
			}(),
			uid: 1000, gid: 1000,
		},
		{
			name: "missing effective cap",
			ready: func() kvmNetReady {
				ready := base
				ready.CapEff = 0
				return ready
			}(),
			uid: 1000, gid: 1000,
		},
		{name: "expected gid zero", ready: base, uid: 1000, gid: 0},
		{name: "actual gid zero", ready: kvmNetReady{UID: 1000, GID: 0, CapEff: want, CapAmb: want}, uid: 1000, gid: 1000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := kvmNetValidateIdentity(tc.ready, tc.uid, tc.gid); err == nil {
				t.Fatal("invalid identity boundary was accepted")
			}
		})
	}
}

func kvmNetTestManifest(id, snapshot, mirror string) kvmNetImageManifest {
	var manifest kvmNetImageManifest
	manifest.ID = id
	manifest.Distribution.Snapshot = snapshot
	manifest.Distribution.Mirror = mirror
	return manifest
}

func TestKVMNetworkImageSelection(t *testing.T) {
	const (
		pinnedSnapshot = "20261108T182718Z"
		goodMirror     = "http://archive.ubuntu.com/ubuntu"
	)
	pinned := kvmNetTestManifest("v1", pinnedSnapshot, "")
	fallback := kvmNetTestManifest("v1-unpinned-test", "unpinned", goodMirror)
	for _, tc := range []struct {
		name        string
		manifests   []kvmNetImageManifest
		allow       bool
		wantKind    kvmNetImageKind
		wantID      string
		wantErr     bool
		errContains string
	}{
		{name: "pinned default", manifests: []kvmNetImageManifest{pinned}, wantKind: kvmNetImagePinned, wantID: "v1"},
		{name: "pinned default ignores fallback", manifests: []kvmNetImageManifest{fallback, pinned}, wantKind: kvmNetImagePinned, wantID: "v1"},
		{name: "consistent fallback with opt-in", manifests: []kvmNetImageManifest{fallback}, allow: true, wantKind: kvmNetImageFallback, wantID: "v1-unpinned-test"},
		{name: "consistent fallback without opt-in", manifests: []kvmNetImageManifest{fallback}, wantErr: true, errContains: kvmNetUnpinnedOptInEnv},
		{name: "opt-in prefers fallback", manifests: []kvmNetImageManifest{pinned, fallback}, allow: true, wantKind: kvmNetImageFallback, wantID: "v1-unpinned-test"},
		{name: "opt-in without fallback uses pinned", manifests: []kvmNetImageManifest{pinned}, allow: true, wantKind: kvmNetImagePinned, wantID: "v1"},
		{name: "https mirror", manifests: []kvmNetImageManifest{kvmNetTestManifest("v2-unpinned-test", "unpinned", "https://mirror.example/ubuntu")}, allow: true, wantKind: kvmNetImageFallback, wantID: "v2-unpinned-test"},
		{name: "suffix only", manifests: []kvmNetImageManifest{kvmNetTestManifest("v1-unpinned-test", pinnedSnapshot, "")}, wantErr: true, errContains: "snapshot"},
		{name: "snapshot only", manifests: []kvmNetImageManifest{kvmNetTestManifest("v1", "unpinned", "")}, wantErr: true, errContains: "suffix"},
		{name: "missing mirror", manifests: []kvmNetImageManifest{kvmNetTestManifest("v1-unpinned-test", "unpinned", "")}, allow: true, wantErr: true, errContains: "mirror"},
		{name: "non-http mirror", manifests: []kvmNetImageManifest{kvmNetTestManifest("v1-unpinned-test", "unpinned", "file:///srv/mirror")}, allow: true, wantErr: true, errContains: "http"},
		{name: "mirror without host", manifests: []kvmNetImageManifest{kvmNetTestManifest("v1-unpinned-test", "unpinned", "https:///ubuntu")}, allow: true, wantErr: true, errContains: "host"},
		{name: "mirror-only partial", manifests: []kvmNetImageManifest{kvmNetTestManifest("v1", pinnedSnapshot, goodMirror)}, wantErr: true, errContains: "suffix"},
		{name: "repeated suffix", manifests: []kvmNetImageManifest{kvmNetTestManifest("v1-unpinned-test-extra-unpinned-test", "unpinned", goodMirror)}, allow: true, wantErr: true, errContains: "terminal"},
		{name: "mid suffix", manifests: []kvmNetImageManifest{kvmNetTestManifest("v1-unpinned-test-extra", "unpinned", goodMirror)}, allow: true, wantErr: true, errContains: "terminal"},
		{name: "invalid id grammar", manifests: []kvmNetImageManifest{kvmNetTestManifest("V1-unpinned-test", "unpinned", goodMirror)}, allow: true, wantErr: true, errContains: "valid image id"},
		{name: "empty collection", wantErr: true, errContains: "empty"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			kind, id, err := kvmNetSelectImage(tc.manifests, tc.allow)
			if (err != nil) != tc.wantErr {
				t.Fatalf("select = %v, %q, %v; wantErr %t", kind, id, err, tc.wantErr)
			}
			if tc.wantErr {
				if !strings.Contains(err.Error(), tc.errContains) {
					t.Fatalf("error %q does not name %q", err, tc.errContains)
				}
				return
			}
			if kind != tc.wantKind || id != tc.wantID {
				t.Fatalf("select = %v, %q; want %v, %q", kind, id, tc.wantKind, tc.wantID)
			}
		})
	}
}

func TestKVMNetworkUnpinnedOptIn(t *testing.T) {
	for _, tc := range []struct {
		name    string
		value   string
		want    bool
		wantErr bool
	}{
		{name: "unset"},
		{name: "explicit one", value: "1", want: true},
		{name: "zero", value: "0", wantErr: true},
		{name: "true", value: "true", wantErr: true},
		{name: "spaces", value: " 1", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(kvmNetUnpinnedOptInEnv, tc.value)
			got, err := kvmNetUnpinnedOptIn()
			if (err != nil) != tc.wantErr {
				t.Fatalf("opt-in %q = %t, %v; wantErr %t", tc.value, got, err, tc.wantErr)
			}
			if !tc.wantErr && got != tc.want {
				t.Fatalf("opt-in %q = %t, want %t", tc.value, got, tc.want)
			}
		})
	}
}

func TestKVMNetworkProbeResultDecoding(t *testing.T) {
	encode := func(t *testing.T, result kvmNetProbeResult) []byte {
		t.Helper()
		raw, err := json.Marshal(result)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	for _, tc := range []struct {
		name    string
		raw     []byte
		wantErr bool
	}{
		{name: "connected", raw: encode(t, kvmNetProbeResult{OK: true, Class: kvmNetProbeConnected})},
		{name: "refused", raw: encode(t, kvmNetProbeResult{Class: kvmNetProbeRefused, Error: "connection refused"})},
		{name: "timeout", raw: encode(t, kvmNetProbeResult{Class: kvmNetProbeTimeout, Error: "i/o timeout"})},
		{name: "invalid", raw: encode(t, kvmNetProbeResult{Class: kvmNetProbeInvalid, Error: "bad address"})},
		{name: "other", raw: encode(t, kvmNetProbeResult{Class: kvmNetProbeOther, Error: "network unreachable"})},
		{name: "empty", raw: nil, wantErr: true},
		{name: "malformed", raw: []byte("{"), wantErr: true},
		{name: "unknown class", raw: encode(t, kvmNetProbeResult{Class: "maybe", Error: "x"}), wantErr: true},
		{name: "connected marked failed", raw: encode(t, kvmNetProbeResult{Class: kvmNetProbeConnected}), wantErr: true},
		{name: "failure marked ok", raw: encode(t, kvmNetProbeResult{OK: true, Class: kvmNetProbeRefused, Error: "x"}), wantErr: true},
		{name: "failure without detail", raw: encode(t, kvmNetProbeResult{Class: kvmNetProbeRefused}), wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := kvmNetDecodeProbeResult(tc.raw)
			if (err != nil) != tc.wantErr {
				t.Fatalf("decode error = %v, wantErr %t", err, tc.wantErr)
			}
		})
	}
	for _, tc := range []struct {
		address string
		wantErr bool
	}{
		{address: "203.0.113.1:30000"},
		{address: "", wantErr: true},
		{address: "203.0.113.1", wantErr: true},
		{address: "203.0.113.1:0", wantErr: true},
		{address: "0.0.0.0:30000", wantErr: true},
		{address: "host:30000", wantErr: true},
	} {
		t.Run("address "+tc.address, func(t *testing.T) {
			if err := kvmNetValidateProbeAddress(tc.address); (err != nil) != tc.wantErr {
				t.Fatalf("validate(%q) = %v, wantErr %t", tc.address, err, tc.wantErr)
			}
		})
	}
	if got := kvmNetProbeClass(syscall.ECONNREFUSED); got != kvmNetProbeRefused {
		t.Fatalf("ECONNREFUSED class = %q", got)
	}
	if got := kvmNetProbeClass(context.DeadlineExceeded); got != kvmNetProbeTimeout {
		t.Fatalf("deadline class = %q", got)
	}
	if got := kvmNetProbeClass(errors.New("other")); got != kvmNetProbeOther {
		t.Fatalf("other class = %q", got)
	}
}
