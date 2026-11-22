package node

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/nathabonfim59/bamboo-raft/internal/ids"
	"github.com/nathabonfim59/bamboo-raft/internal/node/qemu"
	"github.com/nathabonfim59/bamboo-raft/internal/node/storage"
	"github.com/nathabonfim59/bamboo-raft/internal/nodelink"
	"github.com/nathabonfim59/bamboo-raft/internal/nodetest"
	"github.com/nathabonfim59/bamboo-raft/internal/objstore/memstore"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// Real-KVM tests. They need the host devices and a built image:
//
//	BBRAFT_TEST_KVM=1 BBRAFT_TEST_IMAGE_DIR=/path/to/images/out \
//	  TMPDIR=/path/with/20GiB-free \
//	  go test ./internal/node -run 'TestKVM' -timeout 20m
//
// The frozen suite includes Snapshot. FenceWatch is at least the bounded boot
// time. Once BBRAFT_TEST_KVM=1 is set, a missing image directory, missing QEMU
// binary or missing /dev/kvm or /dev/vhost-vsock fails instead of skipping.
//
// BBRAFT_TEST_QEMU_PREFIX=<prefix> (optional; make check never needs it) puts
// <prefix>/bin first on PATH for the test, so the KVM tests run against that
// QEMU and qemu-img instead of the distro's. internal/node/qemu10.sh builds
// the pinned upstream QEMU 10.0 into such a prefix and prints it.
func requireKVM(t *testing.T) (imageDir string) {
	t.Helper()
	if os.Getenv("BBRAFT_TEST_KVM") != "1" {
		t.Skip("set BBRAFT_TEST_KVM=1 and BBRAFT_TEST_IMAGE_DIR")
	}
	imageDir = os.Getenv("BBRAFT_TEST_IMAGE_DIR")
	if imageDir == "" {
		t.Fatal("BBRAFT_TEST_KVM=1 requires BBRAFT_TEST_IMAGE_DIR")
	}
	if info, err := os.Stat(imageDir); err != nil || !info.IsDir() {
		t.Fatalf("BBRAFT_TEST_IMAGE_DIR %q: %v", imageDir, err)
	}
	if os.Geteuid() == 0 {
		t.Fatal("the node refuses to run QEMU as root; run the tests as the node service user")
	}
	kvmUseQEMUPrefix(t)
	if err := qemu.CheckHost(t.Context(), slog.New(slog.DiscardHandler)); err != nil {
		t.Fatalf("KVM host check: %v", err)
	}
	return imageDir
}

// kvmUseQEMUPrefix applies BBRAFT_TEST_QEMU_PREFIX: an absolute prefix whose
// bin/ holds qemu-system-x86_64 and qemu-img goes first on PATH for this test
// (and the node it starts: the unjailed launcher resolves both by name). A
// set but unusable prefix fails instead of silently testing the distro QEMU.
func kvmUseQEMUPrefix(t *testing.T) {
	t.Helper()
	prefix := os.Getenv("BBRAFT_TEST_QEMU_PREFIX")
	if prefix == "" {
		return
	}
	bin, err := kvmQEMUPrefixBin(prefix)
	if err != nil {
		t.Fatalf("BBRAFT_TEST_QEMU_PREFIX: %v", err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
}

// kvmQEMUPrefixBin returns prefix's bin directory after checking that prefix
// is an absolute clean path whose bin/ holds executable qemu-system-x86_64
// and qemu-img.
func kvmQEMUPrefixBin(prefix string) (string, error) {
	if !filepath.IsAbs(prefix) || filepath.Clean(prefix) != prefix {
		return "", fmt.Errorf("%q is not an absolute clean path", prefix)
	}
	bin := filepath.Join(prefix, "bin")
	for _, tool := range []string{"qemu-system-x86_64", "qemu-img"} {
		if info, err := os.Stat(filepath.Join(bin, tool)); err != nil || info.IsDir() || info.Mode()&0o111 == 0 {
			return "", fmt.Errorf("%q has no executable bin/%s (%v); build it with internal/node/qemu10.sh", prefix, tool, err)
		}
	}
	return bin, nil
}

// kvmCPUOverride maps BBRAFT_TEST_CPU_MODEL to Manager CPU settings. Unset
// keeps the strict production default. "host" is a test-only escape hatch and
// must also set AllowHostCPU; named models forward normally. "max" is never
// special-cased, so Manager normalization rejects it.
func kvmCPUOverride() (string, bool) {
	model := strings.TrimSpace(os.Getenv("BBRAFT_TEST_CPU_MODEL"))
	if model == "" {
		return "", false
	}
	if strings.EqualFold(model, "host") {
		return "host", true
	}
	return model, false
}

func newKVMManager(t *testing.T, imageDir string) *Manager {
	t.Helper()
	return newKVMManagerWith(t, imageDir, nil)
}

// newKVMManagerWith is newKVMManager with configure applied to the Config
// just before New (for example an object store).
func newKVMManagerWith(t *testing.T, imageDir string, configure func(*Config)) *Manager {
	t.Helper()
	capacity := &nodev1.Resources{Vcpus: 16, MemoryMib: 32 << 10, DiskGib: 200}
	cpuModel, allowHostCPU := kvmCPUOverride()
	cfg := Config{
		NodeID:       "kvm-test",
		DataDir:      t.TempDir(),
		ImageDir:     imageDir,
		QemuVersion:  "",
		CPUModel:     cpuModel,
		AllowHostCPU: allowHostCPU,
		Capacity:     capacity,
		BootTimeout:  2 * time.Minute,
		// KVM conformance keeps the explicit dev/test user-mode network until
		// the real-TAP A8 gate; production launches always set Network.
		AllowUserNetworking: true,
		Log:                 slog.New(slog.DiscardHandler),
	}
	if configure != nil {
		configure(&cfg)
	}
	m, err := New(t.Context(), cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := m.Shutdown(ctx); err != nil {
			t.Errorf("Shutdown: %v", err)
		}
	})
	return m
}

// TestKVMCPUOverrideSelection covers the test env boundary: unset keeps the
// production fixed default, host requires the test-only allowance, named
// models forward, and max is always rejected.
func TestKVMCPUOverrideSelection(t *testing.T) {
	for _, tc := range []struct {
		name      string
		env       string
		wantModel string
		wantAllow bool
		wantErr   bool
	}{
		{"unset production default", "", "", false, false},
		{"host test escape", "host", "host", true, false},
		{"named model", "Skylake-Client", "Skylake-Client", false, false},
		{"max rejected", "max", "max", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("BBRAFT_TEST_CPU_MODEL", tc.env)
			model, allow := kvmCPUOverride()
			if model != tc.wantModel || allow != tc.wantAllow {
				t.Fatalf("override = %q,%t, want %q,%t", model, allow, tc.wantModel, tc.wantAllow)
			}
			cfg := Config{
				NodeID: "node-test", DataDir: t.TempDir(), ImageDir: t.TempDir(),
				CPUModel: model, AllowHostCPU: allow, Nested: qemu.NestedOff,
				Capacity: &nodev1.Resources{Vcpus: 1, MemoryMib: 1, DiskGib: 1},
				// Test-factory construction: the dev/test user-mode network.
				AllowUserNetworking: true,
			}
			err := normalizeConfig(&cfg)
			if gotErr := err != nil; gotErr != tc.wantErr {
				t.Fatalf("normalizeConfig error = %v, wantErr %t", err, tc.wantErr)
			}
		})
	}
}

// TestKVMHostRequiresExplicitAllowance proves host is not a production model:
// without the test-only flag Manager refuses it, while true reaches qemu's
// real launch Args with enforce retained.
func TestKVMHostRequiresExplicitAllowance(t *testing.T) {
	base := func(allow bool) Config {
		return Config{
			NodeID: "node-test", DataDir: t.TempDir(), ImageDir: t.TempDir(),
			CPUModel: "host", AllowHostCPU: allow, Nested: qemu.NestedAuto,
			Capacity: &nodev1.Resources{Vcpus: 1, MemoryMib: 1, DiskGib: 1},
			// Test-factory construction: the dev/test user-mode network.
			AllowUserNetworking: true,
		}
	}
	cfg := base(false)
	if err := normalizeConfig(&cfg); err == nil {
		t.Fatal("host CPU accepted without AllowHostCPU")
	}
	// Direct Config must match qemu's exact opt-in spelling; the env factory,
	// not Manager, is responsible for canonicalizing its own input.
	cfg = base(true)
	cfg.CPUModel = "Host"
	if err := normalizeConfig(&cfg); err == nil {
		t.Fatal("direct Config Host spelling accepted with AllowHostCPU")
	}
	cfg = base(true)
	if err := normalizeConfig(&cfg); err != nil {
		t.Fatalf("host CPU with AllowHostCPU: %v", err)
	}
	store, err := storage.Open(storage.Config{DataDir: cfg.DataDir, ImageDir: cfg.ImageDir})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	m := &Manager{cfg: cfg, store: store}
	qcfg := m.launchConfig(storage.VMMeta{
		SandboxID: "bx_22222222", Image: "img-v0",
		Resources: storage.Resources{VCPUs: 1, MemoryMiB: 1024, DiskGiB: 1},
		VsockCID:  3,
	}, nil)
	// AMD fixture: auto nested adds nothing, so the full Args still exposes
	// the test-only host model with enforce.
	qcfg.HostCPU = &qemu.HostCPU{Vendor: "AuthenticAMD", Svm: true, CPUs: 1}
	argv, err := qcfg.Args()
	if err != nil {
		t.Fatalf("host launch Args: %v", err)
	}
	cpu := ""
	for i := range len(argv) - 1 {
		if argv[i] == "-cpu" {
			cpu = argv[i+1]
			break
		}
	}
	if cpu != "host,enforce" {
		t.Fatalf("-cpu = %q, want host,enforce", cpu)
	}
}

// TestKVMCPUInfoModelTruthful reports the actual configured model: explicit
// host is not hidden, and unset still reports qemu.DefaultCPUModel.
func TestKVMCPUInfoModelTruthful(t *testing.T) {
	store, err := storage.Open(storage.Config{DataDir: t.TempDir(), ImageDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	for _, tc := range []struct {
		name      string
		model     string
		allowHost bool
		want      string
	}{
		{"production default", "", false, qemu.DefaultCPUModel},
		{"explicit test host", "host", true, "host"},
		{"explicit named", "Skylake-Client", false, "Skylake-Client"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := &Manager{
				cfg:      Config{CPUModel: tc.model, AllowHostCPU: tc.allowHost},
				store:    store,
				capacity: resources{vcpus: 1, memoryMib: 1, diskGiB: 1},
				diskFree: func() (uint64, error) { return 1 << 40, nil },
			}
			info, err := m.Info(t.Context(), &nodev1.InfoRequest{})
			if err != nil {
				t.Fatal(err)
			}
			if info.GetCpuModel() != tc.want {
				t.Fatalf("Info.cpu_model = %q, want %q", info.GetCpuModel(), tc.want)
			}
		})
	}
}

// TestKVMConformanceDirect runs the whole suite, Phase 2 snapshots and
// restores against a memstore, in process.
func TestKVMConformanceDirect(t *testing.T) {
	imageDir := requireKVM(t)
	store := memstore.New()
	m := newKVMManagerWith(t, imageDir, func(cfg *Config) {
		cfg.Store = store
		cfg.ChunkCacheBytes = 1 << 30
	})
	client := startNodeClient(t, &harness{mg: m})
	nodetest.Run(t, client, nodetest.Options{
		BootTimeout: 2 * time.Minute,
		FenceWatch:  2 * time.Minute,
		// Processes uses the assembled A5 guest slice; SSH belongs to A8.
		Phase1: nodetest.Phase1{Stop: true, Processes: true, Files: true},
		Phase2: nodetest.Phase2{Snapshots: true, Reset: true, Store: store},
	})
}

// startKVMHub runs a hub and this Manager as a nodelink node, like the real
// bbraft-node command does.
func startKVMHub(t *testing.T, m *Manager) *nodelink.Hub {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.DiscardHandler)
	hub := nodelink.NewHub(func(context.Context, nodelink.Hello) error { return nil }, log)
	hubCtx, hubCancel := context.WithCancel(context.Background())
	hubDone := make(chan struct{})
	go func() {
		_ = hub.Serve(hubCtx, ln)
		close(hubDone)
	}()
	t.Cleanup(func() {
		hubCancel()
		<-hubDone
	})

	srv := grpc.NewServer(nodelink.ServerOptions()...)
	m.Register(srv)
	nodeCtx, nodeCancel := context.WithCancel(context.Background())
	nodeDone := make(chan struct{})
	go func() {
		_ = nodelink.Serve(nodeCtx, nodelink.DialConfig{
			Addr:       ln.Addr().String(),
			NodeID:     "kvm-test",
			Token:      "test-token",
			Log:        log,
			MinBackoff: 50 * time.Millisecond,
			MaxBackoff: 200 * time.Millisecond,
		}, srv)
		close(nodeDone)
	}()
	t.Cleanup(func() {
		nodeCancel()
		<-nodeDone
	})
	return hub
}

func waitKVMLink(t *testing.T, hub *nodelink.Hub) *nodelink.Link {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	link, err := hub.Wait(ctx, "kvm-test")
	if err != nil {
		t.Fatalf("node never connected: %v", err)
	}
	return link
}

// TestKVMConformanceLink is TestKVMConformanceDirect through nodelink.
func TestKVMConformanceLink(t *testing.T) {
	imageDir := requireKVM(t)
	store := memstore.New()
	m := newKVMManagerWith(t, imageDir, func(cfg *Config) {
		cfg.Store = store
		cfg.ChunkCacheBytes = 1 << 30
	})
	link := waitKVMLink(t, startKVMHub(t, m))
	nodetest.Run(t, link.Client, nodetest.Options{
		BootTimeout: 2 * time.Minute,
		FenceWatch:  2 * time.Minute,
		// Processes uses the assembled A5 guest slice; SSH belongs to A8.
		Phase1: nodetest.Phase1{Stop: true, Processes: true, Files: true},
		Phase2: nodetest.Phase2{Snapshots: true, Reset: true, Store: store},
	})
}

// TestKVMLinkReconnect proves a successful VM survives a nodelink transport
// replacement: the node redials, the new link sees the same VM and commands
// still work.
func TestKVMLinkReconnect(t *testing.T) {
	imageDir := requireKVM(t)
	m := newKVMManager(t, imageDir)
	hub := startKVMHub(t, m)
	first := waitKVMLink(t, hub)

	id := ids.Sandbox()
	vm, err := first.Client.CreateVM(t.Context(), &nodev1.CreateVMRequest{
		SandboxId: id,
		Resources: &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: 10},
	})
	if err != nil {
		t.Fatalf("CreateVM over link: %v", err)
	}
	defer m.DeleteVM(context.Background(), &nodev1.DeleteVMRequest{SandboxId: id})
	if res, err := nodetest.Exec(t.Context(), first.Client, &nodev1.ExecRequest{
		SandboxId: id, Command: "echo survived > reconnect-marker",
	}); err != nil || res.Exit.GetExitCode() != 0 {
		t.Fatalf("marker command: %v %v", res, err)
	}
	first.Close()

	deadline := time.Now().Add(10 * time.Second)
	var second *nodelink.Link
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		link, err := hub.Wait(ctx, "kvm-test")
		cancel()
		if err == nil && link != first {
			second = link
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("node did not redial after the link closed")
		}
		time.Sleep(50 * time.Millisecond)
	}
	again, err := second.Client.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id})
	if err != nil {
		t.Fatalf("GetVM over the replacement link: %v", err)
	}
	if again.GetCreatedAtUnixMs() != vm.GetCreatedAtUnixMs() || again.GetState() != nodev1.VMState_VM_STATE_READY {
		t.Fatalf("replacement link sees %v, want the same READY VM", again)
	}
	res, err := nodetest.Exec(t.Context(), second.Client, &nodev1.ExecRequest{SandboxId: id, Command: "cat reconnect-marker"})
	if err != nil || res.Stdout != "survived\n" {
		t.Fatalf("Exec over the replacement link: %+v, %v", res, err)
	}
}

// TestKVMOwnershipAndDiskGrowth checks the parts the shared suite leaves
// permissive: the sandbox user identity, PutFile ownership/mode, and an
// overlay grown past the image base with the guest filesystem resized to it.
func TestKVMOwnershipAndDiskGrowth(t *testing.T) {
	imageDir := requireKVM(t)
	m := newKVMManager(t, imageDir)
	id := ids.Sandbox()
	res := &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: 12}
	if _, err := m.CreateVM(t.Context(), &nodev1.CreateVMRequest{SandboxId: id, Resources: res}); err != nil {
		t.Fatalf("CreateVM: %v", err)
	}
	defer m.DeleteVM(context.Background(), &nodev1.DeleteVMRequest{SandboxId: id})

	client := startNodeClient(t, &harness{mg: m})
	for _, tc := range []struct {
		name, cmd, want string
	}{
		{"uid", "id -u", "1000\n"},
		{"gid", "id -g", "1000\n"},
		{"home", `printf '%s\n' "$HOME"`, "/home/user\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res, err := nodetest.Exec(t.Context(), client, &nodev1.ExecRequest{SandboxId: id, Command: tc.cmd})
			if err != nil {
				t.Fatal(err)
			}
			if res.Stdout != tc.want {
				t.Fatalf("%s: stdout %q, want %q", tc.cmd, res.Stdout, tc.want)
			}
		})
	}

	if _, err := nodetest.PutFile(t.Context(), client, id, "own/check.txt", 0o640, []byte("owned\n"), 1<<20); err != nil {
		t.Fatalf("PutFile: %v", err)
	}
	stat, err := nodetest.Exec(t.Context(), client, &nodev1.ExecRequest{
		SandboxId: id,
		Command:   `stat -c '%u:%g:%a' own/check.txt`,
	})
	if err != nil {
		t.Fatal(err)
	}
	if stat.Stdout != "1000:1000:640\n" {
		t.Fatalf("PutFile ownership/mode = %q, want 1000:1000:640", stat.Stdout)
	}

	res2, err := nodetest.Exec(t.Context(), client, &nodev1.ExecRequest{
		SandboxId: id,
		Command:   `df -B1 / | awk 'NR==2 {print $2}'`,
	})
	if err != nil {
		t.Fatal(err)
	}
	size, err := strconv.ParseUint(strings.TrimSpace(res2.Stdout), 10, 64)
	if err != nil {
		t.Fatalf("df output %q: %v", res2.Stdout, err)
	}
	want := uint64(12) << 30 * 95 / 100
	if size < want {
		t.Fatalf("guest filesystem %d bytes, want at least %d (12 GiB minus 5%%)", size, want)
	}
}
