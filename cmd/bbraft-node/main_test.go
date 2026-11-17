package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/nathabonfim59/bamboo-raft/internal/node"
	nodenet "github.com/nathabonfim59/bamboo-raft/internal/node/net"
	"github.com/nathabonfim59/bamboo-raft/internal/node/qemu"
	"github.com/nathabonfim59/bamboo-raft/internal/nodelink"
	"github.com/nathabonfim59/bamboo-raft/internal/version"
)

// spy records what runWithDeps drove, in order.
type spy struct {
	mu            sync.Mutex
	events        []string
	nodeCfg       node.Config
	serveCfg      nodelink.DialConfig
	cpu           qemu.CPUOption
	cpuMode       qemu.NestedMode
	probeCPU      qemu.CPUOption
	probeOptions  qemu.CPUProbeOptions
	registeredSrv *grpc.Server
	servedSrv     *grpc.Server
	serveCalled   chan struct{}
	serveResult   func(ctx context.Context) error
	mgr           *fakeManager
	newManagerErr error

	// factsCalls and tokenLookups isolate host-fact probing and the
	// credential read from the flag-environment lookups parsing performs.
	factsCalls   int
	tokenLookups int

	// network records the exact validated configuration handed to the
	// injected factory and the fake the command later borrows and closes.
	networkCfg    nodenet.Config
	networkLog    *slog.Logger
	networkCalls  int
	net           *fakeNetwork
	newNetworkErr error
	netCheckErr   error
	netCloseErr   error
}

func (s *spy) add(e string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, e)
}

func (s *spy) snapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.events...)
}

// fakeManager is a retry-safe Shutdown recorder: the first call can return a
// configured error and later calls succeed, like the real Manager.
type fakeManager struct {
	s                   *spy
	shutdownErrs        []error
	shutdownCtxErr      error
	shutdownHasDeadline bool
}

func (m *fakeManager) Register(srv *grpc.Server) { m.s.registeredSrv = srv }

func (m *fakeManager) Shutdown(ctx context.Context) error {
	m.s.add("shutdown")
	// Capture the context state at call time: run's deferred cancel will
	// have ended it by the time the test inspects anything.
	m.shutdownCtxErr = ctx.Err()
	_, m.shutdownHasDeadline = ctx.Deadline()
	if len(m.shutdownErrs) > 0 {
		err := m.shutdownErrs[0]
		m.shutdownErrs = m.shutdownErrs[1:]
		return err
	}
	return nil
}

// fakeDeps returns a fully injected deps: no QEMU, no filesystem state and
// no real link.
func fakeDeps(env map[string]string) (deps, *spy) {
	s := &spy{serveCalled: make(chan struct{})}
	s.mgr = &fakeManager{s: s}
	envLookup := testEnv(env)
	d := deps{
		lookupEnv: func(name string) (string, bool) {
			// The node token fallback is the only credential read; parseFlags
			// reads its own BBRAFT_* flag variables. Counting it separately
			// proves fail-closed validation ran before the credential load.
			if name == "BBRAFT_NODE_TOKEN" {
				s.tokenLookups++
			}
			return envLookup(name)
		},
		hostname: func() (string, error) { return "test-node", nil },
		facts: func(string) (hostFacts, error) {
			s.factsCalls++
			return hostFacts{vcpus: 8, memoryMiB: 16384, diskGiB: 200}, nil
		},
		hostCPU: func() (qemu.HostCPU, error) {
			s.add("hostCPU")
			return qemu.HostCPU{Vendor: "GenuineIntel", Vmx: true, CPUs: 8}, nil
		},
		resolveCPU: func(model string, mode qemu.NestedMode, host qemu.HostCPU) (qemu.CPUOption, error) {
			s.add("resolveCPU")
			opt, err := qemu.ResolveCPU(model, mode, host)
			s.cpu, s.cpuMode = opt, mode
			return opt, err
		},
		resolveTestCPU: func(model string, mode qemu.NestedMode, host qemu.HostCPU) (qemu.CPUOption, error) {
			s.add("resolveTestCPU")
			opt, err := qemu.ResolveTestCPU(model, mode, host)
			s.cpu, s.cpuMode = opt, mode
			return opt, err
		},
		checkHost: func(context.Context, *slog.Logger) error {
			s.add("checkHost")
			return nil
		},
		probeCPU: func(_ context.Context, option qemu.CPUOption, opts qemu.CPUProbeOptions) (qemu.CPUProbeEvidence, error) {
			s.add("probeCPU")
			s.probeCPU, s.probeOptions = option, opts
			return qemu.CPUProbeEvidence{Status: "prelaunch", CPUs: 1, QEMUVersion: "8.2.2", PID: 4242}, nil
		},
		newManager: func(_ context.Context, cfg node.Config) (vmManager, error) {
			s.nodeCfg = cfg
			s.add("newManager")
			if s.newManagerErr != nil {
				return nil, s.newManagerErr
			}
			return s.mgr, nil
		},
		// Network fixtures opt in to construction explicitly. A fixture that
		// reaches this factory without doing so is in TAP mode by accident.
		newNetwork: func(nodenet.Config, *slog.Logger) (nodeNetwork, error) {
			s.add("newNetwork")
			return nil, errUnexpectedNetwork
		},
		serve: func(ctx context.Context, cfg nodelink.DialConfig, srv *grpc.Server) error {
			s.serveCfg = cfg
			s.servedSrv = srv
			s.add("serve")
			close(s.serveCalled)
			if s.serveResult != nil {
				return s.serveResult(ctx)
			}
			<-ctx.Done()
			return nil
		},
	}
	return d, s
}

// devDeps returns fake deps in the explicit dev/test user networking mode.
// Tests that exercise CPU, token or lifecycle behaviour are not TAP fixtures;
// they must never construct or check production networking.
func devDeps(env map[string]string) (deps, *spy) {
	if _, ok := env["BBRAFT_NODE_NETWORK_MODE"]; ok {
		return fakeDeps(env)
	}
	env = maps.Clone(env)
	if env == nil {
		env = make(map[string]string, 1)
	}
	env["BBRAFT_NODE_NETWORK_MODE"] = networkModeUser
	return fakeDeps(env)
}

func TestRunLifecycle(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	d, s := devDeps(map[string]string{"BBRAFT_NODE_TOKEN": "  tok-123\n"})
	var stderr bytes.Buffer
	done := make(chan error, 1)
	go func() { done <- runWithDeps(ctx, nil, &stderr, d) }()

	<-s.serveCalled
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}

	if got, want := s.serveCfg.Addr, "127.0.0.1:7701"; got != want {
		t.Errorf("dial addr %q, want %q", got, want)
	}
	if s.serveCfg.NodeID != "test-node" {
		t.Errorf("dial node id %q, want test-node", s.serveCfg.NodeID)
	}
	if s.serveCfg.Token != "tok-123" {
		t.Errorf("dial token %q, want trimmed tok-123", s.serveCfg.Token)
	}
	if s.serveCfg.TLS != nil {
		t.Error("phase 0.5 dials plaintext: TLS must be nil")
	}
	if s.serveCfg.Version != version.Version {
		t.Errorf("dial version %q, want %q", s.serveCfg.Version, version.Version)
	}
	if s.serveCfg.Log == nil {
		t.Error("dial config has no logger")
	}

	// The manager got the resolved config: node id, dirs, detected
	// capacity minus reserves and the cpu model.
	cfg := s.nodeCfg
	if cfg.NodeID != "test-node" || cfg.DataDir != "/var/lib/bbraft" || cfg.ImageDir != "/var/lib/bbraft/images" {
		t.Fatalf("manager config %+v", cfg)
	}
	if cfg.CPUModel != "Haswell-noTSX" || cfg.BootTimeout != defaultBootTimeout {
		t.Fatalf("manager config %+v", cfg)
	}
	if got := cfg.Capacity; got.GetVcpus() != 7 || got.GetMemoryMib() != 15360 || got.GetDiskGib() != 199 {
		t.Fatalf("manager capacity %v", got)
	}
	if cfg.HostVCPUs != 8 || cfg.DiskOvercommit != 1 {
		t.Fatalf("manager overcommit config HostVCPUs=%d disk=%v", cfg.HostVCPUs, cfg.DiskOvercommit)
	}
	if !strings.Contains(stderr.String(), "capacity resolved") {
		t.Errorf("startup log misses effective capacity: %q", stderr.String())
	}
	if cfg.Log == nil {
		t.Error("manager config has no logger")
	}

	// CPU policy resolved through the shared helper: GenuineIntel+vmx and
	// auto mode gives Haswell-noTSX,+vmx,enforce.
	if s.cpuMode != qemu.NestedAuto {
		t.Errorf("nested mode %q, want auto", s.cpuMode)
	}
	if s.cpu.Model != "Haswell-noTSX" || s.cpu.Nested != "vmx" {
		t.Fatalf("resolved cpu %+v, want Haswell-noTSX/+vmx", s.cpu)
	}
	if s.probeCPU.String() != s.cpu.String() {
		t.Fatalf("probe CPU %q, want the resolved %q", s.probeCPU.String(), s.cpu.String())
	}

	// Register ran on the very server handed to nodelink.
	if s.registeredSrv == nil || s.registeredSrv != s.servedSrv {
		t.Error("manager did not register on the served grpc.Server")
	}

	if got := s.snapshot(); !equalStrings(got, []string{"hostCPU", "resolveCPU", "checkHost", "probeCPU", "newManager", "serve", "shutdown"}) {
		t.Fatalf("event order %v", got)
	}

	// The serve context is canceled by now; Shutdown must have run on an
	// independent, still-live, bounded context. Reusing the caller's ctx
	// would have reported context.Canceled at call time.
	if err := s.mgr.shutdownCtxErr; err != nil {
		t.Errorf("shutdown ctx must be live when called after the serve ctx ended, got %v", err)
	}
	if !s.mgr.shutdownHasDeadline {
		t.Error("shutdown ctx has no deadline; cleanup must be bounded")
	}

	if strings.Contains(stderr.String(), "tok-123") {
		t.Error("the token leaked into stderr")
	}
}

// TestRunOvercommitWiring drives ratios through flags into the effective
// capacity, Manager HostVCPUs/DiskOvercommit and startup log.
func TestRunOvercommitWiring(t *testing.T) {
	d, s := devDeps(map[string]string{"BBRAFT_NODE_TOKEN": "tok"})
	d.serve = func(context.Context, nodelink.DialConfig, *grpc.Server) error {
		s.add("serve")
		close(s.serveCalled)
		return nil
	}
	var stderr strings.Builder
	args := []string{"-node.cpu-overcommit=2", "-node.memory-overcommit=2", "-node.disk-overcommit=2"}
	if err := runWithDeps(t.Context(), args, &stderr, d); err != nil {
		t.Fatal(err)
	}
	cfg := s.nodeCfg
	if got := cfg.Capacity; got.GetVcpus() != 15 || got.GetMemoryMib() != 31744 || got.GetDiskGib() != 399 {
		t.Fatalf("overcommitted manager capacity %v", got)
	}
	if cfg.HostVCPUs != 8 || cfg.DiskOvercommit != 2 {
		t.Fatalf("manager overcommit config HostVCPUs=%d disk=%v", cfg.HostVCPUs, cfg.DiskOvercommit)
	}
	logs := stderr.String()
	for _, want := range []string{"cpu_overcommit=2", "memory_overcommit=2", "disk_overcommit=2", "host_vcpus=8"} {
		if !strings.Contains(logs, want) {
			t.Errorf("capacity log %q misses %q", logs, want)
		}
	}
}

func TestRunServeErrorStillShutsDown(t *testing.T) {
	d, s := devDeps(map[string]string{"BBRAFT_NODE_TOKEN": "tok"})
	want := errors.New("link loop failed")
	s.serveResult = func(context.Context) error { return want }
	err := runWithDeps(t.Context(), nil, io.Discard, d)
	if !errors.Is(err, want) {
		t.Fatalf("run error %v, want %v", err, want)
	}
	if got := s.snapshot(); !equalStrings(got, []string{"hostCPU", "resolveCPU", "checkHost", "probeCPU", "newManager", "serve", "shutdown"}) {
		t.Fatalf("event order %v", got)
	}
}

// TestRunProbeFailureStopsBeforeManager proves an unrealizable enforced CPU
// exits after CheckHost and before any manager, registration or link exists,
// while preserving the configured CPU option and full probe diagnostic.
func TestRunProbeFailureStopsBeforeManager(t *testing.T) {
	d, s := devDeps(map[string]string{"BBRAFT_NODE_TOKEN": "tok"})
	probeErr := errors.New("qemu: cpu probe: qemu-system-x86_64: warning: host doesn't support requested feature: CPUID.01H:ECX.pcid [bit 17]")
	d.probeCPU = func(context.Context, qemu.CPUOption, qemu.CPUProbeOptions) (qemu.CPUProbeEvidence, error) {
		s.add("probeCPU")
		return qemu.CPUProbeEvidence{}, probeErr
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	err := runWithDeps(ctx, nil, io.Discard, d)
	if !errors.Is(err, probeErr) {
		t.Fatalf("run error %v, want probe error", err)
	}
	if !strings.Contains(err.Error(), s.cpu.String()) {
		t.Fatalf("error %q does not name the configured CPU %q", err, s.cpu.String())
	}
	if !strings.Contains(err.Error(), "CPUID.01H:ECX.pcid [bit 17]") {
		t.Fatalf("error lost probe diagnostic: %v", err)
	}
	if got := s.snapshot(); !equalStrings(got, []string{"hostCPU", "resolveCPU", "checkHost", "probeCPU"}) {
		t.Fatalf("event order %v: no manager/link may start after probe failure", got)
	}
}

// TestRunStartupCancelDuringProbeStopsBeforeManager blocks inside the startup
// probe, cancels the caller, and requires a canceled error with no manager,
// registration or link. Cleanup releases/joins even if a regression ignores
// the caller context.
func TestRunStartupCancelDuringProbeStopsBeforeManager(t *testing.T) {
	d, s := devDeps(map[string]string{"BBRAFT_NODE_TOKEN": "tok"})
	entered := make(chan struct{})
	release := make(chan struct{})
	var enterOnce, releaseOnce sync.Once
	releaseProbe := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseProbe()
	d.probeCPU = func(ctx context.Context, _ qemu.CPUOption, _ qemu.CPUProbeOptions) (qemu.CPUProbeEvidence, error) {
		s.add("probeCPU")
		enterOnce.Do(func() { close(entered) })
		select {
		case <-ctx.Done():
			return qemu.CPUProbeEvidence{}, context.Cause(ctx)
		case <-release:
			return qemu.CPUProbeEvidence{Status: "prelaunch", CPUs: 1}, nil
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runWithDeps(ctx, nil, io.Discard, d) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("startup never reached the probe")
	}
	cancel()
	var err error
	select {
	case err = <-done:
	case <-time.After(2 * time.Second):
		releaseProbe()
		select {
		case err = <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("canceled startup did not join")
		}
		t.Fatalf("probe ignored caller cancellation: %v", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("startup error = %v, want context.Canceled", err)
	}
	if got := s.snapshot(); !equalStrings(got, []string{"hostCPU", "resolveCPU", "checkHost", "probeCPU"}) {
		t.Fatalf("event order %v: manager/link started after probe cancellation", got)
	}
	if s.registeredSrv != nil {
		t.Fatal("server registration ran after probe cancellation")
	}
}

// The CPU policy is the shared qemu.ResolveCPU; these tests drive it
// through the injected seams.
func TestRunCPUPolicy(t *testing.T) {
	for _, tc := range []struct {
		name      string
		args      []string
		host      qemu.HostCPU
		hostErr   error
		wantErr   bool
		wantModel string
		wantNest  string
		wantLog   string
	}{
		{"auto intel", nil, qemu.HostCPU{Vendor: "GenuineIntel", Vmx: true, CPUs: 4}, nil, false, "Haswell-noTSX", "vmx", ""},
		{"auto amd off", nil, qemu.HostCPU{Vendor: "AuthenticAMD", Svm: true, CPUs: 4}, nil, false, "Haswell-noTSX", "", "kvm_amd"},
		{"auto no feature off", nil, qemu.HostCPU{Vendor: "GenuineIntel", CPUs: 4}, nil, false, "Haswell-noTSX", "", ""},
		{"auto detection failure off", nil, qemu.HostCPU{}, errors.New("cpuinfo gone"), false, "Haswell-noTSX", "", ""},
		{"auto partial facts on error off", nil, qemu.HostCPU{Vendor: "GenuineIntel", Vmx: true, CPUs: 4}, errors.New("cpuinfo truncated"), false, "Haswell-noTSX", "", ""},
		{"on intel", []string{"-node.cpu-nested=on"}, qemu.HostCPU{Vendor: "GenuineIntel", Vmx: true, CPUs: 4}, nil, false, "Haswell-noTSX", "vmx", ""},
		{"on amd refused", []string{"-node.cpu-nested=on"}, qemu.HostCPU{Vendor: "AuthenticAMD", Svm: true, CPUs: 4}, nil, true, "", "", ""},
		{"on no feature refused", []string{"-node.cpu-nested=on"}, qemu.HostCPU{Vendor: "GenuineIntel", CPUs: 4}, nil, true, "", "", ""},
		{"on detection failure refused", []string{"-node.cpu-nested=on"}, qemu.HostCPU{}, errors.New("cpuinfo gone"), true, "", "", ""},
		{"off intel", []string{"-node.cpu-nested=off"}, qemu.HostCPU{Vendor: "GenuineIntel", Vmx: true, CPUs: 4}, nil, false, "Haswell-noTSX", "", ""},
		{"explicit model honored", []string{"-node.cpu-model=Skylake-Server"}, qemu.HostCPU{Vendor: "GenuineIntel", Vmx: true, CPUs: 4}, nil, false, "Skylake-Server", "vmx", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, s := devDeps(map[string]string{"BBRAFT_NODE_TOKEN": "tok"})
			d.hostCPU = func() (qemu.HostCPU, error) {
				s.add("hostCPU")
				return tc.host, tc.hostErr
			}
			d.serve = func(context.Context, nodelink.DialConfig, *grpc.Server) error {
				s.add("serve")
				close(s.serveCalled)
				return nil
			}
			var stderr strings.Builder
			err := runWithDeps(t.Context(), tc.args, &stderr, d)
			if tc.wantErr {
				if err == nil {
					t.Fatal("want a startup error")
				}
				for _, e := range s.snapshot() {
					if e == "serve" || e == "newManager" || e == "checkHost" {
						t.Fatalf("events %v: nothing past the CPU check may run", s.snapshot())
					}
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if s.cpu.Model != tc.wantModel || s.cpu.Nested != tc.wantNest {
				t.Fatalf("resolved cpu %+v, want model=%q nested=%q", s.cpu, tc.wantModel, tc.wantNest)
			}
			if s.nodeCfg.CPUModel != tc.wantModel {
				t.Fatalf("manager CPUModel %q, want %q", s.nodeCfg.CPUModel, tc.wantModel)
			}
			if s.nodeCfg.Nested != s.cpuMode {
				t.Fatalf("manager Nested %q, want the parsed mode %q", s.nodeCfg.Nested, s.cpuMode)
			}
			if tc.wantLog != "" && !strings.Contains(stderr.String(), tc.wantLog) {
				t.Errorf("stderr missing %q, got %q", tc.wantLog, stderr.String())
			}
		})
	}
}

// The dev/test opt-in routes -node.cpu-model=host through the shared
// qemu.ResolveTestCPU policy, forwards AllowHostCPU to the manager and
// warns at startup. Everything else keeps the production ResolveCPU path.
func TestRunCPUAllowHost(t *testing.T) {
	for _, tc := range []struct {
		name       string
		args       []string
		env        map[string]string
		host       qemu.HostCPU
		wantErr    bool
		wantModel  string
		wantNest   string
		wantEvents []string
	}{
		{"host intel auto", []string{"-node.cpu-allow-host", "-node.cpu-model=host"}, nil,
			qemu.HostCPU{Vendor: "GenuineIntel", Vmx: true, CPUs: 4}, false, "host", "vmx",
			[]string{"hostCPU", "resolveTestCPU", "checkHost", "probeCPU", "newManager", "serve", "shutdown"}},
		{"host amd auto", []string{"-node.cpu-allow-host", "-node.cpu-model=host"}, nil,
			qemu.HostCPU{Vendor: "AuthenticAMD", Svm: true, CPUs: 4}, false, "host", "",
			[]string{"hostCPU", "resolveTestCPU", "checkHost", "probeCPU", "newManager", "serve", "shutdown"}},
		{"host via env opt-in", []string{"-node.cpu-model=host"}, map[string]string{"BBRAFT_NODE_CPU_ALLOW_HOST": "true"},
			qemu.HostCPU{Vendor: "GenuineIntel", Vmx: true, CPUs: 4}, false, "host", "vmx",
			[]string{"hostCPU", "resolveTestCPU", "checkHost", "probeCPU", "newManager", "serve", "shutdown"}},
		{"host nested off skips detection", []string{"-node.cpu-allow-host", "-node.cpu-model=host", "-node.cpu-nested=off"}, nil,
			qemu.HostCPU{}, false, "host", "",
			[]string{"resolveTestCPU", "checkHost", "probeCPU", "newManager", "serve", "shutdown"}},
		{"named model with opt-in stays on ResolveCPU", []string{"-node.cpu-allow-host"}, nil,
			qemu.HostCPU{Vendor: "GenuineIntel", Vmx: true, CPUs: 4}, false, "Haswell-noTSX", "vmx",
			[]string{"hostCPU", "resolveCPU", "checkHost", "probeCPU", "newManager", "serve", "shutdown"}},
		{"host nested on amd refused", []string{"-node.cpu-allow-host", "-node.cpu-model=host", "-node.cpu-nested=on"}, nil,
			qemu.HostCPU{Vendor: "AuthenticAMD", Svm: true, CPUs: 4}, true, "", "",
			[]string{"hostCPU", "resolveTestCPU"}},
		{"host refused without opt-in", []string{"-node.cpu-model=host"}, nil,
			qemu.HostCPU{Vendor: "GenuineIntel", Vmx: true, CPUs: 4}, true, "", "", nil},
		{"max refused with opt-in", []string{"-node.cpu-allow-host", "-node.cpu-model=max"}, nil,
			qemu.HostCPU{Vendor: "GenuineIntel", Vmx: true, CPUs: 4}, true, "", "", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := map[string]string{"BBRAFT_NODE_TOKEN": "tok"}
			for k, v := range tc.env {
				env[k] = v
			}
			d, s := devDeps(env)
			d.hostCPU = func() (qemu.HostCPU, error) {
				s.add("hostCPU")
				return tc.host, nil
			}
			d.serve = func(context.Context, nodelink.DialConfig, *grpc.Server) error {
				s.add("serve")
				close(s.serveCalled)
				return nil
			}
			var stderr strings.Builder
			err := runWithDeps(t.Context(), tc.args, &stderr, d)
			if tc.wantErr {
				if err == nil {
					t.Fatal("want a startup error")
				}
				for _, e := range s.snapshot() {
					if e == "serve" || e == "newManager" || e == "checkHost" {
						t.Fatalf("events %v: nothing past the CPU check may run", s.snapshot())
					}
				}
				if tc.wantEvents != nil {
					if got := s.snapshot(); !equalStrings(got, tc.wantEvents) {
						t.Fatalf("event order %v, want %v", got, tc.wantEvents)
					}
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if s.cpu.Model != tc.wantModel || s.cpu.Nested != tc.wantNest {
				t.Fatalf("resolved cpu %+v, want model=%q nested=%q", s.cpu, tc.wantModel, tc.wantNest)
			}
			if s.nodeCfg.CPUModel != tc.wantModel {
				t.Fatalf("manager CPUModel %q, want %q", s.nodeCfg.CPUModel, tc.wantModel)
			}
			if s.probeCPU.String() != s.cpu.String() {
				t.Fatalf("probe CPU %q, want the resolved %q", s.probeCPU.String(), s.cpu.String())
			}
			if !s.nodeCfg.AllowHostCPU {
				t.Error("opt-in must forward AllowHostCPU to the manager")
			}
			if got := s.snapshot(); !equalStrings(got, tc.wantEvents) {
				t.Fatalf("event order %v, want %v", got, tc.wantEvents)
			}
			if !strings.Contains(stderr.String(), "level=WARN") {
				t.Errorf("opt-in must warn at startup, got %q", stderr.String())
			}
		})
	}
}

// The AMD auto-off reason blames the fixed Intel guest vendor; with the
// host model the guest vendor is the host's, so the log must not claim it.
func TestRunCPUAllowHostAMDReason(t *testing.T) {
	d, s := devDeps(map[string]string{"BBRAFT_NODE_TOKEN": "tok"})
	d.hostCPU = func() (qemu.HostCPU, error) {
		return qemu.HostCPU{Vendor: "AuthenticAMD", Svm: true, CPUs: 4}, nil
	}
	d.serve = func(context.Context, nodelink.DialConfig, *grpc.Server) error {
		s.add("serve")
		close(s.serveCalled)
		return nil
	}
	var stderr strings.Builder
	args := []string{"-node.cpu-allow-host", "-node.cpu-model=host"}
	if err := runWithDeps(t.Context(), args, &stderr, d); err != nil {
		t.Fatal(err)
	}
	out := stderr.String()
	if strings.Contains(out, "fixed Intel guest vendor") {
		t.Errorf("host passthrough must not claim a fixed Intel guest vendor: %q", out)
	}
	if !strings.Contains(out, "AuthenticAMD") {
		t.Errorf("the AMD nested-off reason must still be logged: %q", out)
	}
}

// Default startup warns about nothing; the opt-in warning appears only
// when the operator asked for it.
func TestRunCPUAllowHostWarns(t *testing.T) {
	d, s := devDeps(map[string]string{"BBRAFT_NODE_TOKEN": "tok"})
	d.serve = func(context.Context, nodelink.DialConfig, *grpc.Server) error {
		s.add("serve")
		close(s.serveCalled)
		return nil
	}
	var stderr strings.Builder
	if err := runWithDeps(t.Context(), nil, &stderr, d); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(stderr.String(), "level=WARN") {
		t.Errorf("default startup must not warn: %q", stderr.String())
	}
	if s.nodeCfg.AllowHostCPU {
		t.Error("AllowHostCPU must stay false by default")
	}
}

// off must not touch host detection at all, matching the shared helper's
// no-host-facts contract for NestedOff.
func TestRunCPUNestedOffSkipsDetection(t *testing.T) {
	d, s := devDeps(map[string]string{"BBRAFT_NODE_TOKEN": "tok"})
	d.hostCPU = func() (qemu.HostCPU, error) {
		s.add("hostCPU")
		return qemu.HostCPU{}, errors.New("detection must not run for off")
	}
	d.serve = func(context.Context, nodelink.DialConfig, *grpc.Server) error {
		s.add("serve")
		return nil
	}
	if err := runWithDeps(t.Context(), []string{"-node.cpu-nested=off"}, io.Discard, d); err != nil {
		t.Fatal(err)
	}
	for _, e := range s.snapshot() {
		if e == "hostCPU" {
			t.Fatalf("events %v: off must skip host CPU detection", s.snapshot())
		}
	}
	if s.nodeCfg.Nested != qemu.NestedOff {
		t.Fatalf("manager Nested %q, want off", s.nodeCfg.Nested)
	}
}

func TestRunShutdownErrorIsReported(t *testing.T) {
	d, s := devDeps(map[string]string{"BBRAFT_NODE_TOKEN": "tok"})
	s.serveResult = func(context.Context) error { return nil }
	s.mgr.shutdownErrs = []error{errors.New("cleanup failed"), nil}
	err := runWithDeps(t.Context(), nil, io.Discard, d)
	if err == nil || !strings.Contains(err.Error(), "cleanup failed") {
		t.Fatalf("run error %v, want the shutdown failure", err)
	}
	// Shutdown is retry-safe: a second call reports the real outcome.
	if err := s.mgr.Shutdown(t.Context()); err != nil {
		t.Fatalf("shutdown retry = %v, want nil", err)
	}
}

func TestRunConfigErrors(t *testing.T) {
	for _, tc := range []struct {
		name string
		env  map[string]string
		args []string
	}{
		{"public server addr", map[string]string{"BBRAFT_NODE_TOKEN": "tok"}, []string{"-server.addr=8.8.8.8:7701"}},
		{"no token", nil, nil},
		{"bad flag", map[string]string{"BBRAFT_NODE_TOKEN": "tok"}, []string{"-node.boot-timeout=0s"}},
		{"malformed env", map[string]string{"BBRAFT_NODE_TOKEN": "tok", "BBRAFT_NODE_CAPACITY_VCPUS": "abc"}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, s := devDeps(tc.env)
			if err := runWithDeps(t.Context(), tc.args, io.Discard, d); err == nil {
				t.Fatal("want a config error")
			}
			if got := s.snapshot(); len(got) != 0 {
				t.Fatalf("events %v: nothing may run on bad config", got)
			}
		})
	}
}

func TestRunTokenFile(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "node-token")
	if err := os.WriteFile(p, []byte("file-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	d, s := devDeps(nil)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- runWithDeps(ctx, []string{"-node.token-file=" + p}, io.Discard, d)
	}()
	<-s.serveCalled
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if s.serveCfg.Token != "file-token" {
		t.Fatalf("dial token %q, want file-token", s.serveCfg.Token)
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestRunNestedVMXFallback covers a host whose CPU has VMX but lacks APICv
// (a desktop Intel CPU without APICv): the first probe is refused with QEMU's missing-feature
// diagnostics, the node retries once with exactly those VMX features turned
// off, warns, and hands the probed option's features to the manager.
func TestRunNestedVMXFallback(t *testing.T) {
	d, s := devDeps(map[string]string{"BBRAFT_NODE_TOKEN": "tok"})
	var probed []string
	d.probeCPU = func(_ context.Context, option qemu.CPUOption, _ qemu.CPUProbeOptions) (qemu.CPUProbeEvidence, error) {
		s.add("probeCPU")
		probed = append(probed, option.String())
		if len(probed) == 1 {
			return qemu.CPUProbeEvidence{}, errors.New("qemu: cpu probe: qemu exited during startup: exit status 1: " +
				"qemu-system-x86_64: warning: host doesn't support requested feature: MSR(48BH).vmx-apicv-register [bit 8]\n" +
				"qemu-system-x86_64: warning: host doesn't support requested feature: MSR(48DH).vmx-posted-intr [bit 7]\n" +
				"qemu-system-x86_64: Host doesn't support requested features")
		}
		return qemu.CPUProbeEvidence{Status: "prelaunch", CPUs: 1, QEMUVersion: "10.0.0", PID: 4242}, nil
	}
	s.serveResult = func(context.Context) error { return nil }
	var logs strings.Builder
	if err := runWithDeps(t.Context(), nil, &logs, d); err != nil {
		t.Fatalf("run: %v", err)
	}
	want := []string{"Haswell-noTSX,+vmx,enforce", "Haswell-noTSX,+vmx,-vmx-apicv-register,-vmx-posted-intr,enforce"}
	if !equalStrings(probed, want) {
		t.Fatalf("probes %q, want %q", probed, want)
	}
	if !equalStrings(s.nodeCfg.CPUFeatures, []string{"-vmx-apicv-register", "-vmx-posted-intr"}) {
		t.Fatalf("manager CPUFeatures %q, want the probed toggles", s.nodeCfg.CPUFeatures)
	}
	if !strings.Contains(logs.String(), "level=WARN") || !strings.Contains(logs.String(), "dropped=vmx-apicv-register,vmx-posted-intr") {
		t.Fatalf("no fallback warning naming the dropped features in:\n%s", logs.String())
	}
}

// TestRunNestedFallbackKeepsBaseFeaturesStrict: a missing feature of the
// fixed model itself (pcid) next to VMX gaps is not retried.
func TestRunNestedFallbackKeepsBaseFeaturesStrict(t *testing.T) {
	d, s := devDeps(map[string]string{"BBRAFT_NODE_TOKEN": "tok"})
	d.probeCPU = func(context.Context, qemu.CPUOption, qemu.CPUProbeOptions) (qemu.CPUProbeEvidence, error) {
		s.add("probeCPU")
		return qemu.CPUProbeEvidence{}, errors.New("qemu: cpu probe: " +
			"qemu-system-x86_64: warning: host doesn't support requested feature: CPUID.01H:ECX.pcid [bit 17]\n" +
			"qemu-system-x86_64: warning: host doesn't support requested feature: MSR(48BH).vmx-apicv-vid [bit 9]")
	}
	if err := runWithDeps(t.Context(), nil, io.Discard, d); err == nil {
		t.Fatal("run succeeded with a missing base-model feature")
	}
	if got := s.snapshot(); !equalStrings(got, []string{"hostCPU", "resolveCPU", "checkHost", "probeCPU"}) {
		t.Fatalf("events %v: exactly one probe and no manager", got)
	}
}

// TestRunCPUFeaturesFlag: operator toggles reach the probe and the manager.
func TestRunCPUFeaturesFlag(t *testing.T) {
	d, s := devDeps(map[string]string{"BBRAFT_NODE_TOKEN": "tok", "BBRAFT_NODE_CPU_FEATURES": "-vmx-apicv-vid"})
	s.serveResult = func(context.Context) error { return nil }
	if err := runWithDeps(t.Context(), nil, io.Discard, d); err != nil {
		t.Fatalf("run: %v", err)
	}
	if got, want := s.probeCPU.String(), "Haswell-noTSX,+vmx,-vmx-apicv-vid,enforce"; got != want {
		t.Fatalf("probe CPU %q, want %q", got, want)
	}
	if !equalStrings(s.nodeCfg.CPUFeatures, []string{"-vmx-apicv-vid"}) {
		t.Fatalf("manager CPUFeatures %q", s.nodeCfg.CPUFeatures)
	}
	d, _ = devDeps(map[string]string{"BBRAFT_NODE_TOKEN": "tok", "BBRAFT_NODE_CPU_FEATURES": "+vmx"})
	if err := runWithDeps(t.Context(), nil, io.Discard, d); err == nil {
		t.Fatal("run accepted +vmx in -node.cpu-features")
	}
}
