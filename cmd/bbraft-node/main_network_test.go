package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"maps"
	"net/netip"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"

	nodenet "github.com/nathabonfim59/bamboo-raft/internal/node/net"
	"github.com/nathabonfim59/bamboo-raft/internal/nodelink"
)

// errUnexpectedNetwork makes an accidental TAP construction in a dev/test
// fixture fail loudly instead of silently exercising production networking.
var errUnexpectedNetwork = errors.New("test: unexpected network construction")

// fakeNetwork is the command's network handle in startup tests. It records
// the checks and closures the command performs; every lifecycle operation is
// a test failure because the command never drives them.
type fakeNetwork struct {
	s        *spy
	checkErr error
	closeErr error

	checkCalls    int
	closes        int
	setupCalls    int
	releaseCalls  int
	reconcileCall int
	unexpectedOps []string
}

func (n *fakeNetwork) Setup(context.Context, string) (*nodenet.Lease, error) {
	n.setupCalls++
	n.unexpectedOps = append(n.unexpectedOps, "Setup")
	return nil, errors.New("test: command startup must not call Setup")
}

func (n *fakeNetwork) Release(context.Context, string) error {
	n.releaseCalls++
	n.unexpectedOps = append(n.unexpectedOps, "Release")
	return errors.New("test: command startup must not call Release")
}

func (n *fakeNetwork) SSHEndpoint(string) (netip.AddrPort, bool) {
	n.unexpectedOps = append(n.unexpectedOps, "SSHEndpoint")
	return netip.AddrPort{}, false
}

func (n *fakeNetwork) Allocations() []nodenet.Allocation {
	n.unexpectedOps = append(n.unexpectedOps, "Allocations")
	return nil
}

func (n *fakeNetwork) Reconcile(context.Context, []string) (nodenet.ReconcileReport, error) {
	n.reconcileCall++
	n.unexpectedOps = append(n.unexpectedOps, "Reconcile")
	return nodenet.ReconcileReport{}, errors.New("test: command startup must not call Reconcile")
}

func (n *fakeNetwork) CheckHost(context.Context) error {
	n.s.add("checkNetwork")
	n.checkCalls++
	return n.checkErr
}

func (n *fakeNetwork) Close() error {
	n.s.add("closeNetwork")
	n.closes++
	return n.closeErr
}

// networkFactory records the exact validated configuration and returns a fake
// network with the configured check/close outcomes.
func (s *spy) networkFactory(cfg nodenet.Config, log *slog.Logger) (nodeNetwork, error) {
	s.networkCalls++
	s.networkCfg = cfg
	s.networkLog = log
	s.add("newNetwork")
	if s.newNetworkErr != nil {
		return nil, s.newNetworkErr
	}
	s.net = &fakeNetwork{s: s, checkErr: s.netCheckErr, closeErr: s.netCloseErr}
	return s.net, nil
}

// assertNoNetworkLifecycleOps fails when the command touched any per-sandbox
// network operation. Startup only constructs, checks and closes the manager;
// Setup/Release/Reconcile and endpoint inspections belong to the lifecycle
// owner, including on failure and cancellation cleanup paths.
func assertNoNetworkLifecycleOps(t *testing.T, s *spy) {
	t.Helper()
	if s.net == nil {
		return
	}
	if len(s.net.unexpectedOps) != 0 {
		t.Fatalf("command drove network lifecycle operations: %v", s.net.unexpectedOps)
	}
}

// serveReturns makes the injected link return err immediately.
func serveReturns(d *deps, s *spy, err error) {
	d.serve = func(context.Context, nodelink.DialConfig, *grpc.Server) error {
		s.add("serve")
		return err
	}
}

// tapNetworkArgs extends the shared complete TAP flags with the production
// bridge, resolvers, management exclusion and ingress interface so the
// command test can compare the whole nodenet configuration.
func tapNetworkArgs() []string {
	return append(tapArgs(),
		"-node.network-mode=tap",
		"-node.network-bridge=bbraft9",
		"-node.network-resolvers=1.1.1.1,9.9.9.9",
		"-node.network-management=100.64.0.0/10",
		"-node.ssh-ingress-iface=eth0",
	)
}

// tapJailDeps is fakeDeps for a complete tap-mode startup: the recording
// network factory and jail fakes on the default directories.
func tapJailDeps(t *testing.T) (deps, *spy) {
	t.Helper()
	d, s := fakeDeps(map[string]string{"BBRAFT_NODE_TOKEN": "tok"})
	d.newNetwork = s.networkFactory
	return withJailFakes(t, d, s, defaultDataDir, defaultImageDir).d, s
}

// tapJailArgs are tapNetworkArgs plus the jail flags.
func tapJailArgs() []string { return append(tapNetworkArgs(), jailFlags()...) }

// tapJailEnv is tapEnv plus the jail variables.
func tapJailEnv() map[string]string {
	env := tapEnv()
	maps.Copy(env, jailEnv())
	return env
}

// wantTail fails unless events end with tail.
func wantTail(t *testing.T, events, tail []string) {
	t.Helper()
	if len(events) < len(tail) || !equalStrings(events[len(events)-len(tail):], tail) {
		t.Fatalf("event tail %v, want %v", events, tail)
	}
}

func tapEnv() map[string]string {
	return map[string]string{
		"BBRAFT_NODE_TOKEN":              "tok",
		"BBRAFT_NODE_NETWORK_MODE":       "tap",
		"BBRAFT_NODE_NETWORK_BRIDGE":     "bbraft9",
		"BBRAFT_NODE_NETWORK_PREFIX":     "10.42.0.0/16",
		"BBRAFT_NODE_NETWORK_GATEWAY":    "10.42.0.1",
		"BBRAFT_NODE_NETWORK_RESOLVERS":  "1.1.1.1,9.9.9.9",
		"BBRAFT_NODE_NETWORK_MANAGEMENT": "100.64.0.0/10",
		"BBRAFT_NODE_SSH_INGRESS_ADDR":   "203.0.113.10",
		"BBRAFT_NODE_SSH_INGRESS_IFACE":  "eth0",
		"BBRAFT_NODE_SSH_PORT_RANGE":     "22000-22999",
	}
}

func tapNetworkArgsReplacing(flag, value string) []string {
	args := tapNetworkArgs()
	for i, a := range args {
		if strings.HasPrefix(a, flag+"=") {
			args[i] = flag + "=" + value
			return args
		}
	}
	return append(args, flag+"="+value)
}

// TestRunTapNetworkWiring proves the command passes the exact validated TAP
// configuration (from flags and from BBRAFT_* variables) to the factory,
// checks the host, hands the same object to the node manager and closes it
// exactly once after Shutdown.
func TestRunTapNetworkWiring(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		env  map[string]string
	}{
		{name: "flags", args: append(tapNetworkArgs(), jailFlags()...), env: map[string]string{"BBRAFT_NODE_TOKEN": "tok"}},
		{name: "environment", env: tapJailEnv()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, s := fakeDeps(tc.env)
			d.newNetwork = s.networkFactory
			serveReturns(&d, s, nil)
			d = withJailFakes(t, d, s, defaultDataDir, defaultImageDir).d
			var stderr strings.Builder

			if err := runWithDeps(t.Context(), tc.args, &stderr, d); err != nil {
				t.Fatal(err)
			}

			want := nodenet.Config{
				Bridge:       "bbraft9",
				Prefix:       netip.MustParsePrefix("10.42.0.0/16"),
				Gateway:      netip.MustParseAddr("10.42.0.1"),
				Resolvers:    []netip.Addr{netip.MustParseAddr("1.1.1.1"), netip.MustParseAddr("9.9.9.9")},
				Management:   []netip.Prefix{netip.MustParsePrefix("100.64.0.0/10")},
				IngressAddr:  netip.MustParseAddr("203.0.113.10"),
				IngressIface: "eth0",
				SSHPorts:     nodenet.PortRange{Min: 22000, Max: 22999},
				StateDir:     filepath.Join(defaultDataDir, networkStateDirName),
			}
			if s.networkCalls != 1 {
				t.Fatalf("factory calls = %d, want 1", s.networkCalls)
			}
			// Positive control for the fail-closed fixtures: a successful
			// startup probes host facts and reads the credential exactly once.
			if s.factsCalls != 1 || s.tokenLookups != 1 {
				t.Fatalf("facts=%d token lookups=%d, want 1 and 1", s.factsCalls, s.tokenLookups)
			}
			if !reflect.DeepEqual(s.networkCfg, want) {
				t.Fatalf("factory config = %+v, want %+v", s.networkCfg, want)
			}
			if s.networkLog == nil || s.networkLog != s.nodeCfg.Log {
				t.Fatal("factory did not receive the manager logger")
			}
			if s.net == nil {
				t.Fatal("factory returned no network")
			}
			if s.nodeCfg.Network != s.net {
				t.Fatalf("manager network = %v, want the constructed fake", s.nodeCfg.Network)
			}
			if s.nodeCfg.AllowUserNetworking {
				t.Fatal("TAP mode must not allow user networking")
			}
			if s.net.checkCalls != 1 {
				t.Fatalf("CheckHost calls = %d, want 1", s.net.checkCalls)
			}
			if s.net.closes != 1 {
				t.Fatalf("Close calls = %d, want 1", s.net.closes)
			}
			assertNoNetworkLifecycleOps(t, s)
			wantEvents := slices.Concat([]string{"hostCPU", "resolveCPU"}, jailStartEvents,
				[]string{"newNetwork", "checkNetwork", "newManager", "serve", "shutdown", "closeNetwork"}, jailCloseEvents)
			if got := s.snapshot(); !equalStrings(got, wantEvents) {
				t.Fatalf("event order %v, want %v", got, wantEvents)
			}
			if !strings.Contains(stderr.String(), "network_mode=tap") {
				t.Errorf("startup log misses the network mode: %q", stderr.String())
			}
			if !strings.Contains(stderr.String(), "bbraft-node ready") {
				t.Errorf("startup log misses readiness: %q", stderr.String())
			}
		})
	}
}

// TestRunDefaultTapConfigFailsClosed proves the production default is TAP:
// an incomplete configuration fails before host facts, credentials, probes,
// the network factory or any manager side effect.
func TestRunDefaultTapConfigFailsClosed(t *testing.T) {
	d, s := fakeDeps(map[string]string{"BBRAFT_NODE_TOKEN": "tok"})
	d.newNetwork = s.networkFactory
	serveReturns(&d, s, nil)

	err := runWithDeps(t.Context(), nil, io.Discard, d)
	if err == nil {
		t.Fatal("want a configuration error")
	}
	for _, missing := range []string{"-node.network-prefix", "-node.network-gateway", "-node.ssh-ingress-addr", "-node.ssh-port-range"} {
		if !strings.Contains(err.Error(), missing) {
			t.Errorf("error %q does not name %s", err, missing)
		}
	}
	if got := s.snapshot(); len(got) != 0 {
		t.Fatalf("events %v: nothing may run on an incomplete default TAP configuration", got)
	}
	if s.networkCalls != 0 {
		t.Fatalf("factory calls = %d, want 0", s.networkCalls)
	}
	if s.factsCalls != 0 {
		t.Fatalf("host facts probed %d times before the fail-closed check", s.factsCalls)
	}
	if s.tokenLookups != 0 {
		t.Fatalf("node token read %d times before the fail-closed check", s.tokenLookups)
	}
}

// TestRunUserNetworkMode proves the explicit dev/test fallback never
// constructs or checks production networking and tells the node manager to
// use QEMU user networking.
func TestRunUserNetworkMode(t *testing.T) {
	d, s := fakeDeps(map[string]string{
		"BBRAFT_NODE_TOKEN":        "tok",
		"BBRAFT_NODE_NETWORK_MODE": networkModeUser,
	})
	d.newNetwork = s.networkFactory
	serveReturns(&d, s, nil)
	var stderr strings.Builder

	if err := runWithDeps(t.Context(), nil, &stderr, d); err != nil {
		t.Fatal(err)
	}
	if s.networkCalls != 0 {
		t.Fatalf("factory calls = %d, want 0 in user mode", s.networkCalls)
	}
	for _, e := range s.snapshot() {
		if e == "newNetwork" || e == "checkNetwork" || e == "closeNetwork" {
			t.Fatalf("events %v: user mode must not construct, check or close production networking", s.snapshot())
		}
	}
	if s.nodeCfg.Network != nil {
		t.Fatalf("manager network = %v, want nil in user mode", s.nodeCfg.Network)
	}
	if !s.nodeCfg.AllowUserNetworking {
		t.Fatal("user mode must set AllowUserNetworking")
	}
	logs := stderr.String()
	for _, want := range []string{"network_mode=user", "dev_test=true"} {
		if !strings.Contains(logs, want) {
			t.Errorf("startup log %q misses %q", logs, want)
		}
	}
	if strings.Contains(logs, "level=WARN") {
		t.Errorf("user mode is reported at INFO, not WARN: %q", logs)
	}
}

// TestRunUnsafeTapConfigFailsClosed covers malformed and unsafe production
// configurations: networking() refuses each before the factory exists.
func TestRunUnsafeTapConfigFailsClosed(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
	}{
		{name: "public guest prefix", args: tapNetworkArgsReplacing("-node.network-prefix", "8.8.8.0/24")},
		{name: "gateway outside prefix", args: tapNetworkArgsReplacing("-node.network-gateway", "10.43.0.1")},
		{name: "resolver inside prefix", args: tapNetworkArgsReplacing("-node.network-resolvers", "10.42.0.53")},
		{name: "management overlaps prefix", args: tapNetworkArgsReplacing("-node.network-management", "10.42.0.0/24")},
		{name: "ingress inside prefix", args: tapNetworkArgsReplacing("-node.ssh-ingress-addr", "10.42.0.10")},
		{name: "privileged ssh port", args: tapNetworkArgsReplacing("-node.ssh-port-range", "22-22000")},
		{name: "ingress interface is bridge", args: tapNetworkArgsReplacing("-node.ssh-ingress-iface", "bbraft9")},
		{name: "invalid bridge name", args: tapNetworkArgsReplacing("-node.network-bridge", "bad bridge")},
		{name: "missing gateway", args: []string{
			"-node.network-mode=tap",
			"-node.network-prefix=10.42.0.0/16",
			"-node.ssh-ingress-addr=203.0.113.10",
			"-node.ssh-port-range=22000-22999",
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, s := fakeDeps(map[string]string{"BBRAFT_NODE_TOKEN": "tok"})
			d.newNetwork = s.networkFactory
			serveReturns(&d, s, nil)
			var stderr strings.Builder

			if err := runWithDeps(t.Context(), tc.args, &stderr, d); err == nil {
				t.Fatal("want a configuration error")
			}
			if got := s.snapshot(); len(got) != 0 {
				t.Fatalf("events %v: nothing may run on unsafe configuration", got)
			}
			if s.networkCalls != 0 {
				t.Fatalf("factory calls = %d, want 0", s.networkCalls)
			}
			if s.factsCalls != 0 {
				t.Fatalf("host facts probed %d times on unsafe configuration", s.factsCalls)
			}
			if s.tokenLookups != 0 {
				t.Fatalf("node token read %d times on unsafe configuration", s.tokenLookups)
			}
			if strings.Contains(stderr.String(), "bbraft-node ready") {
				t.Errorf("readiness logged after a configuration failure: %q", stderr.String())
			}
		})
	}
}

// TestRunNetworkFactoryFailure proves a construction error is returned
// without a host check, manager, listener or readiness, and nothing is closed
// because no handle exists.
func TestRunNetworkFactoryFailure(t *testing.T) {
	d, s := tapJailDeps(t)
	want := errors.New("nodenet: state directory disagrees")
	s.newNetworkErr = want
	serveReturns(&d, s, nil)
	var stderr strings.Builder

	err := runWithDeps(t.Context(), tapJailArgs(), &stderr, d)
	if !errors.Is(err, want) {
		t.Fatalf("run error %v, want %v", err, want)
	}
	wantEvents := slices.Concat([]string{"hostCPU", "resolveCPU"}, jailStartEvents, []string{"newNetwork"}, jailCloseEvents)
	if got := s.snapshot(); !equalStrings(got, wantEvents) {
		t.Fatalf("event order %v, want %v", got, wantEvents)
	}
	if s.net != nil {
		t.Fatal("a failed factory must not leave a network handle")
	}
	if strings.Contains(stderr.String(), "bbraft-node ready") {
		t.Errorf("readiness logged after a factory failure: %q", stderr.String())
	}
}

// TestRunNetworkCheckHostFailure proves a host check failure aborts startup
// after exactly one Close and before the manager, listener or readiness.
func TestRunNetworkCheckHostFailure(t *testing.T) {
	d, s := tapJailDeps(t)
	want := errors.New("nodenet: net.ipv4.ip_forward must be 1")
	s.netCheckErr = want
	serveReturns(&d, s, nil)
	var stderr strings.Builder

	err := runWithDeps(t.Context(), tapJailArgs(), &stderr, d)
	if !errors.Is(err, want) {
		t.Fatalf("run error %v, want %v", err, want)
	}
	wantEvents := slices.Concat([]string{"hostCPU", "resolveCPU"}, jailStartEvents, []string{"newNetwork", "checkNetwork", "closeNetwork"}, jailCloseEvents)
	if got := s.snapshot(); !equalStrings(got, wantEvents) {
		t.Fatalf("event order %v, want %v", got, wantEvents)
	}
	if s.net == nil || s.net.closes != 1 {
		t.Fatalf("network closes = %v, want exactly 1", s.net)
	}
	assertNoNetworkLifecycleOps(t, s)
	if strings.Contains(stderr.String(), "bbraft-node ready") {
		t.Errorf("readiness logged after a host-check failure: %q", stderr.String())
	}
}

// TestRunCPUProbeFailureSkipsNetwork proves the network factory runs only
// after the jailed CPU probe succeeds.
func TestRunCPUProbeFailureSkipsNetwork(t *testing.T) {
	d, s := fakeDeps(map[string]string{"BBRAFT_NODE_TOKEN": "tok"})
	d.newNetwork = s.networkFactory
	f := withJailFakes(t, d, s, defaultDataDir, defaultImageDir)
	want := errors.New("qemu: enforced CPU cannot be realized")
	f.probeErrs = []error{want}
	d = f.d
	serveReturns(&d, s, nil)

	err := runWithDeps(t.Context(), tapJailArgs(), io.Discard, d)
	if !errors.Is(err, want) {
		t.Fatalf("run error %v, want %v", err, want)
	}
	wantEvents := slices.Concat([]string{"hostCPU", "resolveCPU"}, jailStartEvents, jailCloseEvents)
	if got := s.snapshot(); !equalStrings(got, wantEvents) {
		t.Fatalf("event order %v, want %v", got, wantEvents)
	}
	if s.networkCalls != 0 {
		t.Fatalf("factory calls = %d, want 0 after a CPU probe failure", s.networkCalls)
	}
}

// TestRunManagerFailureClosesNetwork proves a node manager construction
// error still closes the already-checked network exactly once.
func TestRunManagerFailureClosesNetwork(t *testing.T) {
	d, s := tapJailDeps(t)
	want := errors.New("node: data dir is locked")
	s.newManagerErr = want
	serveReturns(&d, s, nil)

	err := runWithDeps(t.Context(), tapJailArgs(), io.Discard, d)
	if !errors.Is(err, want) {
		t.Fatalf("run error %v, want %v", err, want)
	}
	wantEvents := slices.Concat([]string{"hostCPU", "resolveCPU"}, jailStartEvents, []string{"newNetwork", "checkNetwork", "newManager", "closeNetwork"}, jailCloseEvents)
	if got := s.snapshot(); !equalStrings(got, wantEvents) {
		t.Fatalf("event order %v, want %v", got, wantEvents)
	}
	if s.net == nil || s.net.closes != 1 {
		t.Fatalf("network closes = %v, want exactly 1", s.net)
	}
	assertNoNetworkLifecycleOps(t, s)
}

// TestRunNetworkErrorsAreJoined proves serve, shutdown and close failures all
// reach the caller, with Shutdown running before the network handle closes.
func TestRunNetworkErrorsAreJoined(t *testing.T) {
	d, s := tapJailDeps(t)
	serveErr := errors.New("link loop failed")
	shutdownErr := errors.New("manager cleanup failed")
	closeErr := errors.New("network close failed")
	serveReturns(&d, s, serveErr)
	s.mgr.shutdownErrs = []error{shutdownErr, nil}
	s.netCloseErr = closeErr

	err := runWithDeps(t.Context(), tapJailArgs(), io.Discard, d)
	for _, want := range []error{serveErr, shutdownErr, closeErr} {
		if !errors.Is(err, want) {
			t.Errorf("run error %v does not wrap %v", err, want)
		}
	}
	if s.net == nil || s.net.closes != 1 {
		t.Fatalf("network closes = %v, want exactly 1", s.net)
	}
	assertNoNetworkLifecycleOps(t, s)
	wantTail(t, s.snapshot(), slices.Concat([]string{"shutdown", "closeNetwork"}, jailCloseEvents))
}

// TestRunCancelClosesNetwork proves a canceled serve joins, shuts the manager
// down and still closes the network exactly once.
func TestRunCancelClosesNetwork(t *testing.T) {
	d, s := tapJailDeps(t)
	entered := make(chan struct{})
	d.serve = func(ctx context.Context, _ nodelink.DialConfig, _ *grpc.Server) error {
		s.add("serve")
		close(entered)
		<-ctx.Done()
		return nil
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runWithDeps(ctx, tapJailArgs(), io.Discard, d) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("startup never reached serve")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("canceled startup did not join")
	}
	if s.net == nil || s.net.closes != 1 {
		t.Fatalf("network closes = %v, want exactly 1", s.net)
	}
	assertNoNetworkLifecycleOps(t, s)
	wantTail(t, s.snapshot(), slices.Concat([]string{"shutdown", "closeNetwork"}, jailCloseEvents))
}

// TestRunCloseErrorIsReported proves a failing network Close after a normal
// run still reaches the caller, after Shutdown and exactly once.
func TestRunCloseErrorIsReported(t *testing.T) {
	d, s := tapJailDeps(t)
	want := errors.New("network close failed")
	s.netCloseErr = want
	serveReturns(&d, s, nil)

	err := runWithDeps(t.Context(), tapJailArgs(), io.Discard, d)
	if !errors.Is(err, want) {
		t.Fatalf("run error %v, want %v", err, want)
	}
	if s.net == nil || s.net.closes != 1 {
		t.Fatalf("network closes = %v, want exactly 1", s.net)
	}
	assertNoNetworkLifecycleOps(t, s)
	wantTail(t, s.snapshot(), slices.Concat([]string{"shutdown", "closeNetwork"}, jailCloseEvents))
}

// TestRealNetworkFactoryUsesPublishedAPI proves the production factory
// constructs the published manager on a temporary state directory and fails
// closed through nodenet's own validation. It never runs host checks.
func TestRealNetworkFactoryUsesPublishedAPI(t *testing.T) {
	dir := t.TempDir()
	log := slog.New(slog.DiscardHandler)
	cfg := nodenet.Config{
		Bridge:      "bbraft0",
		Prefix:      netip.MustParsePrefix("10.42.0.0/16"),
		Gateway:     netip.MustParseAddr("10.42.0.1"),
		Resolvers:   []netip.Addr{netip.MustParseAddr("1.1.1.1")},
		IngressAddr: netip.MustParseAddr("203.0.113.10"),
		SSHPorts:    nodenet.PortRange{Min: 22000, Max: 22999},
		StateDir:    filepath.Join(dir, networkStateDirName),
	}
	n, err := realDeps().newNetwork(cfg, log)
	if err != nil {
		t.Fatalf("construct published network manager: %v", err)
	}
	if _, ok := n.(*nodenet.Manager); !ok {
		t.Fatalf("factory returned %T, want *nodenet.Manager", n)
	}
	if err := n.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	bad := cfg
	bad.Prefix = netip.Prefix{}
	if _, err := realDeps().newNetwork(bad, log); err == nil {
		t.Fatal("published constructor accepted an invalid configuration")
	}
}

// Compile-time checks keep the test stand-ins honest against the command
// interfaces.
var (
	_ nodeNetwork = (*fakeNetwork)(nil)
	_ vmManager   = (*fakeManager)(nil)
)

// startupRunner is a [nodenet.Runner] for the real network manager: it answers
// the host probes with a fixed address list and records every state-changing
// command, so a test can prove start-up changed nothing.
type startupRunner struct {
	addrs string
	runs  []string
	outs  []string
}

func (r *startupRunner) Run(_ context.Context, name string, args []string, _ []byte) error {
	r.runs = append(r.runs, name+" "+strings.Join(args, " "))
	return nil
}

func (r *startupRunner) Output(_ context.Context, name string, args []string) ([]byte, error) {
	cmd := name + " " + strings.Join(args, " ")
	r.outs = append(r.outs, cmd)
	switch cmd {
	case "ip -Version", "nft --version":
		return []byte("ok\n"), nil
	case "ip -j -4 addr show":
		return []byte(r.addrs), nil
	}
	return nil, errors.New("test: unexpected command " + cmd)
}

// TestRunRefusesGuestPrefixOverlappingAHostInterface is the two-node e2e
// failure at command level: the host's own interface holds 10.42.0.2/16 and
// the guest prefix is 10.42.0.0/16. Start-up runs the real network manager's
// CheckHost right after construction, which refuses before the node manager,
// the link, any TAP or any nft/ip change exists.
func TestRunRefusesGuestPrefixOverlappingAHostInterface(t *testing.T) {
	d, s := tapJailDeps(t)
	runner := &startupRunner{addrs: `[{"ifname":"enp0s2","addr_info":[{"local":"10.42.0.2","prefixlen":16}]},` +
		`{"ifname":"eth0","addr_info":[{"local":"203.0.113.10","prefixlen":24}]}]`}
	d.newNetwork = func(cfg nodenet.Config, log *slog.Logger) (nodeNetwork, error) {
		s.add("newNetwork")
		cfg.StateDir = t.TempDir()
		return nodenet.New(cfg, nodenet.Deps{Runner: runner, Logger: log})
	}
	serveReturns(&d, s, nil)
	var stderr strings.Builder

	err := runWithDeps(t.Context(), tapJailArgs(), &stderr, d)
	if err == nil {
		t.Fatal("start-up accepted a guest prefix that overlaps a host interface")
	}
	for _, want := range []string{"node networking: check host", "enp0s2", "10.42.0.2/16", "10.42.0.0/16", "-node.network-prefix"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err, want)
		}
	}
	for _, event := range s.snapshot() {
		if event == "newManager" || event == "serve" {
			t.Errorf("start-up reached %q after the refusal: %v", event, s.snapshot())
		}
	}
	if len(runner.runs) != 0 {
		t.Errorf("start-up changed the host before refusing: %v", runner.runs)
	}
	if strings.Contains(stderr.String(), "bbraft-node ready") || strings.Contains(stderr.String(), "vm networking ready") {
		t.Errorf("readiness logged after the refusal: %q", stderr.String())
	}
}
