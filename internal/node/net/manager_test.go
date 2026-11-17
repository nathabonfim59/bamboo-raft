package nodenet

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// netTestStateFiles returns the committed state files in dir.
func netTestStateFiles(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read state dir: %v", err)
	}
	var out []string
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), stateSuffix) {
			out = append(out, entry.Name())
		}
	}
	return out
}

func TestSetupLifecycle(t *testing.T) {
	cfg := netTestConfig(t)
	m, runner, tapper := netTestManager(t, cfg)
	ctx := t.Context()

	first, err := m.Setup(ctx, "bx_abcdefgh")
	if err != nil {
		t.Fatalf("Setup first: %v", err)
	}
	defer first.Close()
	if first.GuestIP != netip.MustParseAddr("10.42.0.2") {
		t.Errorf("guest ip = %s, want 10.42.0.2", first.GuestIP)
	}
	if first.SSHPort != 30000 {
		t.Errorf("ssh port = %d, want 30000", first.SSHPort)
	}
	if first.Prefix != cfg.Prefix || first.Gateway != cfg.Gateway || first.Bridge != cfg.Bridge {
		t.Errorf("allocation network identity = %s/%s/%s", first.Prefix, first.Gateway, first.Bridge)
	}
	if !slices.Equal(first.Resolvers, cfg.Resolvers) {
		t.Errorf("resolvers = %v, want %v", first.Resolvers, cfg.Resolvers)
	}
	if want := netip.MustParseAddrPort("203.0.113.10:30000"); first.SSHEndpoint != want {
		t.Errorf("ssh endpoint = %s, want %s", first.SSHEndpoint, want)
	}
	if first.Tap == nil || first.Vhost == nil {
		t.Fatal("lease must carry pre-opened TAP and vhost descriptors")
	}
	if first.MAC != macForAddr(first.GuestIP) || first.TAPName != tapFor("bx_abcdefgh") {
		t.Errorf("identity is not derived from the allocation")
	}
	// The host TAP end must never share the guest's MAC: QEMU and the bridge
	// source filter use lease.MAC, the link itself uses the host prefix.
	hostMAC := hostMACForAddr(first.GuestIP)
	if first.MAC == hostMAC {
		t.Fatalf("host TAP MAC %s equals the guest MAC", hostMAC)
	}
	if got := runner.linkMAC(first.TAPName); got != hostMAC {
		t.Errorf("host TAP MAC = %q, want %q (guest MAC is %q)", got, hostMAC, first.MAC)
	}
	if got := tapper.seen(); !slices.Equal(got, []string{tapFor("bx_abcdefgh")}) {
		t.Errorf("tapper opened %v", got)
	}
	if got := tapper.persistedCount(); got != 1 {
		t.Errorf("taps persisted = %d, want 1", got)
	}

	// Rules must exist before the TAP can emit a frame, and the state file is
	// the last step.
	rules := runner.indexOf("nft -f -")
	up := runner.indexOf("ip link set dev " + first.TAPName + " up")
	if rules < 0 || up < 0 {
		t.Fatalf("rules applied at %d, tap up at %d", rules, up)
	}
	if rules > up {
		t.Error("nftables rules must be applied before the TAP is brought up")
	}
	for _, want := range []string{
		"ip link add bbraft0 type bridge",
		"ip addr replace 10.42.0.1/16 dev bbraft0",
		"ip link set dev bbraft0 up",
		"ip link set dev " + first.TAPName + " master bbraft0",
		"ip link set dev " + first.TAPName + " type bridge_slave isolated on",
		"ip link set dev " + first.TAPName + " address " + hostMACForAddr(first.GuestIP) + " mtu 1500",
		"ip link set dev " + first.TAPName + " alias " + aliasFor("bx_abcdefgh"),
	} {
		if runner.indexOf(want) < 0 {
			t.Errorf("missing command %q; got %v", want, runner.commands())
		}
	}
	if got := netTestStateFiles(t, cfg.StateDir); !slices.Equal(got, []string{"bx_abcdefgh.json"}) {
		t.Errorf("state files = %v", got)
	}
	if !strings.Contains(runner.lastNftBatch(t), "10.42.0.2") {
		t.Error("rules do not contain the allocation")
	}

	second, err := m.Setup(ctx, "bx_98765432")
	if err != nil {
		t.Fatalf("Setup second: %v", err)
	}
	defer second.Close()
	if second.GuestIP != netip.MustParseAddr("10.42.0.3") || second.SSHPort != 30001 {
		t.Errorf("second allocation = %s:%d", second.GuestIP, second.SSHPort)
	}

	if err := m.Release(ctx, "bx_abcdefgh"); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if _, ok := m.Allocation("bx_abcdefgh"); ok {
		t.Error("released sandbox still has an allocation")
	}
	if _, ok := runner.linkKind(first.TAPName); ok {
		t.Error("released TAP link still exists")
	}
	batch := runner.lastNftBatch(t)
	if strings.Contains(batch, "10.42.0.2") {
		t.Error("released address is still in the rules")
	}
	if !strings.Contains(batch, "10.42.0.3") {
		t.Error("remaining address disappeared from the rules")
	}
	if got := netTestStateFiles(t, cfg.StateDir); !slices.Equal(got, []string{"bx_98765432.json"}) {
		t.Errorf("state files after release = %v", got)
	}
	// Release is idempotent, including for something never allocated.
	for _, id := range []string{"bx_abcdefgh", "bx_22222222"} {
		if err := m.Release(ctx, id); err != nil {
			t.Errorf("second Release(%s) = %v", id, err)
		}
	}
	// The pool is reusable: the next sandbox gets the lowest free identity.
	third, err := m.Setup(ctx, "bx_22222222")
	if err != nil {
		t.Fatalf("Setup third: %v", err)
	}
	defer third.Close()
	if third.GuestIP != netip.MustParseAddr("10.42.0.2") || third.SSHPort != 30000 {
		t.Errorf("reused allocation = %s:%d, want 10.42.0.2:30000", third.GuestIP, third.SSHPort)
	}
}

func TestSetupIsIdempotent(t *testing.T) {
	cfg := netTestConfig(t)
	m, runner, _ := netTestManager(t, cfg)
	ctx := t.Context()

	first, err := m.Setup(ctx, "bx_abcdefgh")
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	firstAlloc := first.Allocation
	if err := first.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	second, err := m.Setup(ctx, "bx_abcdefgh")
	if err != nil {
		t.Fatalf("Setup again: %v", err)
	}
	defer second.Close()
	if second.Allocation != firstAlloc {
		t.Errorf("second Setup changed the identity:\n got %+v\nwant %+v", second.Allocation, firstAlloc)
	}
	if got := netTestStateFiles(t, cfg.StateDir); len(got) != 1 {
		t.Errorf("state files = %v, want one", got)
	}
	// Adoption re-applies the rules without allocating a second address.
	if got := len(runner.nftBatches()); got != 2 {
		t.Errorf("nft batches = %d, want 2 (create and adopt)", got)
	}
	if allocs := m.Allocations(); len(allocs) != 1 {
		t.Errorf("allocations = %v, want one", allocs)
	}
}

func TestSetupPoolExhaustion(t *testing.T) {
	t.Run("addresses", func(t *testing.T) {
		cfg := netTestConfig(t)
		cfg.Prefix = netip.MustParsePrefix("10.42.0.0/30") // one usable address
		cfg.SSHPorts = PortRange{Min: 30000, Max: 30009}
		m, _, _ := netTestManager(t, cfg)

		lease, err := m.Setup(t.Context(), "bx_abcdefgh")
		if err != nil {
			t.Fatalf("Setup: %v", err)
		}
		defer lease.Close()
		if lease.GuestIP != netip.MustParseAddr("10.42.0.2") {
			t.Fatalf("guest ip = %s, want 10.42.0.2", lease.GuestIP)
		}
		if _, err := m.Setup(t.Context(), "bx_98765432"); !errors.Is(err, ErrPoolExhausted) {
			t.Fatalf("second Setup = %v, want ErrPoolExhausted", err)
		}
	})

	t.Run("ports", func(t *testing.T) {
		cfg := netTestConfig(t)
		cfg.Prefix = netip.MustParsePrefix("10.42.0.0/29")
		cfg.SSHPorts = PortRange{Min: 30000, Max: 30000}
		m, _, _ := netTestManager(t, cfg)

		lease, err := m.Setup(t.Context(), "bx_abcdefgh")
		if err != nil {
			t.Fatalf("Setup: %v", err)
		}
		defer lease.Close()
		if _, err := m.Setup(t.Context(), "bx_98765432"); !errors.Is(err, ErrPortExhausted) {
			t.Fatalf("second Setup = %v, want ErrPortExhausted", err)
		}
		// A failed port allocation must not leak the address it picked.
		if got := len(netTestStateFiles(t, cfg.StateDir)); got != 1 {
			t.Fatalf("state files = %d, want 1", got)
		}
	})
}

func TestSetupRollsBackOnRulesFailure(t *testing.T) {
	cfg := netTestConfig(t)
	m, runner, _ := netTestManager(t, cfg)
	tap := tapFor("bx_abcdefgh")

	runner.failOnce = "nft"
	runner.failErr = errors.New("nft: syntax error")

	_, err := m.Setup(t.Context(), "bx_abcdefgh")
	if err == nil {
		t.Fatal("Setup must fail when the ruleset cannot be applied")
	}
	if !strings.Contains(err.Error(), "nft") {
		t.Errorf("error = %v, want the nft failure", err)
	}
	if runner.indexOf("ip link set dev "+tap+" up") >= 0 {
		t.Error("the TAP must not be brought up when the rules failed")
	}
	if _, ok := runner.linkKind(tap); ok {
		t.Error("rollback must delete the TAP")
	}
	if got := netTestStateFiles(t, cfg.StateDir); len(got) != 0 {
		t.Errorf("rollback left state files: %v", got)
	}
	if _, ok := m.Allocation("bx_abcdefgh"); ok {
		t.Error("rollback left the allocation in memory")
	}
	// The rollback ruleset must not describe a sandbox that does not exist.
	if batch := runner.lastNftBatch(t); strings.Contains(batch, "10.42.0.2") {
		t.Error("rollback ruleset still contains the failed allocation")
	}
	// Nothing leaked: the address and port are reusable.
	lease, err := m.Setup(t.Context(), "bx_98765432")
	if err != nil {
		t.Fatalf("Setup after rollback: %v", err)
	}
	defer lease.Close()
	if lease.GuestIP != netip.MustParseAddr("10.42.0.2") || lease.SSHPort != 30000 {
		t.Errorf("allocation after rollback = %s:%d, want 10.42.0.2:30000", lease.GuestIP, lease.SSHPort)
	}
}

func TestSetupRollsBackOnCancellation(t *testing.T) {
	cfg := netTestConfig(t)
	m, runner, _ := netTestManager(t, cfg)
	tap := tapFor("bx_abcdefgh")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	runner.cancel = cancel
	runner.cancelOn = "nft -f -"

	_, err := m.Setup(ctx, "bx_abcdefgh")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Setup = %v, want context.Canceled", err)
	}
	// Cleanup runs with a detached context, so the cancellation does not skip
	// deleting the TAP or restoring the rules.
	if _, ok := runner.linkKind(tap); ok {
		t.Error("cancelled setup must delete the TAP")
	}
	if got := netTestStateFiles(t, cfg.StateDir); len(got) != 0 {
		t.Errorf("cancelled setup left state files: %v", got)
	}
	if runs := runner.nftBatches(); len(runs) != 2 {
		t.Fatalf("nft batches = %d, want the failed apply and the rollback", len(runs))
	}
	if strings.Contains(runner.lastNftBatch(t), "10.42.0.2") {
		t.Error("rollback ruleset still contains the cancelled allocation")
	}
}

func TestSetupHonoursPreCancelledContext(t *testing.T) {
	cfg := netTestConfig(t)
	m, runner, _ := netTestManager(t, cfg)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	if _, err := m.Setup(ctx, "bx_abcdefgh"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Setup = %v, want context.Canceled", err)
	}
	if got := runner.commands(); len(got) != 0 {
		t.Errorf("a pre-cancelled Setup ran commands: %v", got)
	}
	if got := netTestStateFiles(t, cfg.StateDir); len(got) != 0 {
		t.Errorf("pre-cancelled setup left state files: %v", got)
	}
}

func TestSetupRejectsInvalidSandboxID(t *testing.T) {
	m, runner, _ := netTestManager(t, netTestConfig(t))
	for _, id := range []string{"", "bx_", "sandbox-1", "bx_ABCDEFGH", "bx_abcdefghi"} {
		if _, err := m.Setup(t.Context(), id); err == nil {
			t.Errorf("Setup(%q) succeeded", id)
		}
	}
	if got := runner.commands(); len(got) != 0 {
		t.Errorf("invalid ids ran commands: %v", got)
	}
}

func TestRestartIdentity(t *testing.T) {
	cfg := netTestConfig(t)
	runner := netTestNewRunner(t)
	first, err := New(cfg, Deps{Runner: runner, Taps: netTestNewTapper(t, runner)})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := t.Context()
	leaseA, err := first.Setup(ctx, "bx_abcdefgh")
	if err != nil {
		t.Fatalf("Setup A: %v", err)
	}
	leaseB, err := first.Setup(ctx, "bx_98765432")
	if err != nil {
		t.Fatalf("Setup B: %v", err)
	}
	before := first.Allocations()
	if err := leaseA.Close(); err != nil {
		t.Fatalf("Close A: %v", err)
	}
	if err := leaseB.Close(); err != nil {
		t.Fatalf("Close B: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("Close manager: %v", err)
	}

	// A restart loads the same identities from the state directory.
	restartRunner := netTestNewRunner(t)
	tapper := netTestNewTapper(t, restartRunner)
	second, err := New(cfg, Deps{Runner: restartRunner, Taps: tapper})
	if err != nil {
		t.Fatalf("New after restart: %v", err)
	}
	defer second.Close()
	after := second.Allocations()
	if len(after) != 2 || after[0] != before[0] || after[1] != before[1] {
		t.Fatalf("restart changed allocations:\n before %+v\n after  %+v", before, after)
	}

	adopted, err := second.Setup(ctx, "bx_abcdefgh")
	if err != nil {
		t.Fatalf("adopt after restart: %v", err)
	}
	defer adopted.Close()
	if adopted.Allocation != before[0] {
		t.Errorf("adoption changed the identity: %+v vs %+v", adopted.Allocation, before[0])
	}
	// Adoption must re-open the TAP and re-apply the rules through the new
	// manager: the original runner still has the link from the first Setup, so
	// only the restart manager's own state counts.
	if _, ok := restartRunner.linkKind(adopted.TAPName); !ok {
		t.Errorf("adoption must re-create the TAP link through the restart manager; commands: %v", restartRunner.commands())
	}
	if got := tapper.seen(); !slices.Equal(got, []string{adopted.TAPName}) {
		t.Errorf("restart tapper opened %v, want the adopted tap", got)
	}
	if got := len(restartRunner.nftBatches()); got != 1 {
		t.Errorf("adoption applied %d nft batches, want 1", got)
	}
	// The next sandbox gets the next free address, not the adopted one.
	third, err := second.Setup(ctx, "bx_22222222")
	if err != nil {
		t.Fatalf("Setup third: %v", err)
	}
	defer third.Close()
	if third.GuestIP != netip.MustParseAddr("10.42.0.4") || third.SSHPort != 30002 {
		t.Errorf("third allocation = %s:%d, want 10.42.0.4:30002", third.GuestIP, third.SSHPort)
	}
}

func TestRestartRefusesConfigMismatch(t *testing.T) {
	base := netTestConfig(t)
	runner := netTestNewRunner(t)
	first, err := New(base, Deps{Runner: runner, Taps: netTestNewTapper(t, runner)})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	lease, err := first.Setup(t.Context(), "bx_abcdefgh")
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	lease.Close()
	if err := first.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	tests := []struct {
		name   string
		change func(*Config)
	}{
		{"prefix", func(c *Config) {
			c.Prefix = netip.MustParsePrefix("10.43.0.0/16")
			c.Gateway = netip.MustParseAddr("10.43.0.1")
		}},
		{"gateway", func(c *Config) { c.Gateway = netip.MustParseAddr("10.42.0.254") }},
		{"bridge", func(c *Config) { c.Bridge = "bbraft1" }},
		{"ingress removed", func(c *Config) {
			c.IngressAddr = netip.Addr{}
			c.SSHPorts = PortRange{}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := netTestConfig(t)
			cfg.StateDir = base.StateDir
			tt.change(&cfg)
			missRunner := netTestNewRunner(t)
			m, err := New(cfg, Deps{Runner: missRunner, Taps: netTestNewTapper(t, missRunner)})
			if !errors.Is(err, ErrStateMismatch) {
				if m != nil {
					m.Close()
				}
				t.Fatalf("New = %v, want ErrStateMismatch", err)
			}
		})
	}
}

func TestStateFileValidation(t *testing.T) {
	cfg := netTestConfig(t)
	runner := netTestNewRunner(t)
	m, err := New(cfg, Deps{Runner: runner, Taps: netTestNewTapper(t, runner)})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	lease, err := m.Setup(t.Context(), "bx_abcdefgh")
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	lease.Close()
	if err := m.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	path := filepath.Join(cfg.StateDir, "bx_abcdefgh.json")

	tests := []struct {
		name    string
		content string
	}{
		{"garbage", "{"},
		{"wrong mac", `{"sandboxId":"bx_abcdefgh","guestIp":"10.42.0.2","prefix":"10.42.0.0/16","gateway":"10.42.0.1","bridge":"bbraft0","mac":"zz:zz","tap":"bt01234567","sshPort":30000}`},
		{"address outside prefix", `{"sandboxId":"bx_abcdefgh","guestIp":"10.43.0.2","prefix":"10.42.0.0/16","gateway":"10.42.0.1","bridge":"bbraft0","mac":"52:54:00:aa:bb:cc","tap":"bt01234567","sshPort":30000}`},
		{"name mismatch", `{"sandboxId":"bx_98765432","guestIp":"10.42.0.2","prefix":"10.42.0.0/16","gateway":"10.42.0.1","bridge":"bbraft0","mac":"52:54:00:aa:bb:cc","tap":"bt01234567","sshPort":30000}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := os.WriteFile(path, []byte(tt.content), 0o600); err != nil {
				t.Fatalf("write state: %v", err)
			}
			m, err := New(cfg, Deps{Runner: netTestNewRunner(t)})
			if err == nil {
				m.Close()
				t.Fatal("New accepted a corrupt state file")
			}
		})
	}

	// A leftover temporary file from an interrupted write is simply removed.
	if err := os.Remove(path); err != nil {
		t.Fatalf("remove corrupt state: %v", err)
	}
	if err := os.WriteFile(filepath.Join(cfg.StateDir, tmpPrefix+"bx_abcdefgh.json"), []byte("partial"), 0o600); err != nil {
		t.Fatalf("write temp: %v", err)
	}
	m, err = New(cfg, Deps{Runner: netTestNewRunner(t)})
	if err != nil {
		t.Fatalf("New with stale temp file: %v", err)
	}
	defer m.Close()
	if got := netTestStateFiles(t, cfg.StateDir); len(got) != 0 {
		t.Errorf("state files = %v, want none", got)
	}
	if _, err := os.Stat(filepath.Join(cfg.StateDir, tmpPrefix+"bx_abcdefgh.json")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("stale temporary file was not removed: %v", err)
	}
}

func TestReconcile(t *testing.T) {
	cfg := netTestConfig(t)
	cfg.SSHPorts = PortRange{Min: 30000, Max: 30009}
	m, runner, _ := netTestManager(t, cfg)
	ctx := t.Context()

	ids := []string{"bx_abcdefgh", "bx_98765432", "bx_22222222"}
	leases := make(map[string]*Lease, len(ids))
	for _, id := range ids {
		lease, err := m.Setup(ctx, id)
		if err != nil {
			t.Fatalf("Setup(%s): %v", id, err)
		}
		leases[id] = lease
		defer lease.Close()
	}

	report, err := m.Reconcile(ctx, []string{ids[1]})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if want := []string{ids[0], ids[2]}; !slices.Equal(report.Released, want) {
		t.Errorf("released = %v, want %v", report.Released, want)
	}
	if got := netTestStateFiles(t, cfg.StateDir); !slices.Equal(got, []string{ids[1] + ".json"}) {
		t.Errorf("state files = %v", got)
	}
	batch := runner.lastNftBatch(t)
	if strings.Contains(batch, "10.42.0.2") || strings.Contains(batch, "10.42.0.4") {
		t.Error("released addresses are still in the rules")
	}
	if !strings.Contains(batch, "10.42.0.3") {
		t.Error("live address disappeared from the rules")
	}

	// Nothing to release still re-applies the rules, so a flushed table is
	// rebuilt at startup.
	batches := len(runner.nftBatches())
	report, err = m.Reconcile(ctx, ids)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if len(report.Released) != 0 {
		t.Errorf("released = %v, want none", report.Released)
	}
	if got := len(runner.nftBatches()); got != batches+1 {
		t.Errorf("nft batches = %d, want %d", got, batches+1)
	}

	// An empty live list releases everything but keeps the owned tables.
	report, err = m.Reconcile(ctx, nil)
	if err != nil {
		t.Fatalf("Reconcile all: %v", err)
	}
	if len(report.Released) != 1 {
		t.Errorf("released = %v, want the remaining sandbox", report.Released)
	}
	if got := netTestStateFiles(t, cfg.StateDir); len(got) != 0 {
		t.Errorf("state files = %v, want none", got)
	}
	if batch := runner.lastNftBatch(t); !strings.Contains(batch, "table inet "+inetTableName) {
		t.Error("rules must still be installed after releasing everything")
	}
}

// TestReleaseKeepsReservationWhenRulesFail proves release order: the TAP is
// quiesced and deleted first, but the allocation and the durable record stay
// until the rules are removed, so a failure cannot reuse the address while the
// stale TAP lives.
func TestReleaseKeepsReservationWhenRulesFail(t *testing.T) {
	cfg := netTestConfig(t)
	m, runner, _ := netTestManager(t, cfg)
	lease, err := m.Setup(t.Context(), "bx_abcdefgh")
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	defer lease.Close()

	runner.failOnce = "nft"
	runner.failErr = errors.New("nft: busy")
	before := len(runner.commands())
	if err := m.Release(t.Context(), "bx_abcdefgh"); err == nil {
		t.Fatal("Release must report a ruleset failure")
	}
	// Ordering inside this release attempt only: quiesce, delete the owned link,
	// and only then rewrite rules. Setup already issued its own down/up pairs, so
	// the commands are sliced to the attempt.
	attempt := runner.commands()[before:]
	down := slices.IndexFunc(attempt, func(cmd string) bool {
		return strings.HasPrefix(cmd, "ip link set dev "+lease.TAPName+" down")
	})
	del := slices.IndexFunc(attempt, func(cmd string) bool {
		return strings.HasPrefix(cmd, "ip link del dev "+lease.TAPName)
	})
	nft := slices.IndexFunc(attempt, func(cmd string) bool { return strings.HasPrefix(cmd, "nft ") })
	if down < 0 || del < 0 || nft < 0 || !(down < del && del < nft) {
		t.Fatalf("release attempt order must be down < del < nft, got %v", attempt)
	}
	// The address and port stay reserved: the allocation and its durable record
	// are still there even though the link is gone.
	if _, ok := m.Allocation("bx_abcdefgh"); !ok {
		t.Error("failed release dropped the reservation")
	}
	if got := netTestStateFiles(t, cfg.StateDir); len(got) != 1 {
		t.Errorf("state files = %v, want the reservation to survive", got)
	}
	next, err := m.Setup(t.Context(), "bx_98765432")
	if err != nil {
		t.Fatalf("Setup after failed release: %v", err)
	}
	defer next.Close()
	if next.GuestIP != netip.MustParseAddr("10.42.0.3") || next.SSHPort != 30001 {
		t.Errorf("address after failed release = %s:%d, want 10.42.0.3:30001 (reserved)", next.GuestIP, next.SSHPort)
	}
	// The retry completes and frees the identity.
	if err := m.Release(t.Context(), "bx_abcdefgh"); err != nil {
		t.Fatalf("retry Release: %v", err)
	}
	if _, ok := m.Allocation("bx_abcdefgh"); ok {
		t.Error("retried release kept the reservation")
	}
	if got := netTestStateFiles(t, cfg.StateDir); !slices.Equal(got, []string{"bx_98765432.json"}) {
		t.Errorf("state files after retry = %v", got)
	}
}

// TestReleaseRetriesDeleteFailure proves a failed link deletion keeps the
// reservation and can be retried.
func TestReleaseRetriesDeleteFailure(t *testing.T) {
	cfg := netTestConfig(t)
	m, runner, _ := netTestManager(t, cfg)
	lease, err := m.Setup(t.Context(), "bx_abcdefgh")
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	defer lease.Close()

	runner.failOnce = "ip link del dev " + lease.TAPName
	runner.failErr = errors.New("ip: device busy")
	before := len(runner.commands())
	if err := m.Release(t.Context(), "bx_abcdefgh"); err == nil {
		t.Fatal("Release must report a delete failure")
	}
	if _, ok := m.Allocation("bx_abcdefgh"); !ok {
		t.Error("failed delete dropped the reservation")
	}
	if got := netTestStateFiles(t, cfg.StateDir); len(got) != 1 {
		t.Errorf("state files = %v, want one", got)
	}
	// The link was not deleted and the failure path restored it. Only the
	// commands of this attempt count: Setup already issued its own up.
	if _, ok := runner.linkKind(lease.TAPName); !ok {
		t.Fatal("failed release deleted the tap")
	}
	attempt := runner.commands()[before:]
	del := slices.IndexFunc(attempt, func(cmd string) bool {
		return strings.HasPrefix(cmd, "ip link del dev "+lease.TAPName)
	})
	up := slices.IndexFunc(attempt, func(cmd string) bool {
		return strings.HasPrefix(cmd, "ip link set dev "+lease.TAPName+" up")
	})
	if del < 0 || up < 0 || up < del {
		t.Fatalf("the failed delete must be followed by a restore: %v", attempt)
	}
	if err := m.Release(t.Context(), "bx_abcdefgh"); err != nil {
		t.Fatalf("retry Release: %v", err)
	}
	if _, ok := runner.linkKind(lease.TAPName); ok {
		t.Error("retried release left the tap")
	}
	if got := netTestStateFiles(t, cfg.StateDir); len(got) != 0 {
		t.Errorf("state files after retry = %v", got)
	}
}

// TestReleaseRetriesStateRemoveFailure proves the durable record is removed
// last and that a failure keeps the allocation reserved.
func TestReleaseRetriesStateRemoveFailure(t *testing.T) {
	cfg := netTestConfig(t)
	m, runner, _ := netTestManager(t, cfg)
	lease, err := m.Setup(t.Context(), "bx_abcdefgh")
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	defer lease.Close()
	store := &netTestState{removeErr: errors.New("state: input/output error"), removeFailures: 1}
	m.state = store

	if err := m.Release(t.Context(), "bx_abcdefgh"); err == nil {
		t.Fatal("Release must report a state removal failure")
	}
	// The link is gone, but the reservation and its record survive, so the
	// address cannot be handed to another sandbox.
	if _, ok := runner.linkKind(tapFor("bx_abcdefgh")); ok {
		t.Error("release should have deleted the owned tap before the state record")
	}
	if _, ok := m.Allocation("bx_abcdefgh"); !ok {
		t.Error("failed state removal dropped the reservation")
	}
	if got := netTestStateFiles(t, cfg.StateDir); len(got) != 1 {
		t.Errorf("state files = %v, want one", got)
	}
	next, err := m.Setup(t.Context(), "bx_98765432")
	if err != nil {
		t.Fatalf("Setup after failed release: %v", err)
	}
	defer next.Close()
	if next.GuestIP == netip.MustParseAddr("10.42.0.2") {
		t.Error("the reserved address was reused while its state record survived")
	}
	store.removeErr = nil
	if err := m.Release(t.Context(), "bx_abcdefgh"); err != nil {
		t.Fatalf("retry Release: %v", err)
	}
	if got := netTestStateFiles(t, cfg.StateDir); !slices.Equal(got, []string{"bx_98765432.json"}) {
		t.Errorf("state files after retry = %v", got)
	}
}

// TestDeleteOwnedTap proves release and sweep only ever delete a link this
// package owns, identified by its alias.
func TestDeleteOwnedTap(t *testing.T) {
	cfg := netTestConfig(t)
	m, runner, _ := netTestManager(t, cfg)
	tap := tapFor("bx_abcdefgh")

	tests := []struct {
		name    string
		link    string
		kind    string
		alias   string
		setLink bool
		wantErr bool
	}{
		{name: "missing link", link: tap},
		{name: "owned ioctl tap", link: tap, kind: "tun", alias: aliasFor("bx_abcdefgh"), setLink: true},
		{name: "owned veth for the packet tests", link: tap, kind: "veth", alias: aliasFor("bx_abcdefgh"), setLink: true},
		{name: "owned tap without linkinfo", link: tap, kind: "unknown", alias: aliasFor("bx_abcdefgh"), setLink: true},
		{name: "same name without an alias", link: tap, kind: "tun", setLink: true, wantErr: true},
		{name: "same name with a foreign alias", link: tap, kind: "tun", alias: "someone-else", setLink: true, wantErr: true},
		{name: "same name with another sandbox alias", link: tap, kind: "tun", alias: aliasFor("bx_98765432"), setLink: true, wantErr: true},
		{name: "bridge with our alias is still refused", link: tap, kind: "bridge", alias: aliasFor("bx_abcdefgh"), setLink: true, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.setLink {
				runner.setLink(tt.link, tt.kind)
				if tt.alias != "" {
					runner.setAlias(tt.link, tt.alias)
				}
			}
			err := m.deleteOwnedTap(t.Context(), "bx_abcdefgh")
			if tt.wantErr {
				if err == nil {
					t.Fatal("deleteOwnedTap accepted a link it does not own")
				}
				if _, ok := runner.linkKind(tt.link); !ok {
					t.Fatal("refused link was deleted")
				}
				return
			}
			if err != nil {
				t.Fatalf("deleteOwnedTap: %v", err)
			}
			if _, ok := runner.linkKind(tt.link); ok {
				t.Errorf("link %s still exists after deleteOwnedTap", tt.link)
			}
		})
	}
}

// TestReleaseUnknownIDNeverDeletesForeignLink proves Release of an id with no
// reservation does not guess: a same-name link without our alias survives.
func TestReleaseUnknownIDNeverDeletesForeignLink(t *testing.T) {
	cfg := netTestConfig(t)
	m, runner, _ := netTestManager(t, cfg)
	tap := tapFor("bx_abcdefgh")
	runner.setLink(tap, "tun")

	if err := m.Release(t.Context(), "bx_abcdefgh"); err == nil {
		t.Fatal("Release must report the foreign same-name link")
	}
	if _, ok := runner.linkKind(tap); !ok {
		t.Fatal("release deleted a link that does not carry our ownership alias")
	}
	// A link that does carry our alias is ours to clean up.
	runner.setAlias(tap, aliasFor("bx_abcdefgh"))
	if err := m.Release(t.Context(), "bx_abcdefgh"); err != nil {
		t.Fatalf("Release of an owned orphan: %v", err)
	}
	if _, ok := runner.linkKind(tap); ok {
		t.Error("owned orphan tap survived release")
	}
}

// TestCommitFailureRollsBackDurably proves an uncertain state commit is
// resolved by removing the durable record before the reservation is reused.
func TestCommitFailureRollsBackDurably(t *testing.T) {
	for _, tt := range []struct {
		name        string
		afterRename bool
	}{
		{"before the rename", false},
		{"after the rename, directory sync failed", true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := netTestConfig(t)
			m, runner, _ := netTestManager(t, cfg)
			m.state = &netTestState{
				afterRename:   tt.afterRename,
				writeFailures: 1,
				writeErr:      errors.New("state: input/output error"),
			}
			if _, err := m.Setup(t.Context(), "bx_abcdefgh"); err == nil {
				t.Fatal("Setup must fail when the state commit fails")
			}
			// A failed commit leaves nothing behind: no record, no link, no
			// reservation, so the address is reusable.
			if got := netTestStateFiles(t, cfg.StateDir); len(got) != 0 {
				t.Errorf("state files = %v, want none", got)
			}
			if _, ok := m.Allocation("bx_abcdefgh"); ok {
				t.Error("failed commit kept the reservation")
			}
			if _, ok := runner.linkKind(tapFor("bx_abcdefgh")); ok {
				t.Error("failed commit left the tap")
			}
			lease, err := m.Setup(t.Context(), "bx_98765432")
			if err != nil {
				t.Fatalf("Setup after failed commit: %v", err)
			}
			defer lease.Close()
			if lease.GuestIP != netip.MustParseAddr("10.42.0.2") || lease.SSHPort != 30000 {
				t.Errorf("allocation after failed commit = %s:%d, want 10.42.0.2:30000", lease.GuestIP, lease.SSHPort)
			}
		})
	}
}

// TestCommitUncertainKeepsReservation proves a commit that may have succeeded
// and cannot be undone durably keeps the reservation instead of handing the
// address and port to another sandbox.
func TestCommitUncertainKeepsReservation(t *testing.T) {
	cfg := netTestConfig(t)
	m, runner, _ := netTestManager(t, cfg)
	tap := tapFor("bx_abcdefgh")
	m.state = &netTestState{
		afterRename:    true,
		writeFailures:  1,
		removeFailures: 1,
		writeErr:       errors.New("state: directory sync failed"),
		removeErr:      errors.New("state: removal failed"),
	}

	_, err := m.Setup(t.Context(), "bx_abcdefgh")
	if err == nil {
		t.Fatal("Setup must report the uncertain commit")
	}
	if !strings.Contains(err.Error(), "reservation") {
		t.Fatalf("error %q does not explain the kept reservation", err)
	}
	if _, ok := m.Allocation("bx_abcdefgh"); !ok {
		t.Fatal("uncertain commit dropped the reservation")
	}
	if _, ok := runner.linkKind(tap); !ok {
		t.Fatal("uncertain commit deleted the tap")
	}
	if got := netTestStateFiles(t, cfg.StateDir); len(got) != 1 {
		t.Fatalf("state files = %v, want the uncertain record to survive", got)
	}
	// The address and port stay reserved for that sandbox.
	next, err := m.Setup(t.Context(), "bx_98765432")
	if err != nil {
		t.Fatalf("Setup after uncertain commit: %v", err)
	}
	defer next.Close()
	if next.GuestIP != netip.MustParseAddr("10.42.0.3") || next.SSHPort != 30001 {
		t.Errorf("allocation after uncertain commit = %s:%d, want 10.42.0.3:30001", next.GuestIP, next.SSHPort)
	}

	// A retry adopts the reservation and completes the commit.
	retry, err := m.Setup(t.Context(), "bx_abcdefgh")
	if err != nil {
		t.Fatalf("retry Setup: %v", err)
	}
	defer retry.Close()
	if retry.GuestIP != netip.MustParseAddr("10.42.0.2") || retry.SSHPort != 30000 {
		t.Errorf("retry allocation = %s:%d, want 10.42.0.2:30000", retry.GuestIP, retry.SSHPort)
	}
	if got := netTestStateFiles(t, cfg.StateDir); len(got) != 2 {
		t.Errorf("state files after retry = %v, want two", got)
	}

	// A restart reads the same two identities.
	restartRunner := netTestNewRunner(t)
	restart, err := New(cfg, Deps{Runner: restartRunner, Taps: netTestNewTapper(t, restartRunner)})
	if err != nil {
		t.Fatalf("New after restart: %v", err)
	}
	defer restart.Close()
	if got := len(restart.Allocations()); got != 2 {
		t.Errorf("allocations after restart = %d, want 2", got)
	}
}

// TestSetupRefusesForeignSameNameTap proves Setup does not touch a same-name
// link without this sandbox's ownership alias, and adopts one that has it.
func TestSetupRefusesForeignSameNameTap(t *testing.T) {
	cfg := netTestConfig(t)
	m, runner, _ := netTestManager(t, cfg)
	tap := tapFor("bx_abcdefgh")

	// Unmarked: must fail closed now that TAPs are only persisted after their
	// alias exists, so an unmarked link is never our crash leftover.
	runner.setLink(tap, "tun")
	if _, err := m.Setup(t.Context(), "bx_abcdefgh"); err == nil {
		t.Fatal("Setup took over an unmarked same-name link")
	}
	if runner.indexOf("ip link set dev "+tap+" address") >= 0 {
		t.Error("Setup mutated an unmarked same-name link")
	}

	// A different alias: fail closed and leave it alone.
	runner.setAlias(tap, aliasFor("bx_98765432"))
	if _, err := m.Setup(t.Context(), "bx_abcdefgh"); err == nil {
		t.Fatal("Setup took over a link owned by another sandbox")
	}
	if got := runner.linkAlias(tap); got != aliasFor("bx_98765432") {
		t.Errorf("foreign link alias changed to %q", got)
	}

	// Our own marked link (a persistent TAP from a node restart) is adopted.
	runner.setAlias(tap, aliasFor("bx_abcdefgh"))
	lease, err := m.Setup(t.Context(), "bx_abcdefgh")
	if err != nil {
		t.Fatalf("Setup of our own leftover: %v", err)
	}
	defer lease.Close()
	if lease.GuestIP != netip.MustParseAddr("10.42.0.2") {
		t.Errorf("guest ip = %s, want 10.42.0.2", lease.GuestIP)
	}
}

// TestAdoptFailureKeepsLiveResources proves a failed re-setup never deletes an
// adopted allocation's TAP or durable record.
func TestAdoptFailureKeepsLiveResources(t *testing.T) {
	cfg := netTestConfig(t)
	m, runner, _ := netTestManager(t, cfg)
	lease, err := m.Setup(t.Context(), "bx_abcdefgh")
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	if err := lease.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	identity := lease.Allocation

	before := len(runner.commands())
	runner.failOnce = "nft"
	runner.failErr = errors.New("nft: transient")
	if _, err := m.Setup(t.Context(), "bx_abcdefgh"); err == nil {
		t.Fatal("adopt must report the ruleset failure")
	}
	attempt := runner.commands()[before:]
	if _, ok := runner.linkKind(identity.TAPName); !ok {
		t.Fatal("failed adopt deleted the live tap")
	}
	if slices.ContainsFunc(attempt, func(cmd string) bool {
		return strings.HasPrefix(cmd, "ip link del dev "+identity.TAPName)
	}) {
		t.Error("failed adopt issued a link deletion")
	}
	if !slices.Contains(attempt, "ip link set dev "+identity.TAPName+" up") {
		t.Error("failed adopt did not restore the quiesced tap")
	}
	if _, ok := m.Allocation("bx_abcdefgh"); !ok {
		t.Error("failed adopt dropped the allocation")
	}
	if got := netTestStateFiles(t, cfg.StateDir); len(got) != 1 {
		t.Errorf("state files = %v, want one", got)
	}

	again, err := m.Setup(t.Context(), "bx_abcdefgh")
	if err != nil {
		t.Fatalf("retry adopt: %v", err)
	}
	defer again.Close()
	if again.Allocation != identity {
		t.Errorf("retry changed the identity: %+v vs %+v", again.Allocation, identity)
	}
}

// TestReconcileSweepsOwnedOrphans proves crash recovery only removes links that
// carry this package's alias and have no allocation.
func TestReconcileSweepsOwnedOrphans(t *testing.T) {
	cfg := netTestConfig(t)
	m, runner, _ := netTestManager(t, cfg)
	orphan := tapFor("bx_abcdefgh")
	live := tapFor("bx_98765432")
	foreignSameName := tapFor("bx_22222222")
	other := "eth0"

	runner.setLink(orphan, "tun")
	runner.setAlias(orphan, aliasFor("bx_abcdefgh"))
	runner.setLink(live, "tun")
	runner.setAlias(live, aliasFor("bx_98765432"))
	runner.setLink(foreignSameName, "tun")
	runner.setLink(other, "unknown")

	report, err := m.Reconcile(t.Context(), []string{"bx_98765432"})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !slices.Equal(report.SweptOrphans, []string{"bx_abcdefgh"}) {
		t.Errorf("swept orphans = %v, want [bx_abcdefgh]", report.SweptOrphans)
	}
	if _, ok := runner.linkKind(orphan); ok {
		t.Error("orphan tap survived reconcile")
	}
	if _, ok := runner.linkKind(live); !ok {
		t.Error("a tap whose sandbox is live was swept")
	}
	if _, ok := runner.linkKind(foreignSameName); !ok {
		t.Error("a same-name link without our alias was deleted")
	}
	if _, ok := runner.linkKind(other); !ok {
		t.Error("an unrelated link was deleted")
	}
}

// netTestRestartManager models a node restart after allocations were persisted:
// the durable records are reloaded, and the ownership-marked TAPs that survived
// the process are seeded into the new fake host. The new host has no nftables
// state, which is the stale-table case Reconcile must repair.
func netTestRestartManager(t *testing.T, cfg Config, links ...string) (*Manager, *netTestRunner, *netTestTapper) {
	t.Helper()
	m, runner, tapper := netTestManager(t, cfg)
	for _, sandboxID := range links {
		runner.setLink(tapFor(sandboxID), "tun")
		runner.setAlias(tapFor(sandboxID), aliasFor(sandboxID))
	}
	return m, runner, tapper
}

// TestReconcileRendersAfterOrphanOnlySweep proves an orphan-only sweep still
// installs the rules of every kept allocation. A restart loads the durable
// allocations but the owned nftables tables may be gone; sweeping a crash-left
// TAP must not be mistaken for a no-op that leaves live guests unfiltered.
func TestReconcileRendersAfterOrphanOnlySweep(t *testing.T) {
	cfg := netTestConfig(t)
	live := []string{"bx_abcdefgh", "bx_98765432"}
	orphan := "bx_22222222"

	first, _, _ := netTestManager(t, cfg)
	for _, sandboxID := range live {
		lease, err := first.Setup(t.Context(), sandboxID)
		if err != nil {
			t.Fatalf("Setup(%s): %v", sandboxID, err)
		}
		lease.Close()
	}
	// Capture the durable truth before the restart. Both the reload and the
	// post-reconcile set must describe exactly these allocations: an empty
	// allocation set would otherwise match an empty-emitted oracle.
	saved := first.Allocations()
	if err := first.Close(); err != nil {
		t.Fatalf("Close first manager: %v", err)
	}

	m, runner, tapper := netTestRestartManager(t, cfg, append(slices.Clone(live), orphan)...)
	if got := m.Allocations(); !slices.Equal(got, saved) {
		t.Fatalf("reload changed the durable allocations: got %+v, want %+v", got, saved)
	}
	report, err := m.Reconcile(t.Context(), live)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if len(report.Released) != 0 {
		t.Errorf("released = %v, want none", report.Released)
	}
	if !slices.Equal(report.SweptOrphans, []string{orphan}) {
		t.Errorf("swept orphans = %v, want [%s]", report.SweptOrphans, orphan)
	}
	if got := m.Allocations(); !slices.Equal(got, saved) {
		t.Errorf("reconcile changed the kept allocations: got %+v, want %+v", got, saved)
	}
	if _, ok := runner.linkKind(tapFor(orphan)); ok {
		t.Error("orphan tap survived reconcile")
	}
	for _, sandboxID := range live {
		if _, ok := runner.linkKind(tapFor(sandboxID)); !ok {
			t.Errorf("live tap for %s was swept", sandboxID)
		}
		if got := runner.linkAlias(tapFor(sandboxID)); got != aliasFor(sandboxID) {
			t.Errorf("live tap for %s lost its ownership alias: %q", sandboxID, got)
		}
	}
	// Reconciliation never reopens a retained TAP: another descriptor on it
	// would fail EBUSY for the running QEMU, and the live guest keeps its own.
	if got := tapper.seen(); len(got) != 0 {
		t.Errorf("reconcile reopened taps %v, want none", got)
	}

	batches := runner.nftBatches()
	if len(batches) != 1 {
		t.Fatalf("orphan-only sweep applied %d nft batches, want exactly 1", len(batches))
	}
	// The oracle is the pre-restart durable set, never m.Allocations() after the
	// operation: that would let a wrongly emptied set produce an empty ruleset
	// that matches itself.
	want, err := Ruleset(cfg, saved)
	if err != nil {
		t.Fatalf("Ruleset: %v", err)
	}
	if batches[0] != want {
		t.Errorf("reconcile rules do not describe exactly the kept allocations:\n--- got ---\n%s\n--- want ---\n%s", batches[0], want)
	}
}

// TestReconcileOrphanSweepRendersAgainOnRetry proves a failed render is
// reported, not hidden behind a successful sweep, and that a retry installs the
// kept allocation rules.
func TestReconcileOrphanSweepRendersAgainOnRetry(t *testing.T) {
	cfg := netTestConfig(t)
	live := []string{"bx_abcdefgh"}
	orphan := "bx_98765432"

	first, _, _ := netTestManager(t, cfg)
	lease, err := first.Setup(t.Context(), live[0])
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	lease.Close()
	saved := first.Allocations()
	if err := first.Close(); err != nil {
		t.Fatalf("Close first manager: %v", err)
	}

	m, runner, _ := netTestRestartManager(t, cfg, append(slices.Clone(live), orphan)...)
	injected := errors.New("nft: injected render failure")
	runner.failOnce = "nft"
	runner.failErr = injected
	report, err := m.Reconcile(t.Context(), live)
	if !errors.Is(err, injected) {
		t.Fatalf("Reconcile error = %v, want the injected nft failure", err)
	}
	if runner.failOnce != "" {
		t.Error("the injected render failure was not consumed: Reconcile never attempted the nft render")
	}
	if got := len(runner.nftBatches()); got != 1 {
		t.Errorf("nft batches after the failed render = %d, want 1 (the attempted render)", got)
	}
	if !slices.Equal(report.SweptOrphans, []string{orphan}) {
		t.Errorf("swept orphans on the failed attempt = %v, want [%s]", report.SweptOrphans, orphan)
	}

	report, err = m.Reconcile(t.Context(), live)
	if err != nil {
		t.Fatalf("retry Reconcile: %v", err)
	}
	if len(report.SweptOrphans) != 0 {
		t.Errorf("retry swept %v, want none", report.SweptOrphans)
	}
	if got := m.Allocations(); !slices.Equal(got, saved) {
		t.Errorf("retry changed the kept allocations: got %+v, want %+v", got, saved)
	}
	want, err := Ruleset(cfg, saved)
	if err != nil {
		t.Fatalf("Ruleset: %v", err)
	}
	if got := len(runner.nftBatches()); got != 2 {
		t.Errorf("nft batches after the retry = %d, want 2 (failed attempt + retry)", got)
	}
	if got := runner.lastNftBatch(t); got != want {
		t.Errorf("retry did not re-render the kept allocations:\n--- got ---\n%s\n--- want ---\n%s", got, want)
	}
}

// TestConcurrentCheckHostAndClose keeps the documented "safe for concurrent
// use" promise honest under -race.
func TestConcurrentCheckHostAndClose(t *testing.T) {
	netTestHostPoints(t)
	cfg := netTestConfig(t)
	m, runner, _ := netTestManager(t, cfg)
	runner.out["nft --version"] = netTestOutput{data: []byte("nftables v1.1.7\n")}
	runner.out["ip -Version"] = netTestOutput{data: []byte("ip utility\n")}
	runner.out["ip -j -4 addr show"] = netTestOutput{data: fmt.Appendf(nil,
		`[{"ifname":"eth0","addr_info":[{"local":"203.0.113.10"}]}]`)}
	m.readSysctl = func(context.Context, string) ([]byte, error) { return []byte("1\n"), nil }

	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			_ = m.CheckHost(t.Context())
			_ = m.Allocations()
			_, _ = m.Allocation("bx_abcdefgh")
			_, _ = m.SSHEndpoint("bx_abcdefgh")
		})
	}
	wg.Go(func() {
		_ = m.Close()
	})
	wg.Wait()
}

// TestWriteAllocationRenameFailure proves a failed commit is reported and
// leaves no temporary file behind, so the next attempt starts clean.
func TestWriteAllocationRenameFailure(t *testing.T) {
	cfg := netTestConfig(t)
	root, err := os.OpenRoot(cfg.StateDir)
	if err != nil {
		t.Fatalf("OpenRoot: %v", err)
	}
	defer root.Close()
	alloc := netTestAllocation(cfg, "bx_abcdefgh", netip.MustParseAddr("10.42.0.2"), 30000)
	// A directory with the committed name makes the rename fail.
	if err := os.Mkdir(filepath.Join(cfg.StateDir, stateName(alloc.SandboxID)), 0o700); err != nil {
		t.Fatalf("create blocking directory: %v", err)
	}
	if err := writeAllocationFile(root, alloc); err == nil {
		t.Fatal("writeAllocationFile succeeded over a directory")
	}
	entries, err := os.ReadDir(cfg.StateDir)
	if err != nil {
		t.Fatalf("read state dir: %v", err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), tmpPrefix) {
			t.Errorf("temporary file %s survived a failed commit", entry.Name())
		}
	}
}

// TestCommandErrorUnwrap proves callers can match the underlying process error.
func TestCommandErrorUnwrap(t *testing.T) {
	err := &CommandError{Name: "nft", ExitCode: 127, Err: os.ErrNotExist}
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatal("CommandError does not unwrap to the process error")
	}
	if got := exitCode(err); got != 127 {
		t.Errorf("exitCode = %d, want 127", got)
	}
	if got := exitCode(errors.New("plain")); got != -1 {
		t.Errorf("exitCode of a plain error = %d, want -1", got)
	}
	if got := stderrOf(err); got != "" {
		t.Errorf("stderrOf = %q, want empty", got)
	}
}

// TestSetupRequiresVhostDescriptor proves a TAP implementation that silently
// skips vhost-net cannot produce a lease.
func TestSetupRequiresVhostDescriptor(t *testing.T) {
	cfg := netTestConfig(t)
	runner := netTestNewRunner(t)
	m, err := New(cfg, Deps{Runner: runner, Taps: noVhostTapper{t: t, runner: runner}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer m.Close()
	_, err = m.Setup(t.Context(), "bx_abcdefgh")
	if err == nil || !strings.Contains(err.Error(), "vhost-net") {
		t.Fatalf("Setup = %v, want a missing vhost-net descriptor error", err)
	}
	if _, ok := m.Allocation("bx_abcdefgh"); ok {
		t.Error("a lease without vhost-net left a reservation")
	}
}

// noVhostTapper is a Tapper that deliberately returns no vhost descriptor.
type noVhostTapper struct {
	t      *testing.T
	runner *netTestRunner
}

func (n noVhostTapper) OpenTap(ctx context.Context, name string) (*os.File, error) {
	n.runner.setLink(name, "tun")
	return os.CreateTemp(n.t.TempDir(), "tap-")
}

func (noVhostTapper) OpenVhostNet(context.Context) (*os.File, error) { return nil, nil }

func (noVhostTapper) PersistTap(context.Context, *os.File) error { return nil }

// TestAdoptConfigureFailureRestoresTap proves a failure before the rules are
// applied on adoption leaves the live TAP up and the reservation intact.
func TestAdoptConfigureFailureRestoresTap(t *testing.T) {
	cfg := netTestConfig(t)
	m, runner, _ := netTestManager(t, cfg)
	lease, err := m.Setup(t.Context(), "bx_abcdefgh")
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	if err := lease.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	identity := lease.Allocation

	before := len(runner.commands())
	runner.failOnce = "ip link set dev " + identity.TAPName + " master"
	runner.failErr = errors.New("ip: transient")
	if _, err := m.Setup(t.Context(), "bx_abcdefgh"); err == nil {
		t.Fatal("adopt must report the configure failure")
	}
	attempt := runner.commands()[before:]
	if _, ok := runner.linkKind(identity.TAPName); !ok {
		t.Fatal("failed adopt deleted the live tap")
	}
	if !slices.Contains(attempt, "ip link set dev "+identity.TAPName+" up") {
		t.Errorf("failed adopt did not restore the quiesced tap: %v", attempt)
	}
	if _, ok := m.Allocation("bx_abcdefgh"); !ok {
		t.Error("failed adopt dropped the allocation")
	}
	if got := netTestStateFiles(t, cfg.StateDir); len(got) != 1 {
		t.Errorf("state files = %v, want one", got)
	}
	if again, err := m.Setup(t.Context(), "bx_abcdefgh"); err != nil || again.Allocation != identity {
		t.Fatalf("retry adopt = %+v, %v; want %+v", again, err, identity)
	}
}

// TestSetupRollbackHoldsReservationWhenCleanupFails proves an incomplete
// rollback (rules and link deletion both failing) keeps the address and port
// reserved and leaves a retryable release.
func TestSetupRollbackHoldsReservationWhenCleanupFails(t *testing.T) {
	cfg := netTestConfig(t)
	m, runner, _ := netTestManager(t, cfg)
	tap := tapFor("bx_abcdefgh")
	runner.failIf = func(cmd netTestCommand) error {
		text := cmd.String()
		if strings.HasPrefix(text, "nft ") || strings.HasPrefix(text, "ip link del dev "+tap) {
			return errors.New("host commands failing")
		}
		return nil
	}

	_, err := m.Setup(t.Context(), "bx_abcdefgh")
	if err == nil {
		t.Fatal("Setup must fail")
	}
	if !strings.Contains(err.Error(), "rollback") {
		t.Errorf("error %q does not report the failed rollback", err)
	}
	// The rules may still reference the TAP and the TAP may still exist, so the
	// address and port must stay reserved.
	if _, ok := m.Allocation("bx_abcdefgh"); !ok {
		t.Error("incomplete rollback dropped the reservation")
	}
	if _, ok := runner.linkKind(tap); !ok {
		t.Error("incomplete rollback reported success but the tap is gone")
	}
	// Once the host commands work again, the reserved address must still be
	// skipped and Release must finish the cleanup.
	runner.clearFaults()
	next, err := m.Setup(t.Context(), "bx_98765432")
	if err != nil {
		t.Fatalf("Setup of another sandbox: %v", err)
	}
	defer next.Close()
	if next.GuestIP != netip.MustParseAddr("10.42.0.3") || next.SSHPort != 30001 {
		t.Errorf("address after incomplete rollback = %s:%d, want 10.42.0.3:30001", next.GuestIP, next.SSHPort)
	}
	if err := m.Release(t.Context(), "bx_abcdefgh"); err != nil {
		t.Fatalf("Release after rollback failure: %v", err)
	}
	if _, ok := m.Allocation("bx_abcdefgh"); ok {
		t.Error("release kept the reservation")
	}
	if _, ok := runner.linkKind(tap); ok {
		t.Error("release left the tap")
	}
}

// TestReleaseRefusesForeignTapBeforeQuiesce proves a foreign same-name link is
// not even set down by Release.
func TestReleaseRefusesForeignTapBeforeQuiesce(t *testing.T) {
	cfg := netTestConfig(t)
	m, runner, _ := netTestManager(t, cfg)
	lease, err := m.Setup(t.Context(), "bx_abcdefgh")
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	defer lease.Close()
	runner.setAlias(lease.TAPName, aliasFor("bx_98765432"))

	before := len(runner.commands())
	if err := m.Release(t.Context(), "bx_abcdefgh"); err == nil {
		t.Fatal("Release must refuse a foreign same-name link")
	}
	attempt := runner.commands()[before:]
	for _, verb := range []string{" down", " up", " del "} {
		if slices.ContainsFunc(attempt, func(cmd string) bool {
			return strings.Contains(cmd, lease.TAPName) && strings.Contains(cmd, verb)
		}) {
			t.Errorf("Release touched a foreign link (%s): %v", strings.TrimSpace(verb), attempt)
		}
	}
	if _, ok := m.Allocation("bx_abcdefgh"); !ok {
		t.Error("refused release dropped the reservation")
	}
	if got := netTestStateFiles(t, cfg.StateDir); len(got) != 1 {
		t.Errorf("state files = %v, want one", got)
	}
	// Restoring the alias makes the release possible again.
	runner.setAlias(lease.TAPName, aliasFor("bx_abcdefgh"))
	if err := m.Release(t.Context(), "bx_abcdefgh"); err != nil {
		t.Fatalf("Release after restoring the alias: %v", err)
	}
}

// TestCheckHostProbesSupportedVersionFlags pins the exact binary probes. Ubuntu's
// iproute2 rejects "--version" with exit 255 ("Option -version is unknown"), so
// CheckHost must use ip -Version. The fake runner returns an error for any
// Output invocation it was not given, so an unsupported probe cannot be blessed
// silently: this test fails instead.
func TestCheckHostProbesSupportedVersionFlags(t *testing.T) {
	netTestHostPoints(t)
	cfg := netTestConfig(t)
	runner := netTestNewRunner(t)
	runner.out["nft --version"] = netTestOutput{data: []byte("nftables v1.1.7\n")}
	runner.out["ip -Version"] = netTestOutput{data: []byte("ip utility, iproute2-6.12.0\n")}
	runner.out["ip -j -4 addr show"] = netTestOutput{data: fmt.Appendf(nil,
		`[{"ifname":"eth0","addr_info":[{"local":"203.0.113.10"}]}]`)}
	m, err := New(cfg, Deps{Runner: runner, Taps: netTestNewTapper(t, runner)})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer m.Close()
	m.readSysctl = func(context.Context, string) ([]byte, error) { return []byte("1\n"), nil }
	if err := m.CheckHost(t.Context()); err != nil {
		t.Fatalf("CheckHost: %v", err)
	}
	probes := runner.outputCommands()
	if !slices.Contains(probes, "ip -Version") {
		t.Errorf("CheckHost did not probe ip -Version; probes: %v", probes)
	}
	if !slices.Contains(probes, "nft --version") {
		t.Errorf("CheckHost did not probe nft --version; probes: %v", probes)
	}
	if slices.Contains(probes, "ip --version") {
		t.Errorf("CheckHost used ip --version, which iproute2 does not support; probes: %v", probes)
	}
}

func TestSSHEndpoint(t *testing.T) {
	cfg := netTestConfig(t)
	m, _, _ := netTestManager(t, cfg)
	lease, err := m.Setup(t.Context(), "bx_abcdefgh")
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	defer lease.Close()

	got, ok := m.SSHEndpoint("bx_abcdefgh")
	if !ok {
		t.Fatal("SSHEndpoint returned no endpoint for an allocated sandbox")
	}
	if want := netip.MustParseAddrPort("203.0.113.10:30000"); got != want {
		t.Errorf("SSHEndpoint = %s, want %s", got, want)
	}
	if _, ok := m.SSHEndpoint("bx_98765432"); ok {
		t.Error("unknown sandbox has an SSH endpoint")
	}

	// Without ingress the sandbox has no endpoint at all.
	cfg.IngressAddr = netip.Addr{}
	cfg.SSHPorts = PortRange{}
	cfg.StateDir = t.TempDir()
	plain, _, _ := netTestManager(t, cfg)
	plainLease, err := plain.Setup(t.Context(), "bx_abcdefgh")
	if err != nil {
		t.Fatalf("Setup without ingress: %v", err)
	}
	defer plainLease.Close()
	if plainLease.SSHPort != 0 || plainLease.SSHEndpoint.IsValid() {
		t.Errorf("lease without ingress = port %d endpoint %s", plainLease.SSHPort, plainLease.SSHEndpoint)
	}
	if _, ok := plain.SSHEndpoint("bx_abcdefgh"); ok {
		t.Error("sandbox without ingress reported an SSH endpoint")
	}
}

func TestSetupPersistsIngressInterface(t *testing.T) {
	cfg := netTestConfig(t)
	cfg.IngressIface = "tailscale0"
	m, runner, _ := netTestManager(t, cfg)
	lease, err := m.Setup(t.Context(), "bx_abcdefgh")
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	defer lease.Close()
	if lease.IngressIface != "tailscale0" {
		t.Errorf("lease ingress interface = %q, want tailscale0", lease.IngressIface)
	}
	persisted, ok := m.Allocation("bx_abcdefgh")
	if !ok || persisted.IngressIface != "tailscale0" {
		t.Errorf("allocation = %+v (ok %v), want ingress interface tailscale0", persisted, ok)
	}
	batch := runner.lastNftBatch(t)
	if !strings.Contains(batch, `iifname "tailscale0" ip daddr 203.0.113.10 tcp dport 30000 dnat ip to 10.42.0.2:22`) {
		t.Errorf("applied ruleset does not bind SSH DNAT to the configured ingress interface")
	}
}

func TestConcurrentSetupDistinctSandboxes(t *testing.T) {
	cfg := netTestConfig(t)
	cfg.SSHPorts = PortRange{Min: 30000, Max: 30049}
	m, _, _ := netTestManager(t, cfg)

	const n = 16
	ids := make([]string, n)
	for i := range n {
		ids[i] = netTestSandboxID(byte(i))
	}
	var wg sync.WaitGroup
	leases := make([]*Lease, n)
	errs := make([]error, n)
	for i := range n {
		wg.Go(func() {
			leases[i], errs[i] = m.Setup(t.Context(), ids[i])
		})
	}
	wg.Wait()

	addresses := make(map[netip.Addr]string, n)
	ports := make(map[uint16]string, n)
	for i := range n {
		if errs[i] != nil {
			t.Fatalf("Setup(%s): %v", ids[i], errs[i])
		}
		defer leases[i].Close()
		if other, dup := addresses[leases[i].GuestIP]; dup {
			t.Errorf("%s and %s share address %s", ids[i], other, leases[i].GuestIP)
		}
		addresses[leases[i].GuestIP] = ids[i]
		if other, dup := ports[leases[i].SSHPort]; dup {
			t.Errorf("%s and %s share port %d", ids[i], other, leases[i].SSHPort)
		}
		ports[leases[i].SSHPort] = ids[i]
	}
	if len(m.Allocations()) != n {
		t.Errorf("allocations = %d, want %d", len(m.Allocations()), n)
	}
	if got := netTestStateFiles(t, cfg.StateDir); len(got) != n {
		t.Errorf("state files = %d, want %d", len(got), n)
	}
}

// netTestSandboxID builds a valid id from a byte without using the random
// generator, so the test is reproducible.
func netTestSandboxID(b byte) string {
	const alphabet = "23456789abcdefghjkmnpqrstuvwxyz"
	var out [8]byte
	for i := range out {
		out[i] = alphabet[(int(b)+i*7)%len(alphabet)]
	}
	return "bx_" + string(out[:])
}

func TestCloseThenUse(t *testing.T) {
	m, _, _ := netTestManager(t, netTestConfig(t))
	if err := m.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := m.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if _, err := m.Setup(t.Context(), "bx_abcdefgh"); !errors.Is(err, ErrClosed) {
		t.Errorf("Setup after Close = %v, want ErrClosed", err)
	}
	if err := m.Release(t.Context(), "bx_abcdefgh"); !errors.Is(err, ErrClosed) {
		t.Errorf("Release after Close = %v, want ErrClosed", err)
	}
	if _, err := m.Reconcile(t.Context(), nil); !errors.Is(err, ErrClosed) {
		t.Errorf("Reconcile after Close = %v, want ErrClosed", err)
	}
	if err := m.CheckHost(t.Context()); !errors.Is(err, ErrClosed) {
		t.Errorf("CheckHost after Close = %v, want ErrClosed", err)
	}
}

func TestAllocationsAreCopies(t *testing.T) {
	cfg := netTestConfig(t)
	m, _, tapper := netTestManager(t, cfg)
	lease, err := m.Setup(t.Context(), "bx_abcdefgh")
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	defer lease.Close()
	if err := lease.Close(); err != nil {
		t.Fatalf("close lease: %v", err)
	}
	if got := len(tapper.seen()); got != 1 {
		t.Errorf("tapper opened %d taps, want 1", got)
	}
	if lease.Tap != nil || lease.Vhost != nil {
		t.Error("lease Close must release its descriptors")
	}

	allocs := m.Allocations()
	allocs[0].GuestIP = netip.MustParseAddr("10.42.0.99")
	if again := m.Allocations(); again[0].GuestIP == netip.MustParseAddr("10.42.0.99") {
		t.Error("Allocations returned a view into manager state")
	}
	if a, ok := m.Allocation("bx_abcdefgh"); !ok || a.GuestIP != netip.MustParseAddr("10.42.0.2") {
		t.Errorf("Allocation = %+v (%v)", a, ok)
	}
}

func TestSetupRecordsCreationTime(t *testing.T) {
	cfg := netTestConfig(t)
	runner := netTestNewRunner(t)
	m, err := New(cfg, Deps{Runner: runner, Taps: netTestNewTapper(t, runner)})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer m.Close()
	fixed := time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)
	m.now = func() time.Time { return fixed }
	lease, err := m.Setup(t.Context(), "bx_abcdefgh")
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	defer lease.Close()
	if !lease.CreatedAt.Equal(fixed) {
		t.Errorf("CreatedAt = %s, want %s", lease.CreatedAt, fixed)
	}
}

func TestSetupFailsClosedWhenTapIsUnavailable(t *testing.T) {
	cfg := netTestConfig(t)
	runner := netTestNewRunner(t)
	tapper := netTestNewTapper(t, runner)
	tapper.tapErr = errors.New("operation not permitted")
	m, err := New(cfg, Deps{Runner: runner, Taps: tapper})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer m.Close()
	if _, err := m.Setup(t.Context(), "bx_abcdefgh"); !errors.Is(err, tapper.tapErr) {
		t.Fatalf("Setup = %v, want the TAP error", err)
	}
	if got := netTestStateFiles(t, cfg.StateDir); len(got) != 0 {
		t.Errorf("state files = %v, want none", got)
	}
}
