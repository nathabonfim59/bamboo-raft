package node

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"fmt"
	"net/netip"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/ids"
	nodenet "github.com/nathabonfim59/bamboo-raft/internal/node/net"
	"github.com/nathabonfim59/bamboo-raft/internal/node/qemu"
	"github.com/nathabonfim59/bamboo-raft/internal/nodetest"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/guestv1"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// sshTestGuest is a scriptable guest for the SSH proxy tests. Only Ping (boot)
// and AddSSHKey are implemented; every reply is staged per test.
type sshTestGuest struct {
	guestv1.UnimplementedGuestServer

	mu       sync.Mutex
	add      func(context.Context, *guestv1.AddSSHKeyRequest) (*guestv1.AddSSHKeyResponse, error)
	calls    int
	requests []*guestv1.AddSSHKeyRequest
}

func (g *sshTestGuest) Ping(context.Context, *guestv1.PingRequest) (*guestv1.PingResponse, error) {
	return &guestv1.PingResponse{Version: "ssh-test"}, nil
}

func (g *sshTestGuest) AddSSHKey(ctx context.Context, req *guestv1.AddSSHKeyRequest) (*guestv1.AddSSHKeyResponse, error) {
	g.mu.Lock()
	g.calls++
	g.requests = append(g.requests, req)
	add := g.add
	g.mu.Unlock()
	if add == nil {
		return nil, status.Error(codes.Unimplemented, "AddSSHKey")
	}
	return add(ctx, req)
}

func (g *sshTestGuest) callCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.calls
}

// sshTestNetwork is an isolated VMNetwork fixture. It keeps the endpoint map
// tests stage, and its allocation methods are deliberately complete enough for
// the forthcoming lifecycle wiring (valid lease descriptors, per-sandbox
// addresses, Release and Reconcile) so those calls never touch a host.
type sshTestNetwork struct {
	mu        sync.Mutex
	endpoints map[string]netip.AddrPort
	allocated map[string]bool
	calls     map[string]int
	next      int
	// onCall runs after the call counter is updated and before SSHEndpoint
	// returns; tests use it to block the node between reading an endpoint and
	// verifying the boot generation.
	onCall func(id string, call int)
}

func newSSHTestNetwork() *sshTestNetwork {
	return &sshTestNetwork{
		endpoints: map[string]netip.AddrPort{},
		allocated: map[string]bool{},
		calls:     map[string]int{},
	}
}

// allow publishes one endpoint for id, as a completed Setup would.
func (n *sshTestNetwork) allow(id string, endpoint netip.AddrPort) {
	n.mu.Lock()
	n.endpoints[id] = endpoint
	n.allocated[id] = true
	n.mu.Unlock()
}

func (n *sshTestNetwork) Setup(_ context.Context, id string) (*nodenet.Lease, error) {
	n.mu.Lock()
	n.next++
	// The gateway is the first host address; guests start at .2 so the fake
	// allocation is launch-ready (a guest address may never equal the gateway
	// or the network address).
	host := byte(n.next + 1)
	endpoint, ok := n.endpoints[id]
	if !ok {
		endpoint = netip.AddrPortFrom(netip.MustParseAddr("203.0.113.10"), uint16(2200+n.next))
		n.endpoints[id] = endpoint
	}
	n.allocated[id] = true
	n.mu.Unlock()
	tap, err := os.Open(os.DevNull)
	if err != nil {
		return nil, fmt.Errorf("ssh test network: open tap fixture: %w", err)
	}
	vhost, err := os.Open(os.DevNull)
	if err != nil {
		tap.Close()
		return nil, fmt.Errorf("ssh test network: open vhost fixture: %w", err)
	}
	return &nodenet.Lease{
		Allocation: nodenet.Allocation{
			SandboxID:   id,
			GuestIP:     netip.AddrFrom4([4]byte{10, 200, 0, host}),
			Prefix:      netip.MustParsePrefix("10.200.0.0/24"),
			Gateway:     netip.MustParseAddr("10.200.0.1"),
			Bridge:      "bbraft0",
			MAC:         fmt.Sprintf("52:54:00:00:00:%02x", host),
			TAPName:     fmt.Sprintf("btsh%d", host),
			IngressAddr: endpoint.Addr(),
			SSHPort:     endpoint.Port(),
			CreatedAt:   time.Now(),
		},
		SSHEndpoint: endpoint,
		Tap:         tap,
		Vhost:       vhost,
	}, nil
}

func (n *sshTestNetwork) Release(_ context.Context, id string) error {
	n.mu.Lock()
	delete(n.endpoints, id)
	delete(n.allocated, id)
	n.mu.Unlock()
	return nil
}

func (n *sshTestNetwork) SSHEndpoint(id string) (netip.AddrPort, bool) {
	n.mu.Lock()
	n.calls[id]++
	call := n.calls[id]
	hook := n.onCall
	n.mu.Unlock()
	// The hook runs before the allocation is read, so a test can stage a
	// restart or release between the caller asking and the network answering.
	if hook != nil {
		hook(id, call)
	}
	n.mu.Lock()
	endpoint, ok := n.endpoints[id]
	if !n.allocated[id] {
		ok = false
	}
	n.mu.Unlock()
	return endpoint, ok
}

func (n *sshTestNetwork) Allocations() []nodenet.Allocation {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make([]nodenet.Allocation, 0, len(n.allocated))
	for id := range n.allocated {
		out = append(out, nodenet.Allocation{SandboxID: id})
	}
	slices.SortFunc(out, func(a, b nodenet.Allocation) int {
		return strings.Compare(a.SandboxID, b.SandboxID)
	})
	return out
}

func (n *sshTestNetwork) Reconcile(_ context.Context, live []string) (nodenet.ReconcileReport, error) {
	keep := make(map[string]struct{}, len(live))
	for _, id := range live {
		keep[id] = struct{}{}
	}
	n.mu.Lock()
	var released []string
	for id := range n.allocated {
		if _, ok := keep[id]; !ok {
			released = append(released, id)
			delete(n.allocated, id)
			delete(n.endpoints, id)
		}
	}
	n.mu.Unlock()
	slices.Sort(released)
	return nodenet.ReconcileReport{Released: released}, nil
}

// TestSSHTestNetworkLeaseIsLaunchReady keeps the fixture honest for the
// forthcoming lifecycle wiring: its lease and allocation must satisfy the
// QEMU NIC contract, not only this package's endpoint read.
func TestSSHTestNetworkLeaseIsLaunchReady(t *testing.T) {
	n := newSSHTestNetwork()
	lease, err := n.Setup(t.Context(), ids.Sandbox())
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	t.Cleanup(func() {
		if err := lease.Close(); err != nil {
			t.Errorf("lease.Close: %v", err)
		}
	})
	network := &qemu.Network{
		GuestIP:   lease.GuestIP,
		PrefixLen: lease.Prefix.Bits(),
		Gateway:   lease.Gateway,
		MAC:       lease.MAC,
		Tap:       lease.Tap,
		Vhost:     lease.Vhost,
	}
	if err := network.Validate(); err != nil {
		t.Fatalf("qemu network from fake lease: %v", err)
	}
}

// sshTestHarness builds a normal manager harness with network injected. A nil
// network keeps the dev user-mode default, which the proxy must reject.
func sshTestHarness(t *testing.T, network VMNetwork) *harness {
	t.Helper()
	return newHarness(t, harnessOptions{configure: func(cfg *Config) {
		if network != nil {
			cfg.Network = network
		}
	}})
}

// sshTestBareKeyLine strips the generated comment from a public key helper, so
// tests stage exactly the one bare line the frozen contract expects.
func sshTestBareKeyLine(t *testing.T, key string) string {
	t.Helper()
	fields := strings.Fields(key)
	if len(fields) < 2 {
		t.Fatalf("test key %q is not an OpenSSH public key", key)
	}
	return fields[0] + " " + fields[1]
}

// sshTestCanonicalHostKey is the canonical wire form AddSSHKey must publish for
// key: the marshalled key without the trailing newline.
func sshTestCanonicalHostKey(t *testing.T, key string) string {
	t.Helper()
	parsed, _, _, _, err := ssh.ParseAuthorizedKey([]byte(sshTestBareKeyLine(t, key)))
	if err != nil {
		t.Fatalf("parse test host key: %v", err)
	}
	return strings.TrimSuffix(string(ssh.MarshalAuthorizedKey(parsed)), "\n")
}

func TestAddSSHKeyProxiesKeyAndEndpoint(t *testing.T) {
	key := nodetest.TestPublicKey(t, "request")
	hostKey := nodetest.TestPublicKey(t, "bbraft-guest")
	wantHostKey := sshTestCanonicalHostKey(t, hostKey)
	var gotKey string
	guest := &sshTestGuest{add: func(_ context.Context, req *guestv1.AddSSHKeyRequest) (*guestv1.AddSSHKeyResponse, error) {
		gotKey = req.GetPublicKey()
		return &guestv1.AddSSHKeyResponse{User: "user", HostKey: hostKey}, nil
	}}
	network := newSSHTestNetwork()
	h := sshTestHarness(t, network)
	id := ids.Sandbox()
	network.allow(id, netip.MustParseAddrPort("203.0.113.9:2201"))
	useGuest(h, guest)
	h.create(id, nil)
	client := startNodeClient(t, h)

	acc, err := client.AddSSHKey(t.Context(), &nodev1.AddSSHKeyRequest{SandboxId: id, PublicKey: key})
	if err != nil {
		t.Fatalf("AddSSHKey: %v", err)
	}
	if gotKey != key {
		t.Fatalf("guest saw key %q, want the request bytes unchanged", gotKey)
	}
	if acc.GetUser() != "user" || acc.GetHost() != "203.0.113.9" || acc.GetPort() != 2201 {
		t.Fatalf("AddSSHKey access = %v", acc)
	}
	if acc.GetHostKey() != wantHostKey {
		t.Fatalf("host key = %q, want canonical %q", acc.GetHostKey(), wantHostKey)
	}
	if strings.ContainsAny(acc.GetHostKey(), "\r\n\x00") {
		t.Fatalf("host key is not one line: %q", acc.GetHostKey())
	}
}

// TestAddSSHKeyStateBeforePayload proves the frozen state and endpoint order:
// sandbox state is resolved first, then ingress availability, and only then
// does the guest see (and validate) the key.
func TestAddSSHKeyStateBeforePayload(t *testing.T) {
	t.Run("unknown sandbox is NotFound before the guest", func(t *testing.T) {
		guest := &sshTestGuest{}
		network := newSSHTestNetwork()
		h := sshTestHarness(t, network)
		useGuest(h, guest)
		id := ids.Sandbox()
		h.create(id, nil)
		client := startNodeClient(t, h)

		_, err := client.AddSSHKey(t.Context(), &nodev1.AddSSHKeyRequest{
			SandboxId: "bx_unknown0", PublicKey: "not a key",
		})
		wantSSHKeyCode(t, err, codes.NotFound)
		if guest.callCount() != 0 {
			t.Fatalf("guest was called %d times for an unknown sandbox", guest.callCount())
		}
	})

	t.Run("stopped sandbox is FailedPrecondition before ingress", func(t *testing.T) {
		guest := &sshTestGuest{}
		// No network: a dev user-mode node would be Unavailable for ingress,
		// but the sandbox state must win because it is resolved first.
		h := sshTestHarness(t, nil)
		id := ids.Sandbox()
		useGuest(h, guest)
		h.create(id, nil)
		if _, err := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1}); err != nil {
			t.Fatalf("StopVM: %v", err)
		}
		client := startNodeClient(t, h)

		_, err := client.AddSSHKey(t.Context(), &nodev1.AddSSHKeyRequest{SandboxId: id, PublicKey: "not a key"})
		wantSSHKeyCode(t, err, codes.FailedPrecondition)
		if guest.callCount() != 0 {
			t.Fatalf("guest was called %d times for a stopped sandbox", guest.callCount())
		}
	})

	t.Run("dev user-mode is Unavailable before payload validation", func(t *testing.T) {
		guest := &sshTestGuest{}
		h := sshTestHarness(t, nil)
		id := ids.Sandbox()
		useGuest(h, guest)
		h.create(id, nil)
		client := startNodeClient(t, h)

		_, err := client.AddSSHKey(t.Context(), &nodev1.AddSSHKeyRequest{SandboxId: id, PublicKey: "not a key"})
		wantSSHKeyCode(t, err, codes.Unavailable)
		if guest.callCount() != 0 {
			t.Fatalf("guest was called %d times without SSH ingress", guest.callCount())
		}
	})

	t.Run("missing allocation is Unavailable before payload validation", func(t *testing.T) {
		guest := &sshTestGuest{}
		network := newSSHTestNetwork()
		h := sshTestHarness(t, network)
		id := ids.Sandbox()
		useGuest(h, guest)
		h.create(id, nil) // no endpoint staged for this sandbox
		// Once the lifecycle auto-Setups connections, create leaves an
		// allocation behind; remove it explicitly to exercise the missing
		// allocation branch rather than relying on Setup never running.
		if err := network.Release(t.Context(), id); err != nil {
			t.Fatalf("Release: %v", err)
		}
		client := startNodeClient(t, h)

		_, err := client.AddSSHKey(t.Context(), &nodev1.AddSSHKeyRequest{SandboxId: id, PublicKey: "not a key"})
		wantSSHKeyCode(t, err, codes.Unavailable)
		if guest.callCount() != 0 {
			t.Fatalf("guest was called %d times without an allocation", guest.callCount())
		}
	})

	t.Run("deleting wins over a non-READY state", func(t *testing.T) {
		guest := &sshTestGuest{}
		h := sshTestHarness(t, nil)
		id := ids.Sandbox()
		useGuest(h, guest)
		h.create(id, nil)
		v, err := h.mg.lookup(id)
		if err != nil {
			t.Fatal(err)
		}
		v.mu.Lock()
		v.state = nodev1.VMState_VM_STATE_STOPPED
		v.deleting = true
		v.mu.Unlock()
		t.Cleanup(func() {
			v.mu.Lock()
			v.deleting = false
			v.state = nodev1.VMState_VM_STATE_READY
			v.mu.Unlock()
		})
		client := startNodeClient(t, h)

		// Deleting is checked before the state, so this is NOT_FOUND rather
		// than the STOPPED sandbox's FAILED_PRECONDITION.
		_, err = client.AddSSHKey(t.Context(), &nodev1.AddSSHKeyRequest{SandboxId: id, PublicKey: "not a key"})
		wantSSHKeyCode(t, err, codes.NotFound)
		if guest.callCount() != 0 {
			t.Fatalf("guest was called %d times for a deleting sandbox", guest.callCount())
		}
	})
}

// TestAddSSHKeyPassesGuestErrorsThrough keeps the guest's own fixed status and
// message untouched: the node is a proxy, not a second validator.
func TestAddSSHKeyPassesGuestErrorsThrough(t *testing.T) {
	cases := []struct {
		name string
		code codes.Code
		msg  string
	}{
		{"invalid key", codes.InvalidArgument, "public_key is not one OpenSSH public key line"},
		{"host key not ready", codes.Unavailable, "guest SSH host key is not ready"},
		{"authorized keys too large", codes.OutOfRange, "authorized_keys would exceed the limit"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			guest := &sshTestGuest{add: func(context.Context, *guestv1.AddSSHKeyRequest) (*guestv1.AddSSHKeyResponse, error) {
				return nil, status.Error(tc.code, tc.msg)
			}}
			network := newSSHTestNetwork()
			h := sshTestHarness(t, network)
			id := ids.Sandbox()
			network.allow(id, netip.MustParseAddrPort("203.0.113.9:2201"))
			useGuest(h, guest)
			h.create(id, nil)
			client := startNodeClient(t, h)

			_, err := client.AddSSHKey(t.Context(), &nodev1.AddSSHKeyRequest{
				SandboxId: id, PublicKey: nodetest.TestPublicKey(t, tc.name),
			})
			wantSSHKeyCode(t, err, tc.code)
			if got := status.Convert(err).Message(); got != tc.msg {
				t.Fatalf("message = %q, want the guest's %q", got, tc.msg)
			}
		})
	}
}

// TestAddSSHKeyValidatesGuestReply checks the untrusted guest half of
// SSHAccess. Every malformed reply is INTERNAL with a fixed message; no byte
// of the reply may appear in it.
func TestAddSSHKeyValidatesGuestReply(t *testing.T) {
	ed := nodetest.TestPublicKey(t, "host")
	bare := sshTestBareKeyLine(t, ed)
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	rsaLine := strings.TrimSpace(string(ssh.MarshalAuthorizedKey(mustSSHPublicKey(t, &rsaKey.PublicKey))))
	const marker = "UNTRUSTEDMARKER"

	cases := []struct {
		name string
		resp *guestv1.AddSSHKeyResponse
	}{
		{"empty reply", nil},
		{"wrong user", &guestv1.AddSSHKeyResponse{User: "root", HostKey: bare}},
		{"empty user", &guestv1.AddSSHKeyResponse{HostKey: bare}},
		{"user with stray space", &guestv1.AddSSHKeyResponse{User: "user ", HostKey: bare}},
		{"empty host key", &guestv1.AddSSHKeyResponse{User: "user"}},
		{"garbage host key", &guestv1.AddSSHKeyResponse{User: "user", HostKey: marker}},
		{"newline in host key", &guestv1.AddSSHKeyResponse{User: "user", HostKey: bare + "\n" + marker}},
		{"CR in host key", &guestv1.AddSSHKeyResponse{User: "user", HostKey: bare[:10] + "\r" + bare[10:]}},
		{"NUL in host key", &guestv1.AddSSHKeyResponse{User: "user", HostKey: bare[:10] + "\x00" + bare[10:]}},
		{"options on host key", &guestv1.AddSSHKeyResponse{User: "user", HostKey: `command="` + marker + `" ` + bare}},
		{"rsa host key", &guestv1.AddSSHKeyResponse{User: "user", HostKey: rsaLine}},
		{"oversized host key", &guestv1.AddSSHKeyResponse{User: "user", HostKey: marker + strings.Repeat("x", maxSSHHostKeyBytes)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			guest := &sshTestGuest{add: func(context.Context, *guestv1.AddSSHKeyRequest) (*guestv1.AddSSHKeyResponse, error) {
				return tc.resp, nil
			}}
			network := newSSHTestNetwork()
			h := sshTestHarness(t, network)
			id := ids.Sandbox()
			network.allow(id, netip.MustParseAddrPort("203.0.113.9:2201"))
			useGuest(h, guest)
			h.create(id, nil)
			client := startNodeClient(t, h)

			_, err := client.AddSSHKey(t.Context(), &nodev1.AddSSHKeyRequest{
				SandboxId: id, PublicKey: nodetest.TestPublicKey(t, tc.name),
			})
			wantSSHKeyCode(t, err, codes.Internal)
			if msg := status.Convert(err).Message(); strings.Contains(msg, marker) {
				t.Fatalf("error message echoed untrusted bytes: %q", msg)
			}
		})
	}
}

// TestAddSSHKeyRejectsUnusableEndpoint proves a misconfigured allocation never
// becomes a fabricated SSH host and port.
func TestAddSSHKeyRejectsUnusableEndpoint(t *testing.T) {
	cases := []struct {
		name     string
		endpoint netip.AddrPort
	}{
		{"zero endpoint", netip.AddrPort{}},
		{"zero port", netip.MustParseAddrPort("203.0.113.9:0")},
		{"unspecified IPv4", netip.MustParseAddrPort("0.0.0.0:22")},
		{"unspecified IPv6", netip.MustParseAddrPort("[::]:22")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			network := newSSHTestNetwork()
			h := sshTestHarness(t, network)
			id := ids.Sandbox()
			network.allow(id, tc.endpoint)
			hostKey := nodetest.TestPublicKey(t, "host")
			requestKey := nodetest.TestPublicKey(t, "request")
			useGuest(h, &sshTestGuest{add: func(context.Context, *guestv1.AddSSHKeyRequest) (*guestv1.AddSSHKeyResponse, error) {
				return &guestv1.AddSSHKeyResponse{User: "user", HostKey: hostKey}, nil
			}})
			h.create(id, nil)
			client := startNodeClient(t, h)

			_, err := client.AddSSHKey(t.Context(), &nodev1.AddSSHKeyRequest{
				SandboxId: id, PublicKey: requestKey,
			})
			wantSSHKeyCode(t, err, codes.Internal)
			msg := status.Convert(err).Message()
			if strings.Contains(msg, tc.endpoint.Addr().String()) && tc.endpoint.Addr().IsValid() {
				t.Fatalf("error message echoed the unusable endpoint %q", msg)
			}
		})
	}
}

// TestAddSSHKeyRejectsStaleReplyAfterRestart stages the guest reply, blocks the
// node on the post-reply endpoint read, and then stops and starts the VM. The
// old boot's host key must never be reported with the new boot's endpoint.
func TestAddSSHKeyRejectsStaleReplyAfterRestart(t *testing.T) {
	oldHostKey := nodetest.TestPublicKey(t, "old-boot")
	requestKey := nodetest.TestPublicKey(t, "request")
	guest := &sshTestGuest{add: func(context.Context, *guestv1.AddSSHKeyRequest) (*guestv1.AddSSHKeyResponse, error) {
		return &guestv1.AddSSHKeyResponse{User: "user", HostKey: oldHostKey}, nil
	}}
	network := newSSHTestNetwork()
	h := sshTestHarness(t, network)
	id := ids.Sandbox()
	network.allow(id, netip.MustParseAddrPort("203.0.113.9:2201"))
	entered := make(chan struct{})
	release := make(chan struct{})
	unblock := sshKeyStageCleanup(t, release)
	network.onCall = func(_ string, call int) {
		if call != 2 {
			return
		}
		close(entered)
		<-release
	}
	useGuest(h, guest)
	h.create(id, nil)
	client := startNodeClient(t, h)

	type result struct {
		acc *nodev1.SSHAccess
		err error
	}
	done := make(chan result, 1)
	go func() {
		acc, err := client.AddSSHKey(t.Context(), &nodev1.AddSSHKeyRequest{
			SandboxId: id, PublicKey: requestKey,
		})
		done <- result{acc: acc, err: err}
	}()
	waitSSHKeyStage(t, entered)
	if _, err := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1}); err != nil {
		t.Fatalf("StopVM: %v", err)
	}
	if _, err := h.mg.StartVM(t.Context(), &nodev1.StartVMRequest{SandboxId: id}); err != nil {
		t.Fatalf("StartVM: %v", err)
	}
	network.allow(id, netip.MustParseAddrPort("203.0.113.9:3301"))
	unblock()
	got := waitSSHKeyResult(t, done)
	if got.err == nil {
		t.Fatalf("AddSSHKey paired a stale host key with the restarted endpoint: %v", got.acc)
	}
	wantSSHKeyCode(t, got.err, codes.Unavailable)
}

// TestAddSSHKeyRejectsStaleReplyAfterDelete is the delete half of the same
// race: a removed sandbox must not report an endpoint at all. The endpoint is
// restored after DeleteVM so the test fails on the missing generation guard
// even once the real lifecycle wires Release into DeleteVM.
func TestAddSSHKeyRejectsStaleReplyAfterDelete(t *testing.T) {
	oldHostKey := nodetest.TestPublicKey(t, "old-boot")
	requestKey := nodetest.TestPublicKey(t, "request")
	guest := &sshTestGuest{add: func(context.Context, *guestv1.AddSSHKeyRequest) (*guestv1.AddSSHKeyResponse, error) {
		return &guestv1.AddSSHKeyResponse{User: "user", HostKey: oldHostKey}, nil
	}}
	network := newSSHTestNetwork()
	h := sshTestHarness(t, network)
	id := ids.Sandbox()
	endpoint := netip.MustParseAddrPort("203.0.113.9:2201")
	network.allow(id, endpoint)
	entered := make(chan struct{})
	release := make(chan struct{})
	unblock := sshKeyStageCleanup(t, release)
	network.onCall = func(_ string, call int) {
		if call != 2 {
			return
		}
		close(entered)
		<-release
	}
	useGuest(h, guest)
	h.create(id, nil)
	client := startNodeClient(t, h)

	done := make(chan error, 1)
	go func() {
		_, err := client.AddSSHKey(t.Context(), &nodev1.AddSSHKeyRequest{
			SandboxId: id, PublicKey: requestKey,
		})
		done <- err
	}()
	waitSSHKeyStage(t, entered)
	if _, err := h.mg.DeleteVM(t.Context(), &nodev1.DeleteVMRequest{SandboxId: id}); err != nil {
		t.Fatalf("DeleteVM: %v", err)
	}
	// Keep the staged allocation present: DeleteVM's future Release must not
	// be what makes this test pass.
	network.allow(id, endpoint)
	unblock()
	err := waitSSHKeyResult(t, done)
	if err == nil {
		t.Fatal("AddSSHKey succeeded for a sandbox deleted during the guest call")
	}
	wantSSHKeyCode(t, err, codes.Unavailable)
}

// sshKeyStageCleanup registers an idempotent unblock for a staged race and
// returns it. It is installed before the RPC goroutine starts, so a failing
// assertion can never leave that goroutine parked forever.
func sshKeyStageCleanup(t *testing.T, release chan struct{}) func() {
	t.Helper()
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unblock)
	return unblock
}

// waitSSHKeyStage waits for a staged race point with a bound, so a broken test
// fails instead of hanging the package.
func waitSSHKeyStage(t *testing.T, entered <-chan struct{}) {
	t.Helper()
	select {
	case <-entered:
	case <-time.After(30 * time.Second):
		t.Fatal("staged AddSSHKey never reached the endpoint read")
	}
}

// waitSSHKeyResult joins a staged RPC goroutine with the same bound, so a
// missing reply is a clean failure rather than a package timeout.
func waitSSHKeyResult[T any](t *testing.T, done <-chan T) T {
	t.Helper()
	select {
	case v := <-done:
		return v
	case <-time.After(30 * time.Second):
		t.Fatal("staged AddSSHKey did not return")
		var zero T
		return zero
	}
}

func mustSSHPublicKey(t *testing.T, key any) ssh.PublicKey {
	t.Helper()
	pub, err := ssh.NewPublicKey(key)
	if err != nil {
		t.Fatalf("ssh.NewPublicKey: %v", err)
	}
	return pub
}

func wantSSHKeyCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if got := status.Code(err); got != want {
		t.Fatalf("status = %s (%v), want %s", got, err, want)
	}
}
