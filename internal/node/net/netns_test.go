package nodenet

// This file is the privileged integration test. It proves, with real packets in
// real network namespaces, the isolation this package claims in its rules, and
// gives every denial a positive control so a blackhole cannot pass a test for
// the wrong reason:
//
//	BBRAFT_TEST_VM_SETUP=1 sh internal/node/net/test-setup.sh   (once, throwaway VM)
//	BBRAFT_TEST_NET=1 go test ./internal/node/net/ -run 'TestNetnsIsolation|TestRealTapDevice' -v
//
// It must run as root and only inside a throwaway VM (with sudo), never on a
// workstation or a node. The setup script only installs packages, loads
// tun/vhost modules and grants ACLs on the devices; it never configures the
// VM's networking or firewall. Every object this test
// creates lives in throwaway network namespaces (node, vm1, vm2 and an
// "internet" side); deleting the namespaces removes the links, addresses and
// nftables tables, cleanup is registered before the first namespace is created,
// and the test fails if a namespace survives.
//
// The "tap" side of each guest link is a veth end inside the node namespace, so
// the manager's ip and nftables path is exactly the production one while the
// other end (the guest) generates packets without a QEMU VM. The real
// /dev/net/tun ioctl path, including IFF_VNET_HDR, is covered by
// TestRealTapDevice.

import (
	"bytes"
	"cmp"
	"context"
	"encoding/binary"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"net/netip"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

const (
	// netGateEnv enables the privileged tests. Never set it outside a
	// throwaway VM.
	netGateEnv = "BBRAFT_TEST_NET"
	// netRequireAllEnv turns a missing prerequisite into a failure, the same
	// convention as BBRAFT_TEST_REQUIRE_ALL for the other KVM suites.
	netRequireAllEnv = "BBRAFT_TEST_REQUIRE_ALL"
	// netProbeEnv and netProbeResultEnv configure the re-executed probe child.
	netProbeEnv       = "BBRAFT_TEST_NET_PROBE"
	netProbeResultEnv = "BBRAFT_TEST_NET_RESULT"
	// netProbeDefaultTimeout bounds a single dial probe. It does not change the
	// rules; it only decides how long "dropped" is observed.
	netProbeDefaultTimeout = 800 * time.Millisecond

	// Guest-range addresses used by the test topology.
	testVM1IP   = "10.42.0.2"
	testVM2IP   = "10.42.0.3"
	testSpoofIP = "10.42.0.99"
	testIngress = "203.0.113.1"
	testUplink  = "203.0.113.2"
	testTailnet = "100.64.0.7"

	// A second external interface reaches the node without carrying the ingress
	// address, so a frame can be delivered to the node's MAC there while its IP
	// destination still matches the configured ingress address.
	testWrongIface  = "wrong0"
	testWrongNode   = "198.51.100.1"
	testWrongClient = "198.51.100.2"

	// Explicit source ports keep each policy phase in a distinct conntrack
	// entry, so a flow from the no-bind control cannot mask the restored-policy
	// result.
	testWrongPortBound    = "41001"
	testWrongPortControl  = "41002"
	testWrongPortRestored = "41003"
	testWrongPortFinal    = "41004"

	// Positive-control services behind the policy: one live listener per
	// destination class the policy must deny.
	testPrivateTarget = "10.0.0.5"
	testMgmtTarget    = "192.0.2.10"
	testCGNATTarget   = "100.64.0.1"
	testBroadcastAddr = "10.42.255.255"
	testBroadcastPort = 5353

	// IPv6 test topology. The guest subnet and the uplink subnet are distinct
	// /64s: the node's bridge is the guest's gateway and the uplink is a
	// separate segment, so every route is unambiguous and the "in" namespace
	// needs exactly one explicit return route to the guest subnet via the node.
	testGuestV6Prefix  = "fd00:42::/64"
	testBridgeV6Addr   = "fd00:42::1"
	testGuestV6Addr    = "fd00:42::99"
	testUplinkV6Prefix = "fd00:99::/64"
	testNodeUplinkV6   = "fd00:99::a1"
	testInUplinkV6     = "fd00:99::a2"
	testV6AppAddr      = "[fd00:99::a2]:8081"
)

// netProbeSpec describes one probe child action.
type netProbeSpec struct {
	Action    string `json:"action"`
	Network   string `json:"network,omitempty"`
	Address   string `json:"address,omitempty"`
	LocalAddr string `json:"localAddr,omitempty"`
	Reply     string `json:"reply,omitempty"`
	ReadyFile string `json:"readyFile,omitempty"`
	Path      string `json:"path,omitempty"`
	Value     string `json:"value,omitempty"`
	TimeoutMS int    `json:"timeoutMs,omitempty"`
	Broadcast bool   `json:"broadcast,omitempty"`
	// ARP spoof fields.
	Iface     string `json:"iface,omitempty"`
	SenderIP  string `json:"senderIp,omitempty"`
	SenderMAC string `json:"senderMac,omitempty"`
	TargetIP  string `json:"targetIp,omitempty"`
	TargetMAC string `json:"targetMac,omitempty"`
}

// netProbeResult is what a probe child reports.
type netProbeResult struct {
	OK    bool   `json:"ok"`
	Class string `json:"class,omitempty"`
	Err   string `json:"err,omitempty"`
	Peer  string `json:"peer,omitempty"`
	Reply string `json:"reply,omitempty"`
}

func netProbeClass(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, os.ErrDeadlineExceeded), os.IsTimeout(err):
		return "timeout"
	case errors.Is(err, syscall.ECONNREFUSED):
		return "refused"
	case errors.Is(err, syscall.ENETUNREACH), errors.Is(err, syscall.EHOSTUNREACH), errors.Is(err, syscall.ENETDOWN):
		return "unreachable"
	case errors.Is(err, syscall.EACCES), errors.Is(err, syscall.EPERM):
		return "denied"
	default:
		return "other"
	}
}

func netProbeTimeout(spec netProbeSpec) time.Duration {
	if spec.TimeoutMS <= 0 {
		return netProbeDefaultTimeout
	}
	return time.Duration(spec.TimeoutMS) * time.Millisecond
}

// TestNetProbeHelper is the child process the integration test starts inside a
// network namespace. It is inert unless the parent sets the probe environment.
func TestNetProbeHelper(t *testing.T) {
	raw := os.Getenv(netProbeEnv)
	if raw == "" {
		t.Skip("probe helper: only run as a child of the privileged network tests")
	}
	var spec netProbeSpec
	if err := json.Unmarshal([]byte(raw), &spec); err != nil {
		t.Fatalf("decode probe spec: %v", err)
	}
	resultPath := os.Getenv(netProbeResultEnv)
	switch spec.Action {
	case "write-file":
		err := os.WriteFile(spec.Path, []byte(spec.Value), 0o600)
		netProbeWriteResult(t, resultPath, netProbeResult{OK: err == nil, Err: netProbeErrText(err), Class: netProbeClass(err)})
	case "read-file":
		data, err := os.ReadFile(spec.Path)
		netProbeWriteResult(t, resultPath, netProbeResult{
			OK:    err == nil,
			Reply: strings.TrimSpace(string(data)),
			Err:   netProbeErrText(err),
		})
	case "dial-tcp", "dial-udp":
		netProbeWriteResult(t, resultPath, netProbeDial(spec))
	case "listen-tcp":
		netProbeListenTCP(t, spec)
	case "listen-udp":
		netProbeListenUDP(t, spec)
	case "open-tap":
		netProbeOpenTap(t, spec)
	case "arp-reply":
		netProbeWriteResult(t, resultPath, netProbeARPReply(spec))
	default:
		t.Fatalf("unknown probe action %q", spec.Action)
	}
}

func netProbeErrText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func netProbeWriteResult(t *testing.T, path string, result netProbeResult) {
	t.Helper()
	data, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("encode probe result: %v", err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write probe result: %v", err)
	}
}

func netProbeAppendResult(t *testing.T, path string, result netProbeResult) {
	t.Helper()
	data, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("encode probe record: %v", err)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatalf("open probe record: %v", err)
	}
	defer f.Close()
	if _, err := f.Write(append(data, '\n')); err != nil {
		t.Fatalf("append probe record: %v", err)
	}
}

// netProbeReady publishes the ready marker atomically: the parent polls for it,
// and a zero-length file created by a plain write would race the write itself.
func netProbeReady(t *testing.T, spec netProbeSpec, bound string) {
	t.Helper()
	if spec.ReadyFile == "" {
		return
	}
	tmp := spec.ReadyFile + ".tmp"
	if err := os.WriteFile(tmp, []byte(bound), 0o600); err != nil {
		t.Fatalf("write ready file: %v", err)
	}
	if err := os.Rename(tmp, spec.ReadyFile); err != nil {
		t.Fatalf("publish ready file: %v", err)
	}
}

func netProbeDial(spec netProbeSpec) netProbeResult {
	timeout := netProbeTimeout(spec)
	dialer := net.Dialer{Timeout: timeout}
	if spec.LocalAddr != "" {
		// The source may be a bare IP (the common case: "spoof from .99") or an
		// explicit IP:port. A bare IP becomes an ephemeral-port TCP/UDP address;
		// ResolveTCPAddr would reject it with "missing port in address".
		ip, port, err := netProbeLocalAddr(spec.LocalAddr)
		if err != nil {
			return netProbeResult{Err: err.Error(), Class: "other"}
		}
		switch spec.Network {
		case "tcp4", "tcp6":
			dialer.LocalAddr = &net.TCPAddr{IP: ip, Port: port}
		case "udp4", "udp6":
			dialer.LocalAddr = &net.UDPAddr{IP: ip, Port: port}
		default:
			return netProbeResult{Err: "a local address needs a tcp/udp network", Class: "other"}
		}
	}
	if spec.Broadcast {
		return netProbeBroadcast(spec)
	}
	conn, err := dialer.Dial(spec.Network, spec.Address)
	if err != nil {
		return netProbeResult{Class: netProbeClass(err), Err: err.Error()}
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		return netProbeResult{Err: err.Error(), Class: "other"}
	}
	if _, err := conn.Write([]byte("x")); err != nil {
		return netProbeResult{Class: netProbeClass(err), Err: err.Error()}
	}
	buf := make([]byte, 8)
	n, err := conn.Read(buf)
	if err != nil {
		return netProbeResult{Class: netProbeClass(err), Err: err.Error()}
	}
	return netProbeResult{OK: true, Reply: string(buf[:n])}
}

// netProbeBroadcast sends from an unconnected UDP socket. A connected socket to
// a broadcast address only accepts packets whose source is that broadcast
// address, which filters out the receiver's ordinary unicast reply; an
// unconnected PacketConn reports the real responder, so the caller can validate
// that the intended receiver answered.
func netProbeBroadcast(spec netProbeSpec) netProbeResult {
	timeout := netProbeTimeout(spec)
	local := spec.LocalAddr
	switch {
	case local == "":
		local = "0.0.0.0:0"
	case !strings.Contains(local, ":"):
		local += ":0"
	}
	pc, err := net.ListenPacket("udp4", local)
	if err != nil {
		return netProbeResult{Err: err.Error(), Class: "other"}
	}
	defer pc.Close()
	udpConn, ok := pc.(*net.UDPConn)
	if !ok {
		return netProbeResult{Err: "not a UDP connection", Class: "other"}
	}
	raw, err := udpConn.SyscallConn()
	if err != nil {
		return netProbeResult{Err: err.Error(), Class: "other"}
	}
	var sockErr error
	if err := raw.Control(func(fd uintptr) {
		sockErr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_BROADCAST, 1)
	}); err != nil {
		return netProbeResult{Err: err.Error(), Class: "other"}
	}
	if sockErr != nil {
		return netProbeResult{Err: sockErr.Error(), Class: "other"}
	}
	dst, err := net.ResolveUDPAddr("udp4", spec.Address)
	if err != nil {
		return netProbeResult{Err: err.Error(), Class: "other"}
	}
	if err := pc.SetDeadline(time.Now().Add(timeout)); err != nil {
		return netProbeResult{Err: err.Error(), Class: "other"}
	}
	if _, err := pc.WriteTo([]byte("x"), dst); err != nil {
		return netProbeResult{Class: netProbeClass(err), Err: err.Error()}
	}
	buf := make([]byte, 16)
	n, from, err := pc.ReadFrom(buf)
	if err != nil {
		return netProbeResult{Class: netProbeClass(err), Err: err.Error()}
	}
	return netProbeResult{OK: true, Reply: string(buf[:n]), Peer: from.String()}
}

// netProbeLocalAddr parses a local source specification: a bare IP (port 0,
// ephemeral) or IP:port.
func netProbeLocalAddr(s string) (net.IP, int, error) {
	if ip := net.ParseIP(s); ip != nil {
		return ip, 0, nil
	}
	host, portText, err := net.SplitHostPort(s)
	if err != nil {
		return nil, 0, fmt.Errorf("local address %q: %w", s, err)
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return nil, 0, fmt.Errorf("local address %q has no IP", s)
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 0 || port > 65535 {
		return nil, 0, fmt.Errorf("local address %q has an invalid port", s)
	}
	return ip, port, nil
}

func netProbeListenTCP(t *testing.T, spec netProbeSpec) {
	t.Helper()
	ln, err := net.Listen(cmp.Or(spec.Network, "tcp4"), spec.Address)
	if err != nil {
		t.Fatalf("listen %s: %v", spec.Address, err)
	}
	defer ln.Close()
	time.AfterFunc(netProbeTimeout(spec)+30*time.Second, func() { ln.Close() })
	netProbeReady(t, spec, ln.Addr().String())
	reply := spec.Reply
	if reply == "" {
		reply = "y"
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		peer := conn.RemoteAddr().String()
		buf := make([]byte, 8)
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		_, _ = conn.Read(buf)
		netProbeAppendResult(t, os.Getenv(netProbeResultEnv), netProbeResult{OK: true, Peer: peer})
		_, _ = conn.Write([]byte(reply))
		conn.Close()
	}
}

func netProbeListenUDP(t *testing.T, spec netProbeSpec) {
	t.Helper()
	pc, err := net.ListenPacket(cmp.Or(spec.Network, "udp4"), spec.Address)
	if err != nil {
		t.Fatalf("listen %s: %v", spec.Address, err)
	}
	defer pc.Close()
	time.AfterFunc(netProbeTimeout(spec)+30*time.Second, func() { pc.Close() })
	netProbeReady(t, spec, pc.LocalAddr().String())
	reply := spec.Reply
	if reply == "" {
		reply = "y"
	}
	buf := make([]byte, 64)
	for {
		_, addr, err := pc.ReadFrom(buf)
		if err != nil {
			return
		}
		netProbeAppendResult(t, os.Getenv(netProbeResultEnv), netProbeResult{OK: true, Peer: addr.String()})
		_, _ = pc.WriteTo([]byte(reply), addr)
	}
}

// netProbeOpenTap opens a real TAP inside the namespace, reports the interface
// flags it actually got (IFF_VNET_HDR is the one QEMU and vhost-net need) and
// holds the descriptor until the parent kills the process, the way QEMU would.
// The parent asks for persistence with SIGUSR1, which runs TUNSETPERSIST on this
// same descriptor: a second TUNSETIFF on an attached non-multiqueue TAP fails
// with EBUSY, so the descriptor must not be duplicated.
func netProbeOpenTap(t *testing.T, spec netProbeSpec) {
	t.Helper()
	tap, err := LinuxTaps{}.OpenTap(t.Context(), spec.Address)
	if err != nil {
		netProbeAppendResult(t, os.Getenv(netProbeResultEnv), netProbeResult{Err: err.Error(), Class: "other"})
		return
	}
	defer tap.Close()
	flags, err := tapFlags(int(tap.Fd()))
	if err != nil {
		netProbeAppendResult(t, os.Getenv(netProbeResultEnv), netProbeResult{Err: err.Error(), Class: "other"})
		return
	}
	if spec.ReadyFile == "" {
		netProbeAppendResult(t, os.Getenv(netProbeResultEnv), netProbeResult{Err: "open-tap needs a ready file", Class: "other"})
		return
	}
	// Register the persistence signal handler before the parent can see the
	// ready file, so a prompt SIGUSR1 is never delivered with the default action.
	persist := make(chan os.Signal, 1)
	signal.Notify(persist, syscall.SIGUSR1)
	defer signal.Stop(persist)
	// The result must exist before the ready file, so the parent can read it as
	// soon as the probe is considered ready.
	netProbeAppendResult(t, os.Getenv(netProbeResultEnv), netProbeResult{OK: true, Reply: strconv.Itoa(int(flags))})
	if err := os.WriteFile(spec.ReadyFile, []byte(spec.Address), 0o600); err != nil {
		t.Fatalf("write ready file: %v", err)
	}

	deadline := time.After(2 * time.Minute)
	for {
		select {
		case <-persist:
			if err := (LinuxTaps{}).PersistTap(t.Context(), tap); err != nil {
				netProbeAppendResult(t, os.Getenv(netProbeResultEnv), netProbeResult{Err: err.Error(), Class: "other"})
				continue
			}
			netProbeAppendResult(t, os.Getenv(netProbeResultEnv), netProbeResult{OK: true, Reply: "persisted"})
		case <-deadline:
			// The bound keeps a leaked child from living forever; the parent
			// always kills it during cleanup.
			return
		}
	}
}

// netProbeARPReply sends a forged ARP reply from the guest interface, claiming
// another IPv4 address. It is the source of the ARP-spoof denial and its
// positive control.
func netProbeARPReply(spec netProbeSpec) netProbeResult {
	senderMAC, err := net.ParseMAC(spec.SenderMAC)
	if err != nil {
		return netProbeResult{Err: "sender mac: " + err.Error(), Class: "other"}
	}
	targetMAC, err := net.ParseMAC(spec.TargetMAC)
	if err != nil {
		return netProbeResult{Err: "target mac: " + err.Error(), Class: "other"}
	}
	senderIP, err := netip.ParseAddr(spec.SenderIP)
	if err != nil {
		return netProbeResult{Err: "sender ip: " + err.Error(), Class: "other"}
	}
	targetIP, err := netip.ParseAddr(spec.TargetIP)
	if err != nil {
		return netProbeResult{Err: "target ip: " + err.Error(), Class: "other"}
	}
	iface, err := net.InterfaceByName(spec.Iface)
	if err != nil {
		return netProbeResult{Err: "interface: " + err.Error(), Class: "other"}
	}
	frame := make([]byte, 14+28)
	copy(frame[0:6], targetMAC)
	copy(frame[6:12], senderMAC)
	binary.BigEndian.PutUint16(frame[12:14], 0x0806) // ARP
	binary.BigEndian.PutUint16(frame[14:16], 1)      // Ethernet
	binary.BigEndian.PutUint16(frame[16:18], 0x0800) // IPv4
	frame[18], frame[19] = 6, 4                      // hlen, plen
	binary.BigEndian.PutUint16(frame[20:22], 2)      // reply
	copy(frame[22:28], senderMAC)
	s4 := senderIP.As4()
	copy(frame[28:32], s4[:])
	copy(frame[32:38], targetMAC)
	t4 := targetIP.As4()
	copy(frame[38:42], t4[:])

	fd, err := syscall.Socket(syscall.AF_PACKET, syscall.SOCK_RAW, int(htons(syscall.ETH_P_ARP)))
	if err != nil {
		return netProbeResult{Err: "socket: " + err.Error(), Class: "other"}
	}
	defer syscall.Close(fd)
	addr := &syscall.SockaddrLinklayer{Ifindex: iface.Index, Halen: 6}
	copy(addr.Addr[:], targetMAC)
	if err := syscall.Sendto(fd, frame, 0, addr); err != nil {
		return netProbeResult{Err: "send: " + err.Error(), Class: "other"}
	}
	return netProbeResult{OK: true}
}

func htons(v uint16) uint16 { return v<<8 | v>>8 }

// TestNetnsIsolation is the privileged end-to-end packet test.
func TestNetnsIsolation(t *testing.T) {
	requirePrivilegedNetTest(t)
	base := fmt.Sprintf("bbraft-net-%d", os.Getpid())
	nodeNS, vm1NS, vm2NS, inNS, wrongNS := base+"-node", base+"-vm1", base+"-vm2", base+"-in", base+"-wrong"
	namespaces := []string{nodeNS, vm1NS, vm2NS, inNS, wrongNS}
	// Register cleanup before creating anything, so a failure in the middle of
	// the setup cannot leak a namespace.
	t.Cleanup(func() { netnsCleanup(t, namespaces) })
	for _, ns := range namespaces {
		netnsAdd(t, ns)
	}

	// Internet side: one veth to the node, a tailnet client address, plus fake
	// public DNS servers and live services on every denied destination class.
	netnsIP(t, nodeNS, "link", "add", "up0", "type", "veth", "peer", "name", "up1")
	netnsIP(t, nodeNS, "addr", "add", testIngress+"/24", "dev", "up0")
	netnsIP(t, nodeNS, "link", "set", "up0", "up")
	netnsIP(t, nodeNS, "link", "set", "up1", "netns", inNS)
	netnsIP(t, inNS, "link", "set", "lo", "up")
	netnsIP(t, inNS, "addr", "add", testUplink+"/24", "dev", "up1")
	netnsIP(t, inNS, "addr", "add", "8.8.8.8/32", "dev", "lo")
	netnsIP(t, inNS, "addr", "add", testTailnet+"/32", "dev", "up1")
	for _, target := range []string{testPrivateTarget, testMgmtTarget, testCGNATTarget} {
		netnsIP(t, inNS, "addr", "add", target+"/32", "dev", "lo")
	}
	netnsIP(t, inNS, "link", "set", "up1", "up")
	// Replies to guest addresses need a route back; the node routes them to the
	// bridge itself.
	netnsIP(t, inNS, "route", "add", "10.42.0.0/16", "via", testIngress, "dev", "up1")
	netnsIP(t, nodeNS, "route", "add", "100.64.0.0/10", "via", testUplink, "dev", "up0")
	netnsIP(t, nodeNS, "route", "add", "default", "via", testUplink, "dev", "up0")

	// Wrong external path: a client on another segment routes the ingress
	// address to the node's wrong0 address. The node still owns that address on
	// up0, so the frame arrives on wrong0 with the ingress address as its IP
	// destination; only the ingress-interface binding can reject it.
	netnsIP(t, nodeNS, "link", "add", testWrongIface, "type", "veth", "peer", "name", "wrong1")
	netnsIP(t, nodeNS, "addr", "add", testWrongNode+"/24", "dev", testWrongIface)
	netnsIP(t, nodeNS, "link", "set", testWrongIface, "up")
	netnsIP(t, nodeNS, "link", "set", "wrong1", "netns", wrongNS)
	netnsIP(t, wrongNS, "link", "set", "lo", "up")
	netnsIP(t, wrongNS, "addr", "add", testWrongClient+"/24", "dev", "wrong1")
	netnsIP(t, wrongNS, "link", "set", "wrong1", "up")
	netnsIP(t, wrongNS, "route", "add", testIngress+"/32", "via", testWrongNode, "dev", "wrong1")
	netProbeRunOf(t, nodeNS, netProbeSpec{Action: "write-file", Path: "/proc/sys/net/ipv4/ip_forward", Value: "1"})
	v6Enabled := netProbeRunOf(t, nodeNS, netProbeSpec{
		Action: "write-file", Path: "/proc/sys/net/ipv6/conf/all/forwarding", Value: "1",
	}).OK
	if !v6Enabled {
		// IPv6 coverage is required when the suite is explicitly required; a
		// normal run still exercises every other rule.
		if os.Getenv(netRequireAllEnv) == "1" {
			t.Fatalf("required by %s: IPv6 forwarding is unavailable in the test namespace", netRequireAllEnv)
		}
		t.Log("IPv6 forwarding is unavailable in the test namespace; the IPv6 bypass probe will be skipped")
	}

	// The real hardening ruleset, rendered with this test's bridge listed. The
	// role template is owned by infra/ansible and is never edited here.
	template := renderHardeningTemplate(t, hardeningVars{VMBridgeInterfaces: []string{"bbraft0"}})
	if !strings.Contains(template, `meta nfproto ipv4 iifname "bbraft0" accept`) {
		t.Fatal("hardening template does not accept forwarded traffic on the VM bridge; the lead-owned template change is required")
	}
	netnsNft(t, nodeNS, template)
	hardeningBefore := netnsNftList(t, nodeNS, "inet", "bbraft_filter")

	cfg := Config{
		Bridge:       "bbraft0",
		Prefix:       netip.MustParsePrefix("10.42.0.0/16"),
		Gateway:      netip.MustParseAddr("10.42.0.1"),
		Resolvers:    []netip.Addr{netip.MustParseAddr(testUplink)},
		Management:   []netip.Prefix{netip.MustParsePrefix("192.0.2.0/24")},
		IngressAddr:  netip.MustParseAddr(testIngress),
		IngressIface: "up0",
		SSHPorts:     PortRange{Min: 30000, Max: 30010},
		StateDir:     t.TempDir(),
	}
	tapper := &netnsTapper{
		t:      t,
		nodeNS: nodeNS,
		peers:  make(map[string]string),
		targets: map[string]string{
			tapFor("bx_abcdefgh"): vm1NS,
			tapFor("bx_98765432"): vm2NS,
		},
	}
	m, err := New(cfg, Deps{Runner: netnsRunner{ns: nodeNS}, Taps: tapper})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer m.Close()
	// The sysctl check must read the node namespace, not the outer VM.
	m.readSysctl = func(ctx context.Context, name string) ([]byte, error) {
		result := netProbeRunOf(t, nodeNS, netProbeSpec{Action: "read-file", Path: "/proc/sys/" + name})
		if !result.OK {
			return nil, errors.New(result.Err)
		}
		return []byte(result.Reply + "\n"), nil
	}

	// Validate the exact batch with the real nft before the manager applies it.
	preview, err := Ruleset(cfg, []Allocation{
		netTestAllocation(cfg, "bx_abcdefgh", netip.MustParseAddr(testVM1IP), 30000),
		netTestAllocation(cfg, "bx_98765432", netip.MustParseAddr(testVM2IP), 30001),
	})
	if err != nil {
		t.Fatalf("Ruleset: %v", err)
	}
	netnsNftCheck(t, nodeNS, preview)

	if netTestDevicesPrerequisite(t) {
		if err := m.CheckHost(t.Context()); err != nil {
			t.Fatalf("CheckHost: %v", err)
		}
	}
	vm1, err := m.Setup(t.Context(), "bx_abcdefgh")
	if err != nil {
		t.Fatalf("Setup vm1: %v", err)
	}
	defer vm1.Close()
	vm2, err := m.Setup(t.Context(), "bx_98765432")
	if err != nil {
		t.Fatalf("Setup vm2: %v", err)
	}
	defer vm2.Close()
	if vm1.GuestIP != netip.MustParseAddr(testVM1IP) || vm2.GuestIP != netip.MustParseAddr(testVM2IP) {
		t.Fatalf("unexpected allocation: %s, %s", vm1.GuestIP, vm2.GuestIP)
	}

	// The manager owns both MACs: the guest MAC (lease.MAC) is what QEMU puts on
	// virtio-net and what the bridge source filter accepts, and the host TAP end
	// carries a distinct locally administered address. The test uses the manager
	// result as-is; it never overrides the host side.
	iface1, iface2 := tapper.peer(vm1.TAPName), tapper.peer(vm2.TAPName)
	for _, vm := range []*Lease{vm1, vm2} {
		hostMAC := hostMACForAddr(vm.GuestIP)
		if hostMAC == vm.MAC {
			t.Fatalf("host TAP MAC for %s equals the guest MAC %s", vm.GuestIP, vm.MAC)
		}
		link := netnsIP(t, nodeNS, "-j", "link", "show", "dev", vm.TAPName)
		if !strings.Contains(link, `"address":"`+hostMAC+`"`) {
			t.Errorf("host TAP %s MAC is not %s: %s", vm.TAPName, hostMAC, link)
		}
	}
	netnsConfigureGuest(t, vm1NS, iface1, vm1)
	netnsConfigureGuest(t, vm2NS, iface2, vm2)
	netnsIP(t, vm1NS, "addr", "add", testSpoofIP+"/32", "dev", iface1)
	// The bridge source filters must still key on the guest MAC, not the host MAC.
	rules := netnsNftList(t, nodeNS, "bridge", bridgeTableName)
	for _, vm := range []*Lease{vm1, vm2} {
		if !strings.Contains(rules, `iifname "`+vm.TAPName+`" ether saddr != `+vm.MAC+` drop`) {
			t.Errorf("bridge rules do not filter %s on the guest MAC %s", vm.TAPName, vm.MAC)
		}
	}

	// Live listeners behind the policy: one per denied destination class, plus
	// the guests' SSH, the public app, DNS servers, the node's own address and a
	// broadcast receiver.
	vm1SSH := netProbeStart(t, vm1NS, netProbeSpec{Action: "listen-tcp", Address: ":22", Reply: "y"})
	vm2SSH := netProbeStart(t, vm2NS, netProbeSpec{Action: "listen-tcp", Address: ":22", Reply: "y"})
	publicApp := netProbeStart(t, inNS, netProbeSpec{Action: "listen-tcp", Address: testUplink + ":8080", Reply: "y"})
	netProbeStart(t, inNS, netProbeSpec{Action: "listen-udp", Address: testUplink + ":53", Reply: "y"})
	netProbeStart(t, inNS, netProbeSpec{Action: "listen-udp", Address: "8.8.8.8:53", Reply: "y"})
	for _, target := range []string{testPrivateTarget, testMgmtTarget, testCGNATTarget} {
		netProbeStart(t, inNS, netProbeSpec{Action: "listen-tcp", Address: target + ":22", Reply: "y"})
	}
	netProbeStart(t, nodeNS, netProbeSpec{Action: "listen-tcp", Address: "10.42.0.1:22", Reply: "y"})
	broadcastReceiver := netProbeStart(t, vm2NS, netProbeSpec{Action: "listen-udp", Address: ":5353", Reply: "y"})
	var v6App *netProbe
	if v6Enabled {
		// Distinct guest and uplink /64s plus nodad: a freshly added address is
		// tentative during duplicate address detection, and binding a listener to
		// it fails with EADDRNOTAVAIL until DAD completes.
		netnsIP(t, nodeNS, "addr", "add", testBridgeV6Addr+"/64", "dev", "bbraft0", "nodad")
		netnsIP(t, nodeNS, "addr", "add", testNodeUplinkV6+"/64", "dev", "up0", "nodad")
		netnsIP(t, inNS, "addr", "add", testInUplinkV6+"/64", "dev", "up1", "nodad")
		// The "in" namespace reaches the guest subnet through the node's uplink
		// address; without this the control replies would be unroutable.
		netnsIP(t, inNS, "-6", "route", "add", testGuestV6Prefix, "via", testNodeUplinkV6, "dev", "up1")
		netnsIP(t, vm1NS, "addr", "add", testGuestV6Addr+"/64", "dev", iface1, "nodad")
		netnsIP(t, vm1NS, "-6", "route", "add", "default", "via", testBridgeV6Addr, "dev", iface1)
		v6App = netProbeStart(t, inNS, netProbeSpec{Action: "listen-tcp", Network: "tcp6", Address: testV6AppAddr, Reply: "y"})
	}

	// Phase 1: the policy is installed. Every target has a live listener, so a
	// timeout means the packet was dropped, not that nothing answered.
	netnsWantOK(t, "public egress", netnsDial(t, vm1NS, "tcp4", testUplink+":8080", ""))
	records := publicApp.records(t)
	if len(records) == 0 {
		t.Fatal("the public listener recorded no connection")
	}
	if !strings.HasPrefix(records[0].Peer, testIngress+":") {
		t.Errorf("public egress peer = %s, want the masqueraded node address %s", records[0].Peer, testIngress)
	}
	netnsWantClass(t, "allowed egress without a listener", netnsDial(t, vm1NS, "tcp4", testUplink+":9999", ""), "refused")
	for _, blocked := range []struct {
		name    string
		address string
	}{
		{"vm to host", "10.42.0.1:22"},
		{"vm to vm", testVM2IP + ":22"},
		{"vm to private range", testPrivateTarget + ":22"},
		{"vm to management", testMgmtTarget + ":22"},
		{"vm to cgnat", testCGNATTarget + ":22"},
	} {
		netnsWantClass(t, blocked.name, netnsDial(t, vm1NS, "tcp4", blocked.address, ""), "timeout")
	}
	netnsWantOK(t, "configured resolver", netnsDial(t, vm1NS, "udp4", testUplink+":53", ""))
	netnsWantClass(t, "unconfigured resolver", netnsDial(t, vm1NS, "udp4", "8.8.8.8:53", ""), "timeout")
	netnsWantClass(t, "spoofed source address", netnsDial(t, vm1NS, "tcp4", testUplink+":8080", testSpoofIP), "timeout")

	// A guest that changes its own MAC is dropped by the bridge source check.
	guest1MAC := vm1.MAC
	netnsIP(t, vm1NS, "link", "set", "dev", iface1, "address", "52:54:00:de:ad:01")
	netnsWantClass(t, "spoofed source mac", netnsDial(t, vm1NS, "tcp4", testUplink+":8080", ""), "timeout")
	netnsIP(t, vm1NS, "link", "set", "dev", iface1, "address", guest1MAC)

	// A forged ARP reply claiming another guest's address must not replace the
	// legitimate neighbour entry the node holds for that guest. Default
	// arp_accept=0 ignores unsolicited replies for unknown addresses but still
	// updates an existing entry, so the test seeds the real entry first (and
	// drops the replacement throttle inside this throwaway namespace).
	netnsIP(t, nodeNS, "neigh", "replace", testVM2IP, "lladdr", vm2.MAC, "dev", "bbraft0", "nud", "stale")
	if got := netProbeRunOf(t, nodeNS, netProbeSpec{Action: "write-file", Path: "/proc/sys/net/ipv4/neigh/bbraft0/locktime", Value: "0"}); !got.OK {
		t.Fatalf("set neighbour locktime in the node namespace: %s", got.Err)
	}
	netnsForgedARP(t, vm1NS, iface1, testVM2IP, guest1MAC)
	if ll := netnsNeighLLAddr(t, nodeNS, testVM2IP); ll != vm2.MAC {
		t.Errorf("arp spoof changed the node neighbour entry: %s -> %q, want the legitimate %s", testVM2IP, ll, vm2.MAC)
	}

	// A broadcast frame from a guest must not reach another guest's listener.
	netnsWantClass(t, "guest broadcast",
		netnsDialBroadcast(t, vm1NS, testBroadcastAddr+":"+strconv.Itoa(testBroadcastPort)), "timeout")
	if got := broadcastReceiver.records(t); len(got) != 0 {
		t.Errorf("the guest broadcast reached the peer listener: %v", got)
	}

	// SSH ingress: only the allocated (address, port) tuples reach guest 22, and
	// DNAT preserves the client address, including a tailnet client.
	netnsWantOK(t, "vm1 ssh ingress", netnsDial(t, inNS, "tcp4", netnsHostPort(vm1.SSHEndpoint), ""))
	netnsWantOK(t, "vm2 ssh ingress", netnsDial(t, inNS, "tcp4", netnsHostPort(vm2.SSHEndpoint), ""))
	netnsWantOK(t, "ssh ingress from a tailnet client",
		netnsDial(t, inNS, "tcp4", netnsHostPort(vm1.SSHEndpoint), testTailnet))
	netnsWantPeers(t, "vm1 ssh ingress client addresses", vm1SSH, testUplink+":", testTailnet+":")
	netnsWantPeers(t, "vm2 ssh ingress client addresses", vm2SSH, testUplink+":")
	netnsWantClass(t, "unallocated ingress port", netnsDial(t, inNS, "tcp4", testIngress+":30009", ""), "timeout")
	netnsWantClass(t, "guest hairpin through ingress",
		netnsDial(t, vm1NS, "tcp4", testIngress+":"+strconv.Itoa(int(vm2.SSHPort)), ""), "timeout")
	netnsWantClass(t, "guest initiated management connection",
		netnsDial(t, vm1NS, "tcp4", testTailnet+":22", ""), "timeout")

	// Ingress interface binding: the same ingress address and port must not be
	// reachable from another external interface. The wrong client's frame still
	// has the configured ingress address as its IP destination but arrives on
	// wrong0, so only the binding can deny it.
	boundRecords := len(vm1SSH.records(t))
	netnsWantClass(t, "ssh ingress from the wrong interface",
		netnsDial(t, wrongNS, "tcp4", netnsHostPort(vm1.SSHEndpoint), testWrongClient+":"+testWrongPortBound), "timeout")
	if got := len(vm1SSH.records(t)); got != boundRecords {
		t.Errorf("wrong-interface SSH ingress reached the guest listener: records %d -> %d", boundRecords, got)
	}
	netnsWantOK(t, "configured-interface ssh ingress",
		netnsDial(t, inNS, "tcp4", netnsHostPort(vm1.SSHEndpoint), ""))

	// No-bind positive control: the same wrong-interface frame reaches the
	// guest when the interface binding is removed. Only the in-memory copies
	// lose the binding; the persisted allocation is never edited, and the
	// manager restores the bound rules from its own state afterwards.
	unboundCfg := cfg
	unboundCfg.IngressIface = ""
	unboundAllocs := m.Allocations()
	for i := range unboundAllocs {
		unboundAllocs[i].IngressIface = ""
	}
	unboundRules, err := Ruleset(unboundCfg, unboundAllocs)
	if err != nil {
		t.Fatalf("unbound Ruleset: %v", err)
	}
	netnsNft(t, nodeNS, unboundRules)
	beforeControl := len(vm1SSH.records(t))
	netnsWantOK(t, "ssh ingress from the wrong interface without binding",
		netnsDial(t, wrongNS, "tcp4", netnsHostPort(vm1.SSHEndpoint), testWrongClient+":"+testWrongPortControl))
	if got := len(netnsWaitRecords(t, vm1SSH, beforeControl+1)); got != beforeControl+1 {
		t.Errorf("no-bind control listener records = %d, want %d", got, beforeControl+1)
	}
	if _, err := m.Reconcile(t.Context(), []string{"bx_abcdefgh", "bx_98765432"}); err != nil {
		t.Fatalf("restore bound rules: %v", err)
	}
	beforeRestored := len(vm1SSH.records(t))
	netnsWantClass(t, "ssh ingress from the wrong interface after restore",
		netnsDial(t, wrongNS, "tcp4", netnsHostPort(vm1.SSHEndpoint), testWrongClient+":"+testWrongPortRestored), "timeout")
	if got := len(vm1SSH.records(t)); got != beforeRestored {
		t.Errorf("restored binding let wrong-interface ingress reach the guest: records %d -> %d", beforeRestored, got)
	}
	netnsWantOK(t, "configured-interface ssh ingress after restore",
		netnsDial(t, inNS, "tcp4", netnsHostPort(vm1.SSHEndpoint), ""))
	if persisted, ok := m.Allocation("bx_abcdefgh"); !ok || persisted.IngressIface != "up0" {
		t.Errorf("restoring rules changed the persisted ingress interface: %+v (ok %v)", persisted, ok)
	}
	if v6Enabled {
		got := netnsDial(t, vm1NS, "tcp6", testV6AppAddr, "")
		if got.OK {
			t.Errorf("ipv6 bypass: the guest reached an IPv6 destination (reply %q)", got.Reply)
		}
		if records := v6App.records(t); len(records) != 0 {
			t.Errorf("ipv6 bypass: the IPv6 listener recorded %v", records)
		}
	}

	// Phase 2: positive controls. Remove every filter this test installed and
	// repeat the same probes through the same topology; each must succeed,
	// proving the listener, route and packet path work and that phase 1 failed
	// because of the rules under test.
	netnsPolicyOff(t, nodeNS)
	netnsSetIsolation(t, nodeNS, "off", vm1.TAPName, vm2.TAPName)
	for _, target := range []struct {
		name    string
		address string
	}{
		{"host control", "10.42.0.1:22"},
		{"peer control", testVM2IP + ":22"},
		{"private control", testPrivateTarget + ":22"},
		{"management control", testMgmtTarget + ":22"},
		{"cgnat control", testCGNATTarget + ":22"},
	} {
		netnsWantOK(t, target.name, netnsDial(t, vm1NS, "tcp4", target.address, ""))
	}
	netnsWantOK(t, "resolver control", netnsDial(t, vm1NS, "udp4", "8.8.8.8:53", ""))
	netnsWantClass(t, "hairpin control", netnsDial(t, vm1NS, "tcp4", testIngress+":"+strconv.Itoa(int(vm2.SSHPort)), ""), "refused")
	netnsWantOK(t, "spoofed source control", netnsDial(t, vm1NS, "tcp4", testUplink+":8080", testSpoofIP))
	// Broadcast control: the reply must come from the intended receiver, and the
	// receiver must record the guest as the sender.
	broadcastControl := netnsDialBroadcast(t, vm1NS, testBroadcastAddr+":"+strconv.Itoa(testBroadcastPort))
	netnsWantOK(t, "broadcast control", broadcastControl)
	if !strings.HasPrefix(broadcastControl.Peer, testVM2IP+":") {
		t.Errorf("broadcast control responder = %q, want the peer listener %s", broadcastControl.Peer, testVM2IP)
	}
	if got := netnsWaitRecords(t, broadcastReceiver, 1); len(got) != 1 {
		t.Errorf("the broadcast control reached %d listeners, want 1", len(got))
	}
	netnsIP(t, vm1NS, "link", "set", "dev", iface1, "address", "52:54:00:de:ad:01")
	netnsWantOK(t, "spoofed source mac control", netnsDial(t, vm1NS, "tcp4", testUplink+":8080", ""))
	netnsIP(t, vm1NS, "link", "set", "dev", iface1, "address", guest1MAC)
	// The control taught the node the spoofed MAC; drop it before the remaining
	// probes so the guest is reachable again at its real address.
	netnsIP(t, nodeNS, "neigh", "flush", "all")
	if v6Enabled {
		netnsWantOK(t, "ipv6 control", netnsDial(t, vm1NS, "tcp6", testV6AppAddr, ""))
		if got := netnsWaitRecords(t, v6App, 1); len(got) != 1 {
			t.Errorf("ipv6 control reached %d listeners, want 1: the routed IPv6 path through the node is broken", len(got))
		}
	}
	// ARP control: with the filters gone, the forged reply must replace the
	// seeded legitimate entry.
	netnsIP(t, nodeNS, "neigh", "replace", testVM2IP, "lladdr", vm2.MAC, "dev", "bbraft0", "nud", "stale")
	netnsForgedARP(t, vm1NS, iface1, testVM2IP, guest1MAC)
	if ll := netnsNeighLLAddr(t, nodeNS, testVM2IP); ll != guest1MAC {
		t.Errorf("arp spoof control did not replace the neighbour entry: %s -> %q, want the attacker %s", testVM2IP, ll, guest1MAC)
	}

	// Restore the policy and re-check a representative set: the tests must fail
	// if the rules were not actually reinstalled. The neighbour entry is reset to
	// the legitimate MAC before the filter returns, so the recheck proves the
	// rule keeps it.
	netnsIP(t, nodeNS, "neigh", "replace", testVM2IP, "lladdr", vm2.MAC, "dev", "bbraft0", "nud", "stale")
	netnsSetIsolation(t, nodeNS, "on", vm1.TAPName, vm2.TAPName)
	netnsNft(t, nodeNS, template)
	if _, err := m.Reconcile(t.Context(), []string{"bx_abcdefgh", "bx_98765432"}); err != nil {
		t.Fatalf("Reconcile after the controls: %v", err)
	}
	for _, target := range []struct {
		name    string
		address string
	}{
		{"host after restore", "10.42.0.1:22"},
		{"private after restore", testPrivateTarget + ":22"},
		{"cgnat after restore", testCGNATTarget + ":22"},
	} {
		netnsWantClass(t, target.name, netnsDial(t, vm1NS, "tcp4", target.address, ""), "timeout")
	}
	netnsWantClass(t, "spoofed source after restore", netnsDial(t, vm1NS, "tcp4", testUplink+":8080", testSpoofIP), "timeout")
	broadcastBefore := len(broadcastReceiver.records(t))
	netnsWantClass(t, "guest broadcast after restore", netnsDialBroadcast(t, vm1NS, testBroadcastAddr+":"+strconv.Itoa(testBroadcastPort)), "timeout")
	if got := len(broadcastReceiver.records(t)); got != broadcastBefore {
		t.Errorf("the broadcast after restore reached the peer listener (%d records, want %d)", got, broadcastBefore)
	}
	netnsForgedARP(t, vm1NS, iface1, testVM2IP, guest1MAC)
	if ll := netnsNeighLLAddr(t, nodeNS, testVM2IP); ll != vm2.MAC {
		t.Errorf("arp spoof changed the neighbour entry after restore: %s -> %q, want the legitimate %s", testVM2IP, ll, vm2.MAC)
	}
	netnsWantOK(t, "vm2 ssh ingress after restore", netnsDial(t, inNS, "tcp4", netnsHostPort(vm2.SSHEndpoint), ""))
	finalRecords := len(vm1SSH.records(t))
	netnsWantClass(t, "wrong-interface ssh ingress after full restore",
		netnsDial(t, wrongNS, "tcp4", netnsHostPort(vm1.SSHEndpoint), testWrongClient+":"+testWrongPortFinal), "timeout")
	if got := len(vm1SSH.records(t)); got != finalRecords {
		t.Errorf("full restore let wrong-interface ingress reach the guest: records %d -> %d", finalRecords, got)
	}

	// Release removes the tuple, the TAP and the state, and leaves the hardening
	// table untouched.
	if err := m.Release(t.Context(), "bx_abcdefgh"); err != nil {
		t.Fatalf("Release vm1: %v", err)
	}
	netnsWantClass(t, "released ingress port", netnsDial(t, inNS, "tcp4", netnsHostPort(vm1.SSHEndpoint), ""), "timeout")
	if _, err := netnsIPOutput(nodeNS, "link", "show", vm1.TAPName); err == nil {
		t.Errorf("released TAP %s still exists", vm1.TAPName)
	}
	if got := netTestStateFiles(t, cfg.StateDir); !slices.Equal(got, []string{"bx_98765432.json"}) {
		t.Errorf("state files after release = %v", got)
	}
	if after := netnsNftList(t, nodeNS, "inet", "bbraft_filter"); after != hardeningBefore {
		t.Errorf("hardening table changed:\n before:\n%s\n after:\n%s", hardeningBefore, after)
	}
	netnsWantOK(t, "vm2 after vm1 release", netnsDial(t, inNS, "tcp4", netnsHostPort(vm2.SSHEndpoint), ""))
}

// TestRealTapDevice proves the TAP ioctl path (including IFF_VNET_HDR, which
// vhost-net needs and QEMU only probes for a supplied fd) and the manager's link
// setup with a real /dev/net/tun device.
func TestRealTapDevice(t *testing.T) {
	requirePrivilegedNetTest(t)
	if _, err := os.Stat(tunDevice); err != nil {
		netTestPrerequisite(t, false, "%s is unavailable: %v", tunDevice, err)
	}
	base := fmt.Sprintf("bbraft-tap-%d", os.Getpid())
	ns := base + "-node"
	namespaces := []string{ns}
	t.Cleanup(func() { netnsCleanup(t, namespaces) })
	netnsAdd(t, ns)
	netProbeRunOf(t, ns, netProbeSpec{Action: "write-file", Path: "/proc/sys/net/ipv4/ip_forward", Value: "1"})

	cfg := Config{
		Bridge:      "bbraft0",
		Prefix:      netip.MustParsePrefix("10.42.0.0/16"),
		Gateway:     netip.MustParseAddr("10.42.0.1"),
		Resolvers:   []netip.Addr{netip.MustParseAddr(testUplink)},
		IngressAddr: netip.MustParseAddr(testIngress),
		SSHPorts:    PortRange{Min: 30000, Max: 30002},
		StateDir:    t.TempDir(),
	}
	taps := &netnsLinuxTaps{t: t, ns: ns, flags: make(map[string]string)}
	m, err := New(cfg, Deps{Runner: netnsRunner{ns: ns}, Taps: taps})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer m.Close()
	lease, err := m.Setup(t.Context(), "bx_abcdefgh")
	if err != nil {
		t.Fatalf("Setup with a real TAP: %v", err)
	}
	defer lease.Close()
	if lease.Tap == nil {
		t.Fatal("no TAP descriptor")
	}
	kind, err := m.linkKind(t.Context(), lease.TAPName)
	if err != nil {
		t.Fatalf("inspect TAP: %v", err)
	}
	if kind != "tun" {
		t.Fatalf("TAP kind = %q, want tun", kind)
	}
	flagText, ok := taps.tapFlags(lease.TAPName)
	if !ok {
		t.Fatalf("the real TAP probe reported no flags")
	}
	flags, err := strconv.Atoi(flagText)
	if err != nil {
		t.Fatalf("decode tap flags %q: %v", flagText, err)
	}
	if flags&int(ifVnetHDR) == 0 {
		t.Errorf("TAP flags = %#x do not include IFF_VNET_HDR; vhost-net cannot work", flags)
	}
	link := netnsIP(t, ns, "-j", "-d", "link", "show", "dev", lease.TAPName)
	for _, want := range []string{
		`"info_kind":"tun"`,
		`"master":"bbraft0"`,
		`"isolated":true`,
		`"ifalias":"` + aliasFor("bx_abcdefgh") + `"`,
		`"address":"` + hostMACForAddr(lease.GuestIP) + `"`,
	} {
		if !strings.Contains(link, want) {
			t.Errorf("TAP link JSON is missing %s: %s", want, link)
		}
	}
	if hostMACForAddr(lease.GuestIP) == lease.MAC {
		t.Fatalf("host TAP MAC equals the guest MAC %s", lease.MAC)
	}

	if err := m.Release(t.Context(), "bx_abcdefgh"); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if _, err := netnsIPOutput(ns, "link", "show", lease.TAPName); err == nil {
		t.Errorf("released TAP %s still exists", lease.TAPName)
	}
	if _, err := netnsIPOutput(ns, "link", "show", "bbraft0"); err != nil {
		t.Errorf("bridge disappeared: %v", err)
	}
}

// ifVnetHDR mirrors unix.IFF_VNET_HDR for a dependency-free assertion in the
// test output.
const ifVnetHDR = 0x1000

func requirePrivilegedNetTest(t *testing.T) {
	t.Helper()
	if os.Getenv(netGateEnv) != "1" {
		t.Skipf("set %s=1 to run the privileged network tests (throwaway VM with sudo only)", netGateEnv)
	}
	if os.Geteuid() != 0 {
		t.Fatalf("%s=1 requires root: run it only inside a throwaway VM", netGateEnv)
	}
	for _, binary := range []string{"ip", "nft"} {
		if _, err := exec.LookPath(binary); err != nil {
			t.Fatalf("%s is required: %v (run internal/node/net/test-setup.sh in the throwaway VM)", binary, err)
		}
	}
}

// netTestPrerequisite skips an optional facility, or fails when the suite is
// explicitly required to exercise everything.
func netTestPrerequisite(t *testing.T, ok bool, format string, args ...any) {
	t.Helper()
	if ok {
		return
	}
	if os.Getenv(netRequireAllEnv) == "1" {
		t.Fatalf("required by %s: %s", netRequireAllEnv, fmt.Sprintf(format, args...))
	}
	t.Skipf(format, args...)
}

// netTestDevicesPrerequisite requires the TAP and vhost devices for CheckHost.
// Missing devices fail the run only when the suite is explicitly required; the
// packet assertions do not depend on them because the TAP is simulated.
func netTestDevicesPrerequisite(t *testing.T) bool {
	t.Helper()
	var missing []string
	for _, device := range []string{tunDevice, vhostNetDevice} {
		if _, err := os.Stat(device); err != nil {
			missing = append(missing, device)
		}
	}
	if len(missing) == 0 {
		return true
	}
	if os.Getenv(netRequireAllEnv) == "1" {
		t.Fatalf("required by %s: missing devices %s (run internal/node/net/test-setup.sh)", netRequireAllEnv, strings.Join(missing, ", "))
	}
	t.Logf("skipping CheckHost: missing devices %s (run internal/node/net/test-setup.sh)", strings.Join(missing, ", "))
	return false
}

// netnsTapper creates a veth pair per TAP: the TAP end stays in the node
// namespace and the peer is moved into the sandbox namespace.
type netnsTapper struct {
	t       *testing.T
	nodeNS  string
	peers   map[string]string
	targets map[string]string
}

func (v *netnsTapper) OpenTap(ctx context.Context, name string) (*os.File, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	target, ok := v.targets[name]
	if !ok {
		return nil, fmt.Errorf("test tapper: no namespace for %s", name)
	}
	peer := name + "p"
	netnsIP(v.t, v.nodeNS, "link", "add", name, "type", "veth", "peer", "name", peer)
	netnsIP(v.t, v.nodeNS, "link", "set", peer, "netns", target)
	v.peers[name] = peer
	return os.CreateTemp(v.t.TempDir(), "tap-")
}

func (v *netnsTapper) OpenVhostNet(context.Context) (*os.File, error) {
	return os.CreateTemp(v.t.TempDir(), "vhost-")
}

// PersistTap is a no-op for a veth: the link lives while the namespaces do.
func (v *netnsTapper) PersistTap(context.Context, *os.File) error { return nil }

func (v *netnsTapper) peer(tapName string) string { return v.peers[tapName] }

// netnsLinuxTaps opens a real TAP inside the namespace through the open-tap
// probe child, which holds the descriptor the way QEMU would, and reports the
// interface flags it got.
type netnsLinuxTaps struct {
	t     *testing.T
	ns    string
	flags map[string]string
	files map[*os.File]string
	taps  map[string]*netProbe
}

func (l *netnsLinuxTaps) OpenTap(ctx context.Context, name string) (*os.File, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	probe := netProbeStart(l.t, l.ns, netProbeSpec{Action: "open-tap", Address: name})
	records := probe.records(l.t)
	if len(records) != 1 || !records[0].OK {
		return nil, fmt.Errorf("open-tap probe for %s reported %v", name, records)
	}
	if l.flags != nil {
		l.flags[name] = records[0].Reply
	}
	if l.taps == nil {
		l.taps = make(map[string]*netProbe)
	}
	l.taps[name] = probe
	file, err := os.CreateTemp(l.t.TempDir(), "tap-")
	if err != nil {
		return nil, err
	}
	if l.files == nil {
		l.files = make(map[*os.File]string)
	}
	l.files[file] = name
	return file, nil
}

func (l *netnsLinuxTaps) PersistTap(ctx context.Context, tap *os.File) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	name := l.files[tap]
	probe, ok := l.taps[name]
	if !ok {
		return fmt.Errorf("persist tap %s: no probe child", name)
	}
	// The real descriptor lives in the probe child; ask it to run TUNSETPERSIST
	// on that same descriptor, which is the same kernel operation the manager
	// performs.
	if err := probe.cmd.Process.Signal(syscall.SIGUSR1); err != nil {
		return fmt.Errorf("persist tap %s: %w", name, err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for _, record := range probe.records(l.t) {
			switch record.Reply {
			case "persisted":
				return nil
			default:
				if record.Err != "" {
					return fmt.Errorf("persist tap %s: %s", name, record.Err)
				}
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("persist tap %s: timed out", name)
}

func (l *netnsLinuxTaps) OpenVhostNet(context.Context) (*os.File, error) {
	return os.CreateTemp(l.t.TempDir(), "vhost-")
}

func (l *netnsLinuxTaps) tapFlags(name string) (string, bool) {
	flags, ok := l.flags[name]
	return flags, ok
}

// netnsRunner runs the manager's ip/nft commands inside the node namespace.
type netnsRunner struct {
	ns string
}

func (r netnsRunner) Run(ctx context.Context, name string, args []string, stdin []byte) error {
	_, err := r.exec(ctx, name, args, stdin)
	return err
}

func (r netnsRunner) Output(ctx context.Context, name string, args []string) ([]byte, error) {
	return r.exec(ctx, name, args, nil)
}

func (r netnsRunner) exec(ctx context.Context, name string, args []string, stdin []byte) ([]byte, error) {
	full := append([]string{"netns", "exec", r.ns, name}, args...)
	cmd := exec.CommandContext(ctx, "ip", full...)
	if stdin != nil {
		cmd.Stdin = bytes.NewReader(stdin)
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		code := -1
		if exitErr, ok := errors.AsType[*exec.ExitError](err); ok {
			code = exitErr.ExitCode()
		}
		return nil, &CommandError{Name: name, Args: slices.Clone(args), Stderr: stderr.String(), ExitCode: code, Err: err}
	}
	return stdout.Bytes(), nil
}

// netProbe is a running background probe child and its result file.
type netProbe struct {
	cmd     *exec.Cmd
	result  string
	ready   string
	output  bytes.Buffer
	started bool
}

func (p *netProbe) kill() {
	if !p.started || p.cmd.Process == nil {
		return
	}
	_ = p.cmd.Process.Kill()
	_ = p.cmd.Wait()
	p.started = false
}

// records reads the records the probe appended so far. Zero records is a valid
// result (a listener that accepted nothing).
func (p *netProbe) records(t *testing.T) []netProbeResult {
	t.Helper()
	return netProbeReadFile(t, p.result)
}

// readyAddr returns the address a listener wrote when it became ready. It is
// empty for probes that do not bind a socket.
func (p *netProbe) readyAddr(t *testing.T) string {
	t.Helper()
	data, err := os.ReadFile(p.ready)
	if err != nil {
		t.Fatalf("read ready file %s: %v", p.ready, err)
	}
	return strings.TrimSpace(string(data))
}

// netProbeStart starts a background probe (a listener or an open-tap helper) and
// waits until it reports ready. Its kill is registered immediately after Start,
// so a failed readiness wait cannot leave the child behind.
func netProbeStart(t *testing.T, ns string, spec netProbeSpec) *netProbe {
	t.Helper()
	dir := t.TempDir()
	spec.ReadyFile = filepath.Join(dir, "ready")
	if spec.TimeoutMS == 0 {
		spec.TimeoutMS = 60_000
	}
	probe := &netProbe{result: filepath.Join(dir, "records.jsonl"), ready: spec.ReadyFile}
	probe.cmd = netProbeCommand(t, ns, spec, probe.result)
	probe.cmd.Stdout = &probe.output
	probe.cmd.Stderr = &probe.output
	if err := probe.cmd.Start(); err != nil {
		t.Fatalf("start probe %+v in %s: %v", spec, ns, err)
	}
	probe.started = true
	t.Cleanup(probe.kill)
	netProbeWaitFor(t, spec.ReadyFile, probe)
	return probe
}

// netProbeRunOf runs a foreground probe and returns its single result.
func netProbeRunOf(t *testing.T, ns string, spec netProbeSpec) netProbeResult {
	t.Helper()
	if spec.TimeoutMS == 0 {
		spec.TimeoutMS = int(netProbeDefaultTimeout / time.Millisecond)
	}
	dir := t.TempDir()
	resultPath := filepath.Join(dir, "result.json")
	cmd := netProbeCommand(t, ns, spec, resultPath)
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output
	if err := cmd.Run(); err != nil {
		t.Fatalf("probe %+v in %s: %v\n%s", spec, ns, err, output.String())
	}
	results := netProbeReadFile(t, resultPath)
	if len(results) != 1 {
		t.Fatalf("probe %+v in %s returned %d results\n%s%s", spec, ns, len(results), output.String(), netProbeFile(resultPath))
	}
	return results[0]
}

// netProbeCommand builds the re-executed probe child in ns.
// netProbeCommand builds the re-executed probe child. An empty ns runs it in the
// current namespace, which is what the unprivileged loopback tests use.
func netProbeCommand(t *testing.T, ns string, spec netProbeSpec, resultPath string) *exec.Cmd {
	t.Helper()
	data, err := json.Marshal(spec)
	if err != nil {
		t.Fatalf("encode probe spec: %v", err)
	}
	helper := []string{os.Args[0], "-test.run=^TestNetProbeHelper$"}
	var cmd *exec.Cmd
	if ns == "" {
		cmd = exec.Command(helper[0], helper[1:]...)
	} else {
		cmd = exec.Command("ip", append([]string{"netns", "exec", ns}, helper...)...)
	}
	cmd.Env = append(os.Environ(),
		netProbeEnv+"="+string(data),
		netProbeResultEnv+"="+resultPath,
	)
	return cmd
}

// netnsDial runs a dial probe inside ns.
func netnsDial(t *testing.T, ns, network, address, localAddr string) netProbeResult {
	t.Helper()
	action := "dial-tcp"
	if strings.HasPrefix(network, "udp") {
		action = "dial-udp"
	}
	return netProbeRunOf(t, ns, netProbeSpec{Action: action, Network: network, Address: address, LocalAddr: localAddr})
}

// netnsDialBroadcast sends a UDP datagram to a subnet broadcast address.
func netnsDialBroadcast(t *testing.T, ns, address string) netProbeResult {
	t.Helper()
	return netProbeRunOf(t, ns, netProbeSpec{Action: "dial-udp", Network: "udp4", Address: address, Broadcast: true})
}

// netnsConfigureGuest gives the guest side its static addressing, its MAC and
// its default route, matching what the QEMU kernel command line and device
// configuration install.
func netnsConfigureGuest(t *testing.T, ns, iface string, lease *Lease) {
	t.Helper()
	netnsIP(t, ns, "link", "set", "lo", "up")
	netnsIP(t, ns, "link", "set", "dev", iface, "address", lease.MAC)
	netnsIP(t, ns, "addr", "add", lease.GuestIP.String()+"/"+strconv.Itoa(lease.Prefix.Bits()), "dev", iface)
	netnsIP(t, ns, "link", "set", iface, "up")
	netnsIP(t, ns, "route", "add", "default", "via", lease.Gateway.String(), "dev", iface)
}

// netnsPolicyOff deletes every filter this test installed, leaving only the
// plain topology. netnsPolicyOn restores the hardening table and the node's own
// tables.
func netnsPolicyOff(t *testing.T, ns string) {
	t.Helper()
	for _, table := range [][2]string{
		{"inet", inetTableName},
		{"bridge", bridgeTableName},
		{"inet", "bbraft_filter"},
	} {
		cmd := exec.Command("ip", "netns", "exec", ns, "nft", "delete", "table", table[0], table[1])
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("delete table %s %s: %v: %s", table[0], table[1], err, out)
		}
	}
}

// netnsSetIsolation turns bridge port isolation on or off for the given taps.
func netnsSetIsolation(t *testing.T, ns, state string, taps ...string) {
	t.Helper()
	for _, tap := range taps {
		netnsIP(t, ns, "link", "set", "dev", tap, "type", "bridge_slave", "isolated", state)
	}
}

// netnsForgedARP sends a forged ARP reply from the guest interface claiming
// victimIP from the attacker's MAC, targeting the node's bridge address.
func netnsForgedARP(t *testing.T, ns, iface, victimIP, attackerMAC string) {
	t.Helper()
	netnsWantOK(t, "arp spoof send", netProbeRunOf(t, ns, netProbeSpec{
		Action: "arp-reply", Iface: iface, SenderIP: victimIP, SenderMAC: attackerMAC,
		TargetIP: "10.42.0.1", TargetMAC: "ff:ff:ff:ff:ff:ff",
	}))
}

// netnsWaitRecords polls a listener until it holds at least want records (or the
// bound expires) and returns what it has.
func netnsWaitRecords(t *testing.T, probe *netProbe, want int) []netProbeResult {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		records := probe.records(t)
		if len(records) >= want || time.Now().After(deadline) {
			return records
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// netnsNeighLLAddr returns the neighbour lladdr the namespace has for ip, or "".
func netnsNeighLLAddr(t *testing.T, ns, ip string) string {
	t.Helper()
	out, err := netnsIPOutput(ns, "neigh", "show", ip)
	if err != nil || strings.TrimSpace(out) == "" {
		return ""
	}
	fields := strings.Fields(out)
	for i, field := range fields {
		if field == "lladdr" && i+1 < len(fields) {
			return fields[i+1]
		}
	}
	return ""
}

// TestNetnsIPv6TopologyInvariants keeps the packet test's IPv6 topology
// unambiguous without privileges: the guest subnet (bridge and guest addresses)
// and the uplink subnet (node uplink, "in" uplink and the IPv6 listener) must be
// distinct /64s. Assigning one /64 to both segments makes every connected route
// ambiguous and the routed IPv6 positive control meaningless, so this test fails
// instead of silently accepting such a topology.
func TestNetnsIPv6TopologyInvariants(t *testing.T) {
	guest := netip.MustParsePrefix(testGuestV6Prefix)
	uplink := netip.MustParsePrefix(testUplinkV6Prefix)
	if guest.Bits() != 64 || uplink.Bits() != 64 {
		t.Fatalf("test prefixes must be /64, got %s and %s", guest, uplink)
	}
	if guest.Overlaps(uplink) {
		t.Fatalf("guest prefix %s overlaps the uplink prefix %s", guest, uplink)
	}
	for _, tt := range []struct {
		name   string
		addr   string
		prefix netip.Prefix
	}{
		{"bridge gateway", testBridgeV6Addr, guest},
		{"guest", testGuestV6Addr, guest},
		{"node uplink", testNodeUplinkV6, uplink},
		{"in uplink", testInUplinkV6, uplink},
	} {
		addr, err := netip.ParseAddr(tt.addr)
		if err != nil {
			t.Fatalf("%s address %q: %v", tt.name, tt.addr, err)
		}
		if !tt.prefix.Contains(addr) {
			t.Errorf("%s %s is not inside %s", tt.name, addr, tt.prefix)
		}
	}
	listener, err := netip.ParseAddrPort(testV6AppAddr)
	if err != nil {
		t.Fatalf("listener address %q: %v", testV6AppAddr, err)
	}
	if !uplink.Contains(listener.Addr()) || listener.Port() == 0 {
		t.Errorf("listener %s must be a port on the uplink prefix %s", listener, uplink)
	}
	if guest.Contains(listener.Addr()) {
		t.Errorf("listener %s must not be a guest address", listener)
	}
}

// TestProbeLoopback runs the probe child in the current namespace (no
// privileges, no netns) and proves the record and source-address invariants the
// packet tests rely on: a fresh listener reports zero records without a read
// failure, a completed dial appends exactly one record, and a bare-IP LocalAddr
// becomes an ephemeral-port socket whose source the server actually sees.
func TestProbeLoopback(t *testing.T) {
	tests := []struct {
		name      string
		listen    string
		dial      string
		network   string
		broadcast bool
	}{
		{name: "tcp", listen: "listen-tcp", dial: "dial-tcp", network: "tcp4"},
		{name: "udp", listen: "listen-udp", dial: "dial-udp", network: "udp4"},
		// The unconnected (broadcast) transport uses the same syscall path as the
		// packet test; a unicast destination on loopback reaches the listener.
		{name: "udp-unconnected", listen: "listen-udp", dial: "dial-udp", network: "udp4", broadcast: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			probe := netProbeStart(t, "", netProbeSpec{
				Action:    tt.listen,
				Network:   tt.network,
				Address:   "127.0.0.1:0",
				Reply:     "y",
				TimeoutMS: 10_000,
			})
			if got := probe.records(t); len(got) != 0 {
				t.Fatalf("a fresh listener reported %v, want zero records", got)
			}
			addr := probe.readyAddr(t)
			if _, _, err := net.SplitHostPort(addr); err != nil {
				t.Fatalf("listener ready address %q is not host:port: %v; child output: %s", addr, err, probe.output.String())
			}
			// A bare IP must be accepted and become the source the server sees.
			got := netProbeRunOf(t, "", netProbeSpec{
				Action:    tt.dial,
				Network:   tt.network,
				Address:   addr,
				LocalAddr: "127.0.0.2",
				TimeoutMS: 3_000,
				Broadcast: tt.broadcast,
			})
			if !got.OK {
				t.Fatalf("dial from a bare local IP failed: class %q err %q", got.Class, got.Err)
			}
			if tt.broadcast && !strings.HasPrefix(got.Peer, "127.0.0.1:") {
				t.Errorf("unconnected transport reported responder %q, want the listener at %s", got.Peer, addr)
			}
			records := probe.records(t)
			if len(records) != 1 {
				t.Fatalf("listener records = %v, want exactly one", records)
			}
			if !strings.HasPrefix(records[0].Peer, "127.0.0.2:") {
				t.Errorf("server saw source %q, want the requested local IP 127.0.0.2", records[0].Peer)
			}
			// An explicit IP:port source is accepted too.
			got = netProbeRunOf(t, "", netProbeSpec{
				Action:    tt.dial,
				Network:   tt.network,
				Address:   addr,
				LocalAddr: "127.0.0.2:0",
				TimeoutMS: 3_000,
				Broadcast: tt.broadcast,
			})
			if !got.OK {
				t.Fatalf("dial from an explicit local IP:port failed: class %q err %q", got.Class, got.Err)
			}
			if len(probe.records(t)) != 2 {
				t.Fatalf("listener records after two dials = %v, want two", probe.records(t))
			}
		})
	}
}

func netnsHostPort(addr netip.AddrPort) string {
	return net.JoinHostPort(addr.Addr().String(), strconv.Itoa(int(addr.Port())))
}

func netnsAdd(t *testing.T, name string) {
	t.Helper()
	if out, err := exec.Command("ip", "netns", "add", name).CombinedOutput(); err != nil {
		t.Fatalf("ip netns add %s: %v: %s", name, err, out)
	}
}

// netnsCleanup deletes the namespaces that exist and proves they are gone. A
// namespace that was never created is not an error, so a partial setup cleans
// up cleanly.
func netnsCleanup(t *testing.T, names []string) {
	t.Helper()
	var errs []string
	var deleted []string
	for _, name := range names {
		path := filepath.Join("/var/run/netns", name)
		if _, err := os.Stat(path); errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if out, err := exec.Command("ip", "netns", "del", name).CombinedOutput(); err != nil {
			errs = append(errs, fmt.Sprintf("delete %s: %v: %s", name, err, out))
			continue
		}
		deleted = append(deleted, name)
		if _, err := os.Stat(path); err == nil {
			errs = append(errs, fmt.Sprintf("%s survived deletion", name))
		}
	}
	if len(errs) > 0 {
		t.Errorf("network namespace cleanup failed: %s", strings.Join(errs, "; "))
		return
	}
	t.Logf("cleaned up network namespaces: %s", strings.Join(deleted, ", "))
}

// netnsIP runs an ip command inside a namespace and fails the test on error.
func netnsIP(t *testing.T, ns string, args ...string) string {
	t.Helper()
	out, err := netnsIPOutput(ns, args...)
	if err != nil {
		t.Fatalf("ip -n %s %s: %v: %s", ns, strings.Join(args, " "), err, out)
	}
	return out
}

// netnsIPOutput runs an ip command inside a namespace and returns its output.
func netnsIPOutput(ns string, args ...string) (string, error) {
	full := append([]string{"-n", ns}, args...)
	out, err := exec.Command("ip", full...).CombinedOutput()
	return string(out), err
}

func netnsNft(t *testing.T, ns, ruleset string) {
	t.Helper()
	cmd := exec.Command("ip", "netns", "exec", ns, "nft", "-f", "-")
	cmd.Stdin = strings.NewReader(ruleset)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("load ruleset in %s: %v: %s", ns, err, out)
	}
}

func netnsNftCheck(t *testing.T, ns, ruleset string) {
	t.Helper()
	cmd := exec.Command("ip", "netns", "exec", ns, "nft", "-c", "-f", "-")
	cmd.Stdin = strings.NewReader(ruleset)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("rendered ruleset does not validate with nft: %v: %s", err, out)
	}
}

// netnsNftList returns the kernel's rendering of one table, used to prove that
// the node never edits another owner's table.
func netnsNftList(t *testing.T, ns, family, table string) string {
	t.Helper()
	cmd := exec.Command("ip", "netns", "exec", ns, "nft", "list", "table", family, table)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("nft list table %s %s: %v: %s", family, table, err, out)
	}
	return string(out)
}

func netProbeReadFile(t *testing.T, path string) []netProbeResult {
	t.Helper()
	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		// A listener that has accepted nothing yet has no records: zero is a
		// legitimate result, not a read failure.
		return nil
	}
	if err != nil {
		t.Fatalf("read probe results %s: %v", path, err)
	}
	var out []netProbeResult
	for line := range strings.SplitSeq(strings.TrimSpace(string(data)), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var result netProbeResult
		if err := json.Unmarshal([]byte(line), &result); err != nil {
			t.Fatalf("decode probe result %q: %v", line, err)
		}
		out = append(out, result)
	}
	return out
}

func netProbeFile(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return "<unreadable: " + err.Error() + ">"
	}
	return string(data)
}

func netProbeWaitFor(t *testing.T, path string, probe *netProbe) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		// Every ready payload is non-empty (a bound address or a TAP name), and
		// the writers publish it atomically, so a zero-length file is not ready.
		if info, err := os.Stat(path); err == nil && info.Size() > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s\nchild output:\n%s\nchild result:\n%s", path, probe.output.String(), netProbeFile(probe.result))
}

func netnsWantOK(t *testing.T, name string, got netProbeResult) {
	t.Helper()
	if !got.OK {
		t.Errorf("%s: expected success, got class %q err %q", name, got.Class, got.Err)
	}
}

func netnsWantClass(t *testing.T, name string, got netProbeResult, want string) {
	t.Helper()
	if got.OK {
		t.Errorf("%s: expected %s, but the probe succeeded (reply %q)", name, want, got.Reply)
		return
	}
	if got.Class != want {
		t.Errorf("%s: expected %s, got class %q err %q", name, want, got.Class, got.Err)
	}
}

// netnsWantPeers asserts that the listener saw one connection from each prefix.
func netnsWantPeers(t *testing.T, name string, probe *netProbe, prefixes ...string) {
	t.Helper()
	records := probe.records(t)
	for _, prefix := range prefixes {
		if !slices.ContainsFunc(records, func(r netProbeResult) bool {
			return strings.HasPrefix(r.Peer, prefix)
		}) {
			t.Errorf("%s: listener saw %v, want a connection from %s", name, records, prefix)
		}
	}
}
