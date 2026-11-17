package api

// Pure validation tests for SSH public keys and node-provided connection
// metadata. They need no store, node or database.

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"math/big"
	"net/http"
	"strings"
	"testing"

	"github.com/nathabonfim59/bamboo-raft/internal/apierr"
	"github.com/nathabonfim59/bamboo-raft/internal/nodefake"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

func TestSSHCanonicalPublicKey(t *testing.T) {
	ed25519Key := sshTestKeyLine(0xaa, "user@example")
	tests := []struct {
		name string
		line string
		want string
		ok   bool
	}{
		{name: "ed25519", line: ed25519Key, want: ed25519Key, ok: true},
		{name: "rsa", line: sshTestKeyLineType("ssh-rsa", 0x11, "rsa@example"),
			want: sshTestKeyLineType("ssh-rsa", 0x11, "rsa@example"), ok: true},
		{name: "ecdsa", line: sshTestKeyLineType("ecdsa-sha2-nistp256", 0x22, ""),
			want: sshTestKeyLineType("ecdsa-sha2-nistp256", 0x22, ""), ok: true},
		{name: "security key", line: sshTestKeyLineType("sk-ssh-ed25519@openssh.com", 0x33, "sk"),
			want: sshTestKeyLineType("sk-ssh-ed25519@openssh.com", 0x33, "sk"), ok: true},
		{name: "extra whitespace normalized", line: " \t" + sshTestKeyLineType("ssh-ed25519", 0x44, "a   b") + "  ",
			want: sshTestKeyLineType("ssh-ed25519", 0x44, "a b"), ok: true},
		{name: "empty", line: "", ok: false},
		{name: "spaces only", line: " \t ", ok: false},
		{name: "type only", line: "ssh-ed25519", ok: false},
		{name: "unknown type", line: "ssh-magic " + sshTestKeyLine(0xaa, ""), ok: false},
		{name: "bad base64", line: "ssh-ed25519 !!!! user", ok: false},
		{name: "embedded type mismatch", line: "ssh-rsa " + sshTestSecondField(ed25519Key) + " user", ok: false},
		{name: "private key", line: "-----BEGIN OPENSSH PRIVATE KEY-----\nabc\n-----END OPENSSH PRIVATE KEY-----", ok: false},
		{name: "options", line: `command="id" ` + ed25519Key, ok: false},
		{name: "trailing newline", line: ed25519Key + "\n", ok: false},
		{name: "leading newline", line: "\n" + ed25519Key, ok: false},
		{name: "carriage return", line: ed25519Key + "\r", ok: false},
		{name: "nul", line: ed25519Key + "\x00", ok: false},
		{name: "vertical tab", line: "ssh-ed25519\v" + sshTestSecondField(ed25519Key), ok: false},
		{name: "form feed", line: "ssh-ed25519\f" + sshTestSecondField(ed25519Key), ok: false},
		{name: "tab separated", line: "ssh-ed25519\t" + sshTestSecondField(ed25519Key),
			want: "ssh-ed25519 " + sshTestSecondField(ed25519Key), ok: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, ok := canonicalSSHPublicKey(test.line)
			if ok != test.ok {
				t.Fatalf("ok = %v, want %v", ok, test.ok)
			}
			if ok && got != test.want {
				t.Errorf("canonical = %q, want %q", got, test.want)
			}
		})
	}
}

func sshTestSecondField(line string) string {
	fields := strings.Fields(line)
	if len(fields) < 2 {
		return ""
	}
	return fields[1]
}

func TestSSHKeyNativeToken(t *testing.T) {
	tests := []struct {
		value string
		ok    bool
	}{
		{value: "user", ok: true},
		{value: "-", ok: true},
		{value: "", ok: false},
		{value: " user", ok: false},
		{value: "user ", ok: false},
		{value: "us er", ok: false},
		{value: "us\ter", ok: false},
		{value: "us\x00er", ok: false},
		{value: "us\ner", ok: false},
	}
	for _, test := range tests {
		if got := sshKeyNativeToken(test.value); got != test.ok {
			t.Errorf("sshKeyNativeToken(%q) = %v, want %v", test.value, got, test.ok)
		}
	}
}

func TestSSHKeyNativeHost(t *testing.T) {
	tests := []struct {
		name string
		host string
		want string
		ok   bool
	}{
		{name: "ipv4", host: "203.0.113.10", want: "203.0.113.10", ok: true},
		{name: "ipv6 native", host: "2001:0db8:0:0:0:0:0:1", want: "2001:db8::1", ok: true},
		{name: "ipv6 bracketed", host: "[2001:db8::1]", want: "2001:db8::1", ok: true},
		{name: "hostname", host: "node.example", want: "node.example", ok: true},
		{name: "empty", host: "", ok: false},
		{name: "padded", host: " 203.0.113.10", ok: false},
		{name: "inner space", host: "203.0.113.10 x", ok: false},
		{name: "brackets not ip", host: "[node]", ok: false},
		{name: "unclosed bracket", host: "[2001:db8::1", ok: false},
		{name: "stray bracket", host: "2001:db8::1]", ok: false},
		{name: "hostname with colon", host: "node:22", ok: false},
		{name: "hostname with slash", host: "node/path", ok: false},
		{name: "zone id", host: "fe80::1%eth0", ok: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, ok := sshKeyNativeHost(test.host)
			if ok != test.ok {
				t.Fatalf("ok = %v, want %v", ok, test.ok)
			}
			if ok && got != test.want {
				t.Errorf("host = %q, want %q", got, test.want)
			}
		})
	}
}

func TestSSHKeyAccessFromNode(t *testing.T) {
	access := func(host string, port uint32) *nodev1.SSHAccess {
		return &nodev1.SSHAccess{User: "user", Host: host, Port: port, HostKey: sshTestKeyLine(0xbb, "host")}
	}
	tests := []struct {
		name       string
		response   *nodev1.SSHAccess
		machineIP  string
		endpoint   string
		wantStatus int
	}{
		{name: "ipv4", response: access("203.0.113.10", 22001),
			machineIP: "203.0.113.10", endpoint: "203.0.113.10:22001"},
		{name: "ipv6 native", response: access("2001:db8::7", 22),
			machineIP: "2001:db8::7", endpoint: "[2001:db8::7]:22"},
		{name: "ipv6 bracketed", response: access("[2001:0db8::7]", 22002),
			machineIP: "2001:db8::7", endpoint: "[2001:db8::7]:22002"},
		{name: "hostname", response: access("node.example", 22003),
			machineIP: "node.example", endpoint: "node.example:22003"},
		{name: "nil", response: nil, wantStatus: http.StatusBadGateway},
		{name: "empty host key", response: &nodev1.SSHAccess{User: "user", Host: "203.0.113.10", Port: 22},
			wantStatus: http.StatusServiceUnavailable},
		{name: "blank host key", response: &nodev1.SSHAccess{User: "user", Host: "203.0.113.10", Port: 22, HostKey: " \t "},
			wantStatus: http.StatusServiceUnavailable},
		{name: "malformed host key", response: &nodev1.SSHAccess{User: "user", Host: "203.0.113.10", Port: 22, HostKey: "ssh-ed25519 !!!"},
			wantStatus: http.StatusBadGateway},
		{name: "empty user", response: &nodev1.SSHAccess{Host: "203.0.113.10", Port: 22, HostKey: sshTestKeyLine(0xbb, "h")},
			wantStatus: http.StatusBadGateway},
		{name: "port zero", response: access("203.0.113.10", 0), wantStatus: http.StatusBadGateway},
		{name: "port too high", response: access("203.0.113.10", 65536), wantStatus: http.StatusBadGateway},
		{name: "bad host", response: access(" node", 22), wantStatus: http.StatusBadGateway},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, apiErr := sshKeyAccessFromNode(test.response)
			if test.wantStatus == 0 {
				if apiErr != nil {
					t.Fatalf("unexpected error: %v", apiErr)
				}
				if got.MachineIP != test.machineIP || got.Endpoint != test.endpoint ||
					got.User != "user" || got.HostKey != test.response.GetHostKey() {
					t.Errorf("access = %+v", got)
				}
				return
			}
			if apiErr == nil || apiErr.Status != test.wantStatus {
				t.Fatalf("error = %v, want status %d", apiErr, test.wantStatus)
			}
			if test.wantStatus == http.StatusServiceUnavailable && apiErr.Code != apierr.CodeSSHHostKeyUnavailable {
				t.Errorf("code = %q", apiErr.Code)
			}
		})
	}
}

// sshTestWireMPInt encodes a positive SSH mpint.
func sshTestWireMPInt(n *big.Int) []byte {
	value := n.Bytes()
	if len(value) > 0 && value[0]&0x80 != 0 {
		value = append([]byte{0}, value...)
	}
	return sshTestWireString(value)
}

// sshTestHostKeyLine renders one host-key line from an SSH wire blob.
func sshTestHostKeyLine(keyType string, blob []byte) string {
	return keyType + " " + base64.StdEncoding.EncodeToString(blob) + " host"
}

func TestSSHCanonicalHostKeyRejectsUnusableMaterial(t *testing.T) {
	ed25519Type := sshTestWireString([]byte("ssh-ed25519"))
	rsaType := sshTestWireString([]byte("ssh-rsa"))
	p256Type := sshTestWireString([]byte("ecdsa-sha2-nistp256"))
	skEd25519Type := sshTestWireString([]byte("sk-ssh-ed25519@openssh.com"))
	skECDSAtype := sshTestWireString([]byte("sk-ecdsa-sha2-nistp256@openssh.com"))
	valid32 := sshTestWireString(make([]byte, 32))
	validPoint := func() []byte {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatalf("generate ecdsa: %v", err)
		}
		point, err := key.PublicKey.Bytes()
		if err != nil {
			t.Fatalf("ecdsa point: %v", err)
		}
		return sshTestWireString(point)
	}()
	rsaExponent := sshTestWireMPInt(big.NewInt(65537))
	rsaExponentThree := sshTestWireMPInt(big.NewInt(3))
	rsaExponentOne := sshTestWireMPInt(big.NewInt(1))
	rsaExponentEven := sshTestWireMPInt(big.NewInt(4))
	rsaModulus := sshTestWireMPInt(new(big.Int).SetBytes([]byte{0x80, 0x01, 0x02}))
	rsaModulusComp := sshTestWireMPInt(big.NewInt(15))
	rsaModulusOne := sshTestWireMPInt(big.NewInt(1))
	rsaModulusSmall := sshTestWireMPInt(big.NewInt(3))
	rsaModulusEven := sshTestWireMPInt(big.NewInt(4))
	tests := []struct {
		name string
		line string
	}{
		{name: "ed25519 type only", line: sshTestHostKeyLine("ssh-ed25519", ed25519Type)},
		{name: "ed25519 short key", line: sshTestHostKeyLine("ssh-ed25519",
			sshTestWireBlob(ed25519Type, sshTestWireString(make([]byte, 31))))},
		{name: "ed25519 long key", line: sshTestHostKeyLine("ssh-ed25519",
			sshTestWireBlob(ed25519Type, sshTestWireString(make([]byte, 33))))},
		{name: "ed25519 trailing byte", line: sshTestHostKeyLine("ssh-ed25519",
			sshTestWireBlob(ed25519Type, valid32, []byte{0x00}))},
		{name: "ed25519 truncated length", line: sshTestHostKeyLine("ssh-ed25519",
			sshTestWireBlob(ed25519Type, []byte{0x00, 0x00, 0x00, 0x20, 0x01, 0x02}))},
		{name: "rsa missing modulus", line: sshTestHostKeyLine("ssh-rsa",
			sshTestWireBlob(rsaType, rsaExponent))},
		{name: "rsa empty exponent", line: sshTestHostKeyLine("ssh-rsa",
			sshTestWireBlob(rsaType, sshTestWireString(nil), rsaModulus))},
		{name: "rsa negative exponent", line: sshTestHostKeyLine("ssh-rsa",
			sshTestWireBlob(rsaType, sshTestWireString([]byte{0x80}), rsaModulus))},
		{name: "rsa trailing byte", line: sshTestHostKeyLine("ssh-rsa",
			sshTestWireBlob(rsaType, rsaExponent, rsaModulus, []byte{0x00}))},
		{name: "rsa exponent one", line: sshTestHostKeyLine("ssh-rsa",
			sshTestWireBlob(rsaType, rsaExponentOne, rsaModulusComp))},
		{name: "rsa even exponent", line: sshTestHostKeyLine("ssh-rsa",
			sshTestWireBlob(rsaType, rsaExponentEven, rsaModulusComp))},
		{name: "rsa modulus one", line: sshTestHostKeyLine("ssh-rsa",
			sshTestWireBlob(rsaType, rsaExponent, rsaModulusOne))},
		{name: "rsa modulus not above exponent", line: sshTestHostKeyLine("ssh-rsa",
			sshTestWireBlob(rsaType, rsaExponent, rsaModulusSmall))},
		{name: "rsa even modulus", line: sshTestHostKeyLine("ssh-rsa",
			sshTestWireBlob(rsaType, rsaExponentThree, rsaModulusEven))},
		{name: "ecdsa curve mismatch", line: sshTestHostKeyLine("ecdsa-sha2-nistp256",
			sshTestWireBlob(p256Type, sshTestWireString([]byte("nistp384")), validPoint))},
		{name: "ecdsa p384 point under p256 type", line: sshTestHostKeyLine("ecdsa-sha2-nistp256",
			sshTestWireBlob(p256Type, sshTestWireString([]byte("nistp384")),
				sshTestWireString(sshTestECDSAPoint(t, elliptic.P384()))))},
		{name: "ecdsa invalid point", line: sshTestHostKeyLine("ecdsa-sha2-nistp256",
			sshTestWireBlob(p256Type, sshTestWireString([]byte("nistp256")), sshTestWireString([]byte{4, 1, 2})))},
		{name: "ecdsa unknown curve", line: sshTestHostKeyLine("ecdsa-sha2-nistp256",
			sshTestWireBlob(p256Type, sshTestWireString([]byte("nistp999")), validPoint))},
		{name: "ecdsa compressed point", line: sshTestHostKeyLine("ecdsa-sha2-nistp256",
			sshTestWireBlob(p256Type, sshTestWireString([]byte("nistp256")),
				sshTestWireString(append([]byte{2}, make([]byte, 32)...))))},
		{name: "sk ed25519 missing application", line: sshTestHostKeyLine("sk-ssh-ed25519@openssh.com",
			sshTestWireBlob(skEd25519Type, valid32))},
		{name: "sk ed25519 empty application", line: sshTestHostKeyLine("sk-ssh-ed25519@openssh.com",
			sshTestWireBlob(skEd25519Type, valid32, sshTestWireString(nil)))},
		{name: "sk ed25519 trailing byte", line: sshTestHostKeyLine("sk-ssh-ed25519@openssh.com",
			sshTestWireBlob(skEd25519Type, valid32, sshTestWireString([]byte("ssh:")), []byte{0x00}))},
		{name: "sk ecdsa missing application", line: sshTestHostKeyLine("sk-ecdsa-sha2-nistp256@openssh.com",
			sshTestWireBlob(skECDSAtype, sshTestWireString([]byte("nistp256")), validPoint))},
		{name: "sk ecdsa wrong curve", line: sshTestHostKeyLine("sk-ecdsa-sha2-nistp256@openssh.com",
			sshTestWireBlob(skECDSAtype, sshTestWireString([]byte("nistp384")), validPoint,
				sshTestWireString([]byte("ssh:"))))},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, ok := canonicalSSHHostKey(test.line); ok {
				t.Fatalf("canonicalSSHHostKey accepted unusable host key: %s", test.line)
			}
			_, apiErr := sshKeyAccessFromNode(&nodev1.SSHAccess{
				User: "user", Host: "203.0.113.10", Port: 22, HostKey: test.line,
			})
			if apiErr == nil || apiErr.Status != http.StatusBadGateway ||
				apiErr.Code != apierr.CodeSandboxDirectFailed {
				t.Fatalf("error = %v, want 502 sandbox_direct_failed", apiErr)
			}
		})
	}
}

func TestSSHCanonicalHostKeyAcceptsSupportedFixtures(t *testing.T) {
	tests := []struct {
		name string
		line string
	}{
		{name: "nodefake ed25519", line: nodefake.HostKey},
		{name: "rsa", line: func() string {
			key, err := rsa.GenerateKey(rand.Reader, 2048)
			if err != nil {
				t.Fatalf("generate rsa: %v", err)
			}
			blob := sshTestWireBlob(
				sshTestWireString([]byte("ssh-rsa")),
				sshTestWireMPInt(big.NewInt(int64(key.E))),
				sshTestWireMPInt(key.N),
			)
			return sshTestHostKeyLine("ssh-rsa", blob)
		}()},
		{name: "ecdsa p256", line: sshTestECDSALine(t, "ecdsa-sha2-nistp256", "nistp256", elliptic.P256())},
		{name: "ecdsa p384", line: sshTestECDSALine(t, "ecdsa-sha2-nistp384", "nistp384", elliptic.P384())},
		{name: "ecdsa p521", line: sshTestECDSALine(t, "ecdsa-sha2-nistp521", "nistp521", elliptic.P521())},
		{name: "sk ed25519", line: sshTestHostKeyLine("sk-ssh-ed25519@openssh.com",
			sshTestWireBlob(sshTestWireString([]byte("sk-ssh-ed25519@openssh.com")),
				sshTestWireString(make([]byte, 32)), sshTestWireString([]byte("ssh:"))))},
		{name: "sk ecdsa", line: func() string {
			point := sshTestECDSAPoint(t, elliptic.P256())
			return sshTestHostKeyLine("sk-ecdsa-sha2-nistp256@openssh.com",
				sshTestWireBlob(sshTestWireString([]byte("sk-ecdsa-sha2-nistp256@openssh.com")),
					sshTestWireString([]byte("nistp256")), sshTestWireString(point),
					sshTestWireString([]byte("ssh:"))))
		}()},
		{name: "tab separated", line: "ssh-ed25519\t" + strings.Split(nodefake.HostKey, " ")[1] + "\thost"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			canonical, ok := canonicalSSHHostKey(test.line)
			if !ok {
				t.Fatalf("canonicalSSHHostKey rejected valid fixture: %s", test.line)
			}
			got, apiErr := sshKeyAccessFromNode(&nodev1.SSHAccess{
				User: "user", Host: "203.0.113.10", Port: 22, HostKey: test.line,
			})
			if apiErr != nil {
				t.Fatalf("sshKeyAccessFromNode: %v", apiErr)
			}
			if got.HostKey != canonical {
				t.Errorf("stored host key = %q, want %q", got.HostKey, canonical)
			}
		})
	}
}

func sshTestECDSAPoint(t *testing.T, curve elliptic.Curve) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(curve, rand.Reader)
	if err != nil {
		t.Fatalf("generate ecdsa: %v", err)
	}
	point, err := key.PublicKey.Bytes()
	if err != nil {
		t.Fatalf("ecdsa point: %v", err)
	}
	return point
}

func sshTestECDSALine(t *testing.T, keyType, curveName string, curve elliptic.Curve) string {
	t.Helper()
	point := sshTestECDSAPoint(t, curve)
	return sshTestHostKeyLine(keyType, sshTestWireBlob(
		sshTestWireString([]byte(keyType)),
		sshTestWireString([]byte(curveName)),
		sshTestWireString(point),
	))
}

func TestSSHMPInt(t *testing.T) {
	tests := []struct {
		name  string
		value []byte
		ok    bool
	}{
		{name: "nil", value: nil, ok: false},
		{name: "empty", value: []byte{}, ok: false},
		{name: "zero", value: []byte{0}, ok: false},
		{name: "double zero", value: []byte{0, 0}, ok: false},
		{name: "sign byte", value: []byte{0, 0x80}, ok: true},
		{name: "small", value: []byte{0x7f}, ok: true},
		{name: "negative", value: []byte{0x80}, ok: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := sshMPInt(test.value); got != test.ok {
				t.Errorf("sshMPInt(%x) = %v, want %v", test.value, got, test.ok)
			}
		})
	}
}
