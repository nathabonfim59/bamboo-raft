package api

// SSH key and connection-metadata validation. The request key is canonicalized
// before the RPC; the node result is validated before it can be stored. Only
// public key lines are accepted: options, private keys and multi-line values
// are refused on both sides. Spaces and tabs are normalized; every other
// control character is refused. Host values are kept native (never trimmed)
// and only a valid IP is canonicalized for rendering.
//
// The request prefilter checks type and blob self-consistency and leaves full
// key validation to the node. A node-provided host key is different: it must
// be usable by the client, so its SSH wire blob is decoded structurally
// (algorithm-specific key material, no truncation, no trailing bytes) before
// it can be persisted.

import (
	"crypto/ecdh"
	"encoding/base64"
	"encoding/binary"
	"math/big"
	"net"
	"strconv"
	"strings"
	"unicode"

	"github.com/nathabonfim59/bamboo-raft/internal/apierr"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// sshPublicKeyTypeSupported reports whether keyType is one of the OpenSSH
// public key types the node contract accepts.
func sshPublicKeyTypeSupported(keyType string) bool {
	switch keyType {
	case "ssh-ed25519", "ssh-rsa",
		"ecdsa-sha2-nistp256", "ecdsa-sha2-nistp384", "ecdsa-sha2-nistp521",
		"sk-ssh-ed25519@openssh.com", "sk-ecdsa-sha2-nistp256@openssh.com":
		return true
	default:
		return false
	}
}

// sshKeyAccess is the validated connection metadata of one successful
// AddSSHKey. Endpoint is the rendered host:port pair; MachineIP is the native
// unbracketed host.
type sshKeyAccess struct {
	MachineIP string
	User      string
	Endpoint  string
	HostKey   string
}

// canonicalSSHPublicKey returns the canonical one-line form of an OpenSSH
// public key and whether line is one. A valid line has a known key type, a
// base64 blob whose embedded type matches, and an optional comment. Options
// (which precede the type), private-key PEM blocks and every control character
// except a tab are refused. Spaces and tabs are normalized to single spaces.
func canonicalSSHPublicKey(line string) (string, bool) {
	if strings.ContainsFunc(line, func(r rune) bool { return unicode.IsControl(r) && r != '\t' }) {
		return "", false
	}
	line = strings.TrimSpace(line)
	if line == "" {
		return "", false
	}
	fields := strings.Fields(line)
	if len(fields) < 2 || !sshPublicKeyTypeSupported(fields[0]) {
		return "", false
	}
	blob, err := base64.StdEncoding.DecodeString(fields[1])
	if err != nil || len(blob) < 4 {
		return "", false
	}
	size := binary.BigEndian.Uint32(blob)
	if uint64(size)+4 > uint64(len(blob)) || string(blob[4:4+size]) != fields[0] {
		return "", false
	}
	return strings.Join(fields, " "), true
}

// canonicalSSHHostKey is canonicalSSHPublicKey plus the structural check a
// host key needs to be usable by a client. The embedded algorithm string alone
// is not enough: an ed25519 line with no key bytes must not be persisted.
func canonicalSSHHostKey(line string) (string, bool) {
	canonical, ok := canonicalSSHPublicKey(line)
	if !ok {
		return "", false
	}
	fields := strings.Fields(canonical)
	blob, err := base64.StdEncoding.DecodeString(fields[1])
	if err != nil || !sshHostKeyMaterial(fields[0], blob) {
		return "", false
	}
	return canonical, true
}

// sshHostKeyMaterial validates the SSH wire blob of one supported host key:
// the algorithm string is followed by exactly the key material of that
// algorithm and nothing else. It uses only the standard library.
func sshHostKeyMaterial(keyType string, blob []byte) bool {
	algorithm, rest, ok := sshReadString(blob)
	if !ok || string(algorithm) != keyType {
		return false
	}
	switch keyType {
	case "ssh-ed25519":
		key, rest, ok := sshReadString(rest)
		return ok && len(key) == 32 && len(rest) == 0
	case "ssh-rsa":
		exponentBytes, rest, ok := sshReadString(rest)
		if !ok {
			return false
		}
		modulusBytes, rest, ok := sshReadString(rest)
		if !ok {
			return false
		}
		return sshRSAKeyMaterial(exponentBytes, modulusBytes) && len(rest) == 0
	case "ecdsa-sha2-nistp256", "ecdsa-sha2-nistp384", "ecdsa-sha2-nistp521":
		curveName, rest, ok := sshReadString(rest)
		if !ok || string(curveName) != strings.TrimPrefix(keyType, "ecdsa-sha2-") {
			return false
		}
		point, rest, ok := sshReadString(rest)
		return ok && sshECDSAPoint(string(curveName), point) && len(rest) == 0
	case "sk-ssh-ed25519@openssh.com":
		key, rest, ok := sshReadString(rest)
		if !ok || len(key) != 32 {
			return false
		}
		application, rest, ok := sshReadString(rest)
		return ok && len(application) > 0 && len(rest) == 0
	case "sk-ecdsa-sha2-nistp256@openssh.com":
		curveName, rest, ok := sshReadString(rest)
		if !ok || string(curveName) != "nistp256" {
			return false
		}
		point, rest, ok := sshReadString(rest)
		if !ok || !sshECDSAPoint(string(curveName), point) {
			return false
		}
		application, rest, ok := sshReadString(rest)
		return ok && len(application) > 0 && len(rest) == 0
	default:
		return false
	}
}

// sshReadString reads one SSH string (uint32 length followed by that many
// bytes) and returns its value and the remaining bytes.
func sshReadString(data []byte) (value, rest []byte, ok bool) {
	if len(data) < 4 {
		return nil, nil, false
	}
	size := binary.BigEndian.Uint32(data[:4])
	if uint64(size) > uint64(len(data)-4) {
		return nil, nil, false
	}
	return data[4 : 4+size], data[4+size:], true
}

// sshMPInt reports whether b is a non-zero, non-negative SSH mpint. The
// optional leading zero is the positive sign byte and is not a magnitude zero.
func sshMPInt(b []byte) bool {
	if len(b) == 0 {
		return false
	}
	if b[0] == 0 {
		for _, value := range b[1:] {
			if value != 0 {
				return true
			}
		}
		return false
	}
	return b[0]&0x80 == 0
}

// sshMPIntValue parses a positive SSH mpint into an integer.
func sshMPIntValue(b []byte) (*big.Int, bool) {
	if !sshMPInt(b) {
		return nil, false
	}
	if b[0] == 0 {
		b = b[1:]
	}
	return new(big.Int).SetBytes(b), true
}

// sshRSAKeyMaterial rejects unusable RSA host keys without imposing a
// strength or minimum-bit policy: the exponent must be odd and at least 3, and
// the modulus must be odd and greater than the exponent.
func sshRSAKeyMaterial(exponentBytes, modulusBytes []byte) bool {
	exponent, ok := sshMPIntValue(exponentBytes)
	if !ok || exponent.Bit(0) == 0 || (exponent.IsInt64() && exponent.Int64() < 3) {
		return false
	}
	modulus, ok := sshMPIntValue(modulusBytes)
	return ok && modulus.Bit(0) == 1 && modulus.Cmp(exponent) > 0
}

// sshECDSAPoint reports whether point is a valid uncompressed NIST point for
// the curve named by the key type.
func sshECDSAPoint(curveName string, point []byte) bool {
	if len(point) == 0 || point[0] != 4 {
		return false
	}
	var curve ecdh.Curve
	switch curveName {
	case "nistp256":
		curve = ecdh.P256()
	case "nistp384":
		curve = ecdh.P384()
	case "nistp521":
		curve = ecdh.P521()
	default:
		return false
	}
	_, err := curve.NewPublicKey(point)
	return err == nil
}

// sshKeyAccessFromNode validates one AddSSHKey result. A missing or empty
// host key is the actionably retryable 503; every other malformed result is
// the 502 protocol failure. Neither is ever stored.
func sshKeyAccessFromNode(response *nodev1.SSHAccess) (sshKeyAccess, *apierr.Error) {
	if response == nil {
		return sshKeyAccess{}, sshKeyDirectFailed(true)
	}
	hostKey := response.GetHostKey()
	if strings.TrimSpace(hostKey) == "" {
		return sshKeyAccess{}, sshKeyHostKeyUnavailable()
	}
	canonicalKey, ok := canonicalSSHHostKey(hostKey)
	if !ok {
		return sshKeyAccess{}, sshKeyDirectFailed(true)
	}
	user := response.GetUser()
	if !sshKeyNativeToken(user) {
		return sshKeyAccess{}, sshKeyDirectFailed(true)
	}
	machineIP, ok := sshKeyNativeHost(response.GetHost())
	if !ok {
		return sshKeyAccess{}, sshKeyDirectFailed(true)
	}
	port := response.GetPort()
	if port == 0 || port > 65535 {
		return sshKeyAccess{}, sshKeyDirectFailed(true)
	}
	return sshKeyAccess{
		MachineIP: machineIP,
		User:      user,
		Endpoint:  net.JoinHostPort(machineIP, strconv.FormatUint(uint64(port), 10)),
		HostKey:   canonicalKey,
	}, nil
}

// sshKeyNativeToken reports whether value is a native non-empty token with no
// whitespace or control character. Valid values are used unchanged, never
// silently trimmed.
func sshKeyNativeToken(value string) bool {
	if value == "" {
		return false
	}
	return !strings.ContainsFunc(value, func(r rune) bool {
		return unicode.IsSpace(r) || unicode.IsControl(r)
	})
}

// sshKeyNativeHost validates and normalizes one node-provided host. A valid IP
// is canonicalized; a bracketed IPv6 literal is unbracketed; every other host
// is an opaque hostname used as-is. Whitespace, control characters, brackets
// around a non-IP and embedded colons in a hostname are refused.
func sshKeyNativeHost(host string) (string, bool) {
	if !sshKeyNativeToken(host) {
		return "", false
	}
	if inner, ok := strings.CutPrefix(host, "["); ok {
		inner, ok = strings.CutSuffix(inner, "]")
		if !ok {
			return "", false
		}
		ip := net.ParseIP(inner)
		if ip == nil {
			return "", false
		}
		return ip.String(), true
	}
	if strings.ContainsAny(host, "[]") {
		return "", false
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.String(), true
	}
	if strings.ContainsAny(host, ":/\\") {
		return "", false
	}
	return host, true
}
