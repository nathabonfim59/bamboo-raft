package cli

import (
	"crypto/ecdh"
	"encoding/base64"
	"errors"
	"math/big"
	"strings"
)

// sshHostKey is one validated OpenSSH public host key. Blob is the decoded
// SSH wire public-key blob, so a known_hosts line can be rebuilt canonically
// instead of trusting response text.
type sshHostKey struct {
	Type string
	Blob []byte
}

// errSSHHostKeyInvalid marks a structurally invalid host key. Callers turn it
// into a safe invalid_response or ssh_host_key_error; no response text is kept.
var errSSHHostKeyInvalid = errors.New("invalid ssh host key")

// sshHostKeyMaxBytes bounds a public host key blob. RSA-8192 is well below it.
const sshHostKeyMaxBytes = 64 << 10

// parseSSHHostKey parses an OpenSSH public key line and validates both the
// declared type and the wire-format key material. Supported types are the
// OpenSSH host key algorithms the CLI can pin: ed25519, RSA, ECDSA P-256/384/
// 521 and the security-key variants. Certificates and ssh-dss are rejected.
// An optional comment may contain any number of words; it is ignored when the
// pin is rebuilt. Spaces and tabs separate fields, as in B4's parser.
func parseSSHHostKey(value string) (sshHostKey, error) {
	if value == "" || sshHasControlBytes(value) {
		return sshHostKey{}, errSSHHostKeyInvalid
	}
	value = strings.Trim(value, " \t")
	if value == "" {
		return sshHostKey{}, errSSHHostKeyInvalid
	}
	fields := strings.Fields(value)
	if len(fields) < 2 {
		return sshHostKey{}, errSSHHostKeyInvalid
	}
	keyType := fields[0]
	if !supportedSSHHostKeyType(keyType) {
		return sshHostKey{}, errSSHHostKeyInvalid
	}
	blob, err := base64.StdEncoding.DecodeString(fields[1])
	if err != nil {
		// ssh-keygen omits padding when the blob length is a multiple of 3;
		// accept the raw form as well.
		blob, err = base64.RawStdEncoding.DecodeString(fields[1])
	}
	if err != nil || len(blob) == 0 || len(blob) > sshHostKeyMaxBytes {
		return sshHostKey{}, errSSHHostKeyInvalid
	}
	if err := validateSSHHostKeyBlob(keyType, blob); err != nil {
		return sshHostKey{}, err
	}
	return sshHostKey{Type: keyType, Blob: blob}, nil
}

// knownHostsLine renders the canonical known_hosts entry for alias. The
// optional server comment is deliberately dropped.
func (k sshHostKey) knownHostsLine(alias string) string {
	return alias + " " + k.Type + " " + base64.StdEncoding.EncodeToString(k.Blob) + "\n"
}

// sshHasControlBytes reports CR, LF, NUL and other ASCII control bytes except
// tab, which is a valid field separator. It runs before trimming so a trailing
// newline is not accepted.
func sshHasControlBytes(value string) bool {
	for i := 0; i < len(value); i++ {
		if value[i] == '\t' {
			continue
		}
		if value[i] < 0x20 || value[i] == 0x7f {
			return true
		}
	}
	return false
}

// supportedSSHHostKeyType reports whether the CLI can validate and pin typ.
func supportedSSHHostKeyType(typ string) bool {
	switch typ {
	case "ssh-ed25519",
		"ssh-rsa",
		"ecdsa-sha2-nistp256",
		"ecdsa-sha2-nistp384",
		"ecdsa-sha2-nistp521",
		"sk-ssh-ed25519@openssh.com",
		"sk-ecdsa-sha2-nistp256@openssh.com",
		"webauthn-sk-ecdsa-sha2-nistp256@openssh.com":
		return true
	default:
		return false
	}
}

// validateSSHHostKeyBlob decodes and checks one wire public-key blob. The inner
// type must match the declared type and every supported type has its exact
// material checked; trailing bytes are refused.
func validateSSHHostKeyBlob(keyType string, blob []byte) error {
	reader := &sshWireReader{data: blob}
	inner, err := reader.string()
	if err != nil || string(inner) != keyType {
		return errSSHHostKeyInvalid
	}
	switch keyType {
	case "ssh-ed25519":
		pub, err := reader.string()
		if err != nil || len(pub) != 32 {
			return errSSHHostKeyInvalid
		}
	case "sk-ssh-ed25519@openssh.com":
		pub, err := reader.string()
		if err != nil || len(pub) != 32 {
			return errSSHHostKeyInvalid
		}
		application, err := reader.string()
		if err != nil || len(application) == 0 { // application
			return errSSHHostKeyInvalid
		}
	case "ssh-rsa":
		if err := validateSSHRSAKey(reader); err != nil {
			return err
		}
	case "ecdsa-sha2-nistp256", "ecdsa-sha2-nistp384", "ecdsa-sha2-nistp521":
		curve, size := sshECDSAKeyType(keyType)
		if err := validateSSHECDSAKey(reader, curveNameForKeyType(keyType), curve, size, false); err != nil {
			return err
		}
	case "sk-ecdsa-sha2-nistp256@openssh.com", "webauthn-sk-ecdsa-sha2-nistp256@openssh.com":
		if err := validateSSHECDSAKey(reader, "nistp256", ecdh.P256(), 65, true); err != nil {
			return err
		}
	default:
		return errSSHHostKeyInvalid
	}
	if err := reader.done(); err != nil {
		return err
	}
	return nil
}

// validateSSHRSAKey checks the e and n mpints structurally: both positive and
// odd, and the modulus greater than the exponent. No strength policy is
// applied here; the server owns key generation.
func validateSSHRSAKey(reader *sshWireReader) error {
	exponentBytes, err := reader.mpint()
	if err != nil {
		return errSSHHostKeyInvalid
	}
	modulusBytes, err := reader.mpint()
	if err != nil {
		return errSSHHostKeyInvalid
	}
	exponent := new(big.Int).SetBytes(exponentBytes)
	modulus := new(big.Int).SetBytes(modulusBytes)
	// e must be odd and at least 3 (BitLen 1 with an odd value is exactly 1);
	// no strength policy beyond B4's structural rule.
	if exponent.Bit(0) != 1 || exponent.BitLen() == 1 {
		return errSSHHostKeyInvalid
	}
	if modulus.Bit(0) != 1 || modulus.Cmp(exponent) <= 0 {
		return errSSHHostKeyInvalid
	}
	return nil
}

// validateSSHECDSAKey checks curve name, uncompressed point length and that the
// point actually belongs to the curve. Security-key types append an
// application string.
func validateSSHECDSAKey(reader *sshWireReader, wantCurve string, curve ecdh.Curve, pointSize int, securityKey bool) error {
	curveName, err := reader.string()
	if err != nil || string(curveName) != wantCurve {
		return errSSHHostKeyInvalid
	}
	point, err := reader.string()
	if err != nil || len(point) != pointSize || point[0] != 0x04 {
		return errSSHHostKeyInvalid
	}
	if _, err := curve.NewPublicKey(point); err != nil {
		return errSSHHostKeyInvalid
	}
	if securityKey {
		application, err := reader.string()
		if err != nil || len(application) == 0 { // application
			return errSSHHostKeyInvalid
		}
	}
	return nil
}

// sshECDSAKeyType returns the curve and uncompressed point size for a type.
func sshECDSAKeyType(keyType string) (ecdh.Curve, int) {
	switch keyType {
	case "ecdsa-sha2-nistp256":
		return ecdh.P256(), 65
	case "ecdsa-sha2-nistp384":
		return ecdh.P384(), 97
	default:
		return ecdh.P521(), 133
	}
}

// curveNameForKeyType returns the SSH curve identifier of an ECDSA key type.
func curveNameForKeyType(keyType string) string {
	return strings.TrimPrefix(keyType, "ecdsa-sha2-")
}

// sshWireReader reads the uint32-length-prefixed fields of an SSH wire blob.
type sshWireReader struct {
	data []byte
	off  int
}

// uint32 reads one big-endian uint32.
func (r *sshWireReader) uint32() (uint32, error) {
	if len(r.data)-r.off < 4 {
		return 0, errSSHHostKeyInvalid
	}
	value := uint32(r.data[r.off])<<24 | uint32(r.data[r.off+1])<<16 |
		uint32(r.data[r.off+2])<<8 | uint32(r.data[r.off+3])
	r.off += 4
	return value, nil
}

// string reads one length-prefixed byte string.
func (r *sshWireReader) string() ([]byte, error) {
	size, err := r.uint32()
	if err != nil {
		return nil, err
	}
	if uint64(size) > uint64(len(r.data)-r.off) {
		return nil, errSSHHostKeyInvalid
	}
	value := r.data[r.off : r.off+int(size)]
	r.off += int(size)
	return value, nil
}

// take reads exactly size raw bytes.
func (r *sshWireReader) take(size int) ([]byte, error) {
	if size < 0 || len(r.data)-r.off < size {
		return nil, errSSHHostKeyInvalid
	}
	value := r.data[r.off : r.off+size]
	r.off += size
	return value, nil
}

// mpint reads a positive multi-precision integer as its magnitude bytes.
func (r *sshWireReader) mpint() ([]byte, error) {
	value, err := r.string()
	if err != nil || len(value) == 0 || value[0]&0x80 != 0 {
		return nil, errSSHHostKeyInvalid
	}
	return value, nil
}

// done reports trailing bytes after the last field.
func (r *sshWireReader) done() error {
	if r.off != len(r.data) {
		return errSSHHostKeyInvalid
	}
	return nil
}
