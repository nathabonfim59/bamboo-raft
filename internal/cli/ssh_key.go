package cli

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
)

const (
	// sshKeyFileName is the managed private key pinned by the CLI design.
	sshKeyFileName = "bbraft_ed25519"
	// sshKeyPubName is the convenience public key written next to it.
	sshKeyPubName = "bbraft_ed25519.pub"
	// sshKeyComment is the OpenSSH comment written into new keys.
	sshKeyComment = "bbraft"
	// sshKeyPEMType is the OpenSSH private key armor.
	sshKeyPEMType = "OPENSSH PRIVATE KEY"
	// sshKeyMagic starts every OpenSSH private key blob.
	sshKeyMagic = "openssh-key-v1\x00"
	// sshKeyMaxBytes bounds a managed private key file.
	sshKeyMaxBytes = 64 << 10
	// sshKeyRepairMode is the only mode the CLI leaves on the private key.
	sshKeyRepairMode = 0o600
)

// sshKeyMaterial is the usable managed key of one invocation.
type sshKeyMaterial struct {
	// PrivatePath is the absolute path passed to ssh/scp -i.
	PrivatePath string
	// PublicKey is the OpenSSH public key line installed through the API.
	PublicKey string
}

// sshKeyFiles is the test seam for key publication. A nil createTemp uses the
// real root-confined implementation; tests inject a failing writer without
// changing any package-level state.
type sshKeyFiles struct {
	createTemp func(root *os.Root) (name string, file *os.File, err error)
}

// errSSHKeyMissing marks the absence of the managed private key.
var errSSHKeyMissing = errors.New("managed ssh key is missing")

// ensureSSHKey returns the managed ed25519 key, creating it on first use.
func ensureSSHKey(dir string) (sshKeyMaterial, error) {
	return ensureSSHKeyFiles(dir, sshKeyFiles{})
}

// ensureSSHKeyFiles is ensureSSHKey with an injectable file-creation seam. The
// private key is published atomically: a 0600 temp file is written and closed,
// then hard-linked into place without clobbering. Concurrent creators race on
// Link; the loser reads the winner's complete file.
func ensureSSHKeyFiles(dir string, files sshKeyFiles) (sshKeyMaterial, error) {
	if dir == "" {
		return sshKeyMaterial{}, sshKeyError("ssh key directory is empty")
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return sshKeyMaterial{}, sshKeyError("create the .ssh directory failed")
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return sshKeyMaterial{}, sshKeyError("open the .ssh directory failed")
	}
	defer root.Close()

	material, err := readManagedSSHKey(root, dir)
	if err == nil {
		return material, nil
	}
	if !errors.Is(err, errSSHKeyMissing) {
		return sshKeyMaterial{}, err
	}
	return createManagedSSHKey(root, dir, files)
}

// readManagedSSHKey opens an existing managed key without following symlinks
// or blocking on special files. It validates ownership and material first and
// only then repairs group/other permissions.
func readManagedSSHKey(root *os.Root, dir string) (sshKeyMaterial, error) {
	// O_NONBLOCK makes a FIFO open return instead of blocking; O_NOFOLLOW makes
	// a symlink fail with ELOOP before its target is touched.
	file, err := root.OpenFile(sshKeyFileName, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return sshKeyMaterial{}, errSSHKeyMissing
		}
		return sshKeyMaterial{}, sshKeyInvalid("existing ssh key path is not a usable regular file")
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return sshKeyMaterial{}, sshKeyError("inspect existing ssh key failed")
	}
	if err := validateManagedSSHKeyFile(info); err != nil {
		return sshKeyMaterial{}, err
	}
	raw, err := io.ReadAll(io.LimitReader(file, sshKeyMaxBytes+1))
	if err != nil || len(raw) > sshKeyMaxBytes {
		return sshKeyMaterial{}, sshKeyInvalid("existing ssh key file is not readable")
	}
	_, publicKey, err := parseManagedSSHKey(raw)
	if err != nil {
		return sshKeyMaterial{}, err
	}
	if err := repairManagedSSHKeyPermissions(file, info); err != nil {
		return sshKeyMaterial{}, err
	}
	publishManagedSSHPublicKey(root, publicKey)
	return sshKeyMaterial{
		PrivatePath: filepath.Join(dir, sshKeyFileName),
		PublicKey:   publicKey,
	}, nil
}

// validateManagedSSHKeyFile accepts a regular key file owned by the current
// user. A second hard link is not itself unsafe: it is the normal window
// between Link(temp, final) and Remove(temp) in a concurrent publication. Link
// count only gates permission repair, which could change another name.
func validateManagedSSHKeyFile(info fs.FileInfo) error {
	if !info.Mode().IsRegular() {
		return sshKeyInvalid("existing ssh key path is not a regular file")
	}
	if info.Size() <= 0 || info.Size() > sshKeyMaxBytes {
		return sshKeyInvalid("existing ssh key file has an invalid size")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || int(stat.Uid) != os.Geteuid() {
		return sshKeyInvalid("existing ssh key file is not owned by the current user")
	}
	return nil
}

// sshKeyModeMask keeps the exact mode comparison from silently retaining
// setuid, setgid or sticky bits when the key claims the managed 0600 mode.
const sshKeyModeMask = os.ModePerm | os.ModeSetuid | os.ModeSetgid | os.ModeSticky

// repairManagedSSHKeyPermissions chmods an owned single-link key to exactly
// 0600. A key already at exactly 0600 is left alone, including during the
// multi-link publication window. A permissive or extra-bit key with another
// name is refused rather than chmodding through the other link.
func repairManagedSSHKeyPermissions(file *os.File, info fs.FileInfo) error {
	if info.Mode()&sshKeyModeMask == sshKeyRepairMode {
		return nil
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Nlink != 1 {
		return sshKeyInvalid("existing ssh key has extra permissions and is linked under another name")
	}
	if err := file.Chmod(sshKeyRepairMode); err != nil {
		return sshKeyError("repair ssh key permissions failed")
	}
	return nil
}

// createManagedSSHKey generates a fresh key and publishes it atomically.
func createManagedSSHKey(root *os.Root, dir string, files sshKeyFiles) (sshKeyMaterial, error) {
	privateKey, err := generateManagedSSHKey()
	if err != nil {
		return sshKeyMaterial{}, err
	}
	encoded, err := marshalManagedSSHKey(privateKey)
	if err != nil {
		return sshKeyMaterial{}, sshKeyError("encode ssh key failed")
	}
	publicLine := sshEd25519PublicLine(privateKey.Public().(ed25519.PublicKey))

	createTemp := files.createTemp
	if createTemp == nil {
		createTemp = createManagedSSHTemp
	}
	name, file, err := createTemp(root)
	if err != nil {
		return sshKeyMaterial{}, sshKeyError("create ssh key file failed")
	}
	removeTemp := func() {
		if name != "" {
			_ = root.Remove(name)
		}
	}
	if err := writeAndCloseSSHKey(file, encoded); err != nil {
		removeTemp()
		return sshKeyMaterial{}, sshKeyError("write ssh key failed")
	}
	if err := root.Link(name, sshKeyFileName); err != nil {
		removeTemp()
		if errors.Is(err, fs.ErrExist) {
			// Another creator won the publication race with a complete file.
			return readManagedSSHKey(root, dir)
		}
		return sshKeyMaterial{}, sshKeyError("publish ssh key failed")
	}
	removeTemp()
	publishManagedSSHPublicKey(root, publicLine)
	return sshKeyMaterial{
		PrivatePath: filepath.Join(dir, sshKeyFileName),
		PublicKey:   publicLine,
	}, nil
}

// writeAndCloseSSHKey writes the encoded key, syncs it and closes it. The file
// is never linked into place before this succeeds.
func writeAndCloseSSHKey(file *os.File, encoded []byte) error {
	if _, err := file.Write(encoded); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

// createManagedSSHTemp creates a random 0600 temp file in the key root.
func createManagedSSHTemp(root *os.Root) (string, *os.File, error) {
	for range 10 {
		var suffix [8]byte
		if _, err := rand.Read(suffix[:]); err != nil {
			return "", nil, err
		}
		name := ".bbraft_ed25519.tmp-" + hex.EncodeToString(suffix[:])
		file, err := root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, sshKeyRepairMode)
		if err == nil {
			return name, file, nil
		}
		if !errors.Is(err, fs.ErrExist) {
			return "", nil, err
		}
	}
	return "", nil, errors.New("temporary key name collision")
}

// publishManagedSSHPublicKey writes the convenience .pub file best-effort. It
// never replaces an existing file (foreign or stale) and a failure is not
// fatal: the public key sent to the API is always derived from the private key.
func publishManagedSSHPublicKey(root *os.Root, publicLine string) {
	name, file, err := createManagedSSHTemp(root)
	if err != nil {
		return
	}
	defer root.Remove(name)
	if err := file.Chmod(0o644); err != nil {
		_ = file.Close()
		return
	}
	if _, err := io.WriteString(file, publicLine+"\n"); err != nil {
		_ = file.Close()
		return
	}
	if err := file.Close(); err != nil {
		return
	}
	_ = root.Link(name, sshKeyPubName)
}

// generateManagedSSHKey creates one ed25519 key pair from crypto/rand.
func generateManagedSSHKey() (ed25519.PrivateKey, error) {
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, sshKeyError("generate ssh key failed")
	}
	return privateKey, nil
}

// marshalManagedSSHKey encodes privateKey in the OpenSSH unencrypted private
// key format (PROTOCOL.key): one ed25519 key, cipher and KDF none.
func marshalManagedSSHKey(privateKey ed25519.PrivateKey) ([]byte, error) {
	var check [4]byte
	if _, err := rand.Read(check[:]); err != nil {
		return nil, err
	}
	checkValue := binary.BigEndian.Uint32(check[:])
	publicKey := privateKey.Public().(ed25519.PublicKey)

	var private sshWireWriter
	private.uint32(checkValue)
	private.uint32(checkValue)
	private.stringBytes([]byte("ssh-ed25519"))
	private.stringBytes(publicKey)
	private.stringBytes(privateKey)
	private.stringBytes([]byte(sshKeyComment))
	for pad := byte(1); private.buf.Len()%8 != 0; pad++ {
		private.buf.WriteByte(pad)
	}

	var outer sshWireWriter
	outer.buf.WriteString(sshKeyMagic)
	outer.stringBytes([]byte("none"))
	outer.stringBytes([]byte("none"))
	outer.stringBytes(nil)
	outer.uint32(1)
	outer.stringBytes(sshEd25519PublicBlob(publicKey))
	outer.stringBytes(private.bytes())
	return pem.EncodeToMemory(&pem.Block{Type: sshKeyPEMType, Bytes: outer.bytes()}), nil
}

// parseManagedSSHKey decodes an unencrypted OpenSSH ed25519 private key and
// verifies that every derived public half agrees.
func parseManagedSSHKey(raw []byte) (ed25519.PrivateKey, string, error) {
	block, rest := pem.Decode(raw)
	if block == nil || block.Type != sshKeyPEMType || len(bytes.TrimSpace(rest)) != 0 {
		return nil, "", sshKeyInvalid("existing ssh key is not an OpenSSH private key")
	}
	reader := &sshWireReader{data: block.Bytes}
	magic, err := reader.take(len(sshKeyMagic))
	if err != nil || string(magic) != sshKeyMagic {
		return nil, "", sshKeyInvalid("existing ssh key has an invalid format")
	}
	cipherName, err := reader.string()
	if err != nil {
		return nil, "", sshKeyInvalid("existing ssh key has an invalid format")
	}
	if string(cipherName) != "none" {
		return nil, "", sshKeyInvalid("existing ssh key is encrypted")
	}
	kdfName, err := reader.string()
	if err != nil {
		return nil, "", sshKeyInvalid("existing ssh key has an invalid format")
	}
	if string(kdfName) != "none" {
		return nil, "", sshKeyInvalid("existing ssh key is encrypted")
	}
	if _, err := reader.string(); err != nil { // KDF options
		return nil, "", sshKeyInvalid("existing ssh key has an invalid format")
	}
	nkeys, err := reader.uint32()
	if err != nil || nkeys != 1 {
		return nil, "", sshKeyInvalid("existing ssh key has an invalid format")
	}
	publicBlob, err := reader.string()
	if err != nil {
		return nil, "", sshKeyInvalid("existing ssh key has an invalid format")
	}
	privateBlob, err := reader.string()
	if err != nil {
		return nil, "", sshKeyInvalid("existing ssh key has an invalid format")
	}
	if len(privateBlob)%8 != 0 {
		return nil, "", sshKeyInvalid("existing ssh key has an invalid private block")
	}
	if err := reader.done(); err != nil {
		return nil, "", sshKeyInvalid("existing ssh key has an invalid format")
	}
	publicKey, err := parseSSHEd25519PublicBlob(publicBlob)
	if err != nil {
		return nil, "", sshKeyInvalid("existing ssh key is not an ed25519 key")
	}

	privateReader := &sshWireReader{data: privateBlob}
	check1, err := privateReader.uint32()
	if err != nil {
		return nil, "", sshKeyInvalid("existing ssh key has an invalid format")
	}
	check2, err := privateReader.uint32()
	if err != nil || check1 != check2 {
		return nil, "", sshKeyInvalid("existing ssh key has an invalid format")
	}
	keyType, err := privateReader.string()
	if err != nil || string(keyType) != "ssh-ed25519" {
		return nil, "", sshKeyInvalid("existing ssh key is not an ed25519 key")
	}
	privatePublic, err := privateReader.string()
	if err != nil || !bytes.Equal(privatePublic, publicKey) {
		return nil, "", sshKeyInvalid("existing ssh key has mismatched public halves")
	}
	privateBytes, err := privateReader.string()
	if err != nil || len(privateBytes) != ed25519.PrivateKeySize {
		return nil, "", sshKeyInvalid("existing ssh key has an invalid private half")
	}
	if _, err := privateReader.string(); err != nil { // comment
		return nil, "", sshKeyInvalid("existing ssh key has an invalid format")
	}
	for pad := 1; privateReader.off < len(privateReader.data); pad++ {
		value, err := privateReader.take(1)
		if err != nil || value[0] != byte(pad) {
			return nil, "", sshKeyInvalid("existing ssh key has an invalid format")
		}
	}
	if !bytes.Equal(privateBytes[32:], publicKey) {
		return nil, "", sshKeyInvalid("existing ssh key has mismatched public halves")
	}
	derived := ed25519.NewKeyFromSeed(privateBytes[:32])
	if !bytes.Equal(derived, privateBytes) {
		return nil, "", sshKeyInvalid("existing ssh key has an invalid private half")
	}
	return derived, sshEd25519PublicLine(publicKey), nil
}

// parseSSHEd25519PublicBlob validates one ssh-ed25519 public blob.
func parseSSHEd25519PublicBlob(blob []byte) (ed25519.PublicKey, error) {
	reader := &sshWireReader{data: blob}
	keyType, err := reader.string()
	if err != nil || string(keyType) != "ssh-ed25519" {
		return nil, errSSHHostKeyInvalid
	}
	publicKey, err := reader.string()
	if err != nil || len(publicKey) != ed25519.PublicKeySize {
		return nil, errSSHHostKeyInvalid
	}
	if err := reader.done(); err != nil {
		return nil, err
	}
	return ed25519.PublicKey(bytes.Clone(publicKey)), nil
}

// sshEd25519PublicBlob builds the SSH wire public-key blob.
func sshEd25519PublicBlob(publicKey ed25519.PublicKey) []byte {
	var writer sshWireWriter
	writer.stringBytes([]byte("ssh-ed25519"))
	writer.stringBytes(publicKey)
	return writer.bytes()
}

// sshEd25519PublicLine builds the OpenSSH public key line for the API.
func sshEd25519PublicLine(publicKey ed25519.PublicKey) string {
	return "ssh-ed25519 " + base64.StdEncoding.EncodeToString(sshEd25519PublicBlob(publicKey)) + " " + sshKeyComment
}

// sshWireWriter writes uint32-length-prefixed SSH wire fields.
type sshWireWriter struct {
	buf bytes.Buffer
}

// uint32 writes one big-endian uint32.
func (w *sshWireWriter) uint32(value uint32) {
	var raw [4]byte
	binary.BigEndian.PutUint32(raw[:], value)
	w.buf.Write(raw[:])
}

// stringBytes writes one length-prefixed byte string.
func (w *sshWireWriter) stringBytes(value []byte) {
	w.uint32(uint32(len(value)))
	w.buf.Write(value)
}

// bytes returns the encoded fields.
func (w *sshWireWriter) bytes() []byte { return w.buf.Bytes() }
