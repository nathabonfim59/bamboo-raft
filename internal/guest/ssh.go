package guest

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"slices"
	"strings"
	"syscall"

	"golang.org/x/crypto/ssh"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/proto/guestv1"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// SSH key installation. AddSSHKey validates one bare OpenSSH public key line
// with the canonical x/crypto parser (never a handwritten wire parser) and
// writes it to the sandbox user's authorized_keys through the same
// credential-dropped file operations as the file RPCs: the kernel enforces
// ownership, modes, ACLs and sticky directories under the pinned confinement
// roots. The host public key is read from the image's per-boot tmpfs file and
// validated as a bare ed25519 key; the node supplies the externally reachable
// host and port itself.
const (
	// defaultSSHHostKeyFile is where the image publishes the ed25519 host
	// public key on every boot (bbraft-ssh-host-key.service), before sshd
	// starts. It is deliberately not in /etc and never in the image.
	defaultSSHHostKeyFile = "/run/ssh/ssh_host_ed25519_key.pub"
	// sshUser is the sandbox account sshd admits (AllowUsers in the image).
	// The frozen proto's AddSSHKeyResponse.user is "user".
	sshUser = "user"
	// sshDirSpec and sshAuthorizedKeysSpec are v1 API paths under the home
	// spec prefix, so they follow the configured HomeRoot.
	sshDirSpec            = homeSpec + "/.ssh"
	sshAuthorizedKeysSpec = sshDirSpec + "/authorized_keys"
	// sshDirMode and sshAuthorizedKeysMode are the modes sshd's StrictModes
	// requires: a private directory and a non-writable key file, both owned
	// by the sandbox user.
	sshDirMode            = 0o700
	sshAuthorizedKeysMode = 0o600
	// maxSSHAuthorizedKeysBytes bounds the read-merge-rewrite of an existing
	// authorized_keys. The file is replaced as a whole, so an oversized file
	// is refused (OUT_OF_RANGE) rather than silently truncated.
	maxSSHAuthorizedKeysBytes = 4 << 20
	// maxSSHHostKeyBytes bounds the host public key read. A real ed25519
	// line is under 100 bytes; anything near this bound is not a key file.
	maxSSHHostKeyBytes = 64 << 10
)

// sshInstaller holds the state AddSSHKey needs besides the file operations:
// a gate that serializes read-merge-rewrite cycles (so concurrent and
// duplicate calls cannot lose each other's key) and the configured host
// public key path.
type sshInstaller struct {
	gate        sshGate
	hostKeyFile string
}

// sshGate is a context-aware mutual-exclusion gate: one token, taken by the
// current SSH install. A waiting caller gives up as soon as its context ends
// instead of blocking behind a sync.Mutex, which has no cancellation. It needs
// no waiter goroutine and no package state; the constructor fills the token.
type sshGate struct {
	token chan struct{}
}

// newSSHGate builds an initialized gate holding its single token.
func newSSHGate() sshGate {
	g := sshGate{token: make(chan struct{}, 1)}
	g.token <- struct{}{}
	return g
}

// acquire takes the gate or reports the caller's cancellation. Cancellation is
// checked before waiting — with both the free token and ctx.Done ready the
// select choice is random, so an already-canceled caller must not be allowed
// to win — and again after taking the token, returning it to the gate so a
// cancellation that lands at that instant never runs the install and never
// strands the gate.
func (g sshGate) acquire(ctx context.Context) error {
	if g.token == nil {
		return status.Error(codes.Internal, "SSH install gate is not initialized")
	}
	if err := ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}
	select {
	case <-g.token:
	case <-ctx.Done():
		return status.FromContextError(ctx.Err()).Err()
	}
	if err := ctx.Err(); err != nil {
		g.release()
		return status.FromContextError(err).Err()
	}
	return nil
}

// release returns the token. It must only follow a successful acquire.
func (g sshGate) release() { g.token <- struct{}{} }

// AddSSHKey installs one OpenSSH public key in the sandbox user's
// authorized_keys and returns the account and the guest's current ed25519
// host public key. The node turns that into nodev1.SSHAccess by supplying the
// externally reachable host and port: the guest cannot know them.
//
// Installation is idempotent. A key that is already authorized is not written
// again, including when its line carries authorized_keys options (from=,
// command= and friends): a restricted key never gains an unrestricted
// duplicate that would widen the user's own restriction. The request itself
// must be one bare key line — no options, no line breaks, one of the public
// key types the frozen contract's reference implementation accepts — and is
// written in canonical form, so its bytes cannot smuggle extra directives
// into the file. Every filesystem operation runs as the sandbox user under
// the pinned roots, exactly like the file RPCs; the key file is replaced
// atomically with mode 0600 and a private 0700 .ssh. The host key is read
// after the install, so a retry while the boot unit is still generating it
// is idempotent and returns UNAVAILABLE until the key exists and parses.
func (s *Service) AddSSHKey(ctx context.Context, req *guestv1.AddSSHKeyRequest) (*guestv1.AddSSHKeyResponse, error) {
	line, key, err := parseAuthorizedKeyLine(req.GetPublicKey())
	if err != nil {
		return nil, err
	}
	if err := s.ssh.gate.acquire(ctx); err != nil {
		return nil, err
	}
	defer s.ssh.gate.release()
	if err := s.installAuthorizedKey(ctx, line, key); err != nil {
		return nil, err
	}
	hostKey, err := readSSHHostKey(s.ssh.hostKeyFile)
	if err != nil {
		return nil, err
	}
	return &guestv1.AddSSHKeyResponse{User: sshUser, HostKey: hostKey}, nil
}

// parseAuthorizedKeyLine validates exactly one bare OpenSSH public key line
// and returns its canonical authorized_keys form plus the parsed key. Raw CR,
// LF and NUL are rejected before any trimming, so a second key, an options
// sentence or a trailing newline can never be trimmed into acceptance. The
// key type is restricted to the seven bare public-key types the frozen
// contract's reference implementation accepts; certificates, ssh-dss and any
// other format x/crypto happens to parse are refused. Parse errors are
// reported with fixed messages and never echo any part of the input.
func parseAuthorizedKeyLine(raw string) (string, ssh.PublicKey, error) {
	invalid := func(msg string) (string, ssh.PublicKey, error) {
		return "", nil, status.Error(codes.InvalidArgument, msg)
	}
	if strings.ContainsAny(raw, "\r\n\x00") {
		return invalid("public_key must be one line: no line breaks or NUL bytes")
	}
	line := strings.TrimSpace(raw)
	if line == "" {
		return invalid("public_key is empty")
	}
	key, comment, options, rest, err := ssh.ParseAuthorizedKey([]byte(line))
	if err != nil {
		return invalid("public_key is not one OpenSSH public key line")
	}
	if len(options) > 0 {
		return invalid("public_key must not carry authorized_keys options")
	}
	if strings.TrimSpace(string(rest)) != "" {
		return invalid("public_key must be exactly one OpenSSH public key line")
	}
	if !isAuthorizedKeyType(key.Type()) {
		return invalid("public_key is not an accepted public key type")
	}
	canonical := strings.TrimSpace(string(ssh.MarshalAuthorizedKey(key)))
	if comment = strings.TrimSpace(comment); comment != "" {
		canonical += " " + comment
	}
	return canonical, key, nil
}

// isAuthorizedKeyType reports whether t is one of the seven bare public-key
// types the frozen contract accepts (nodefake.ValidPublicKey's list). The
// check is a whitelist on purpose: x/crypto also parses certificates, ssh-dss
// and exotic formats the sandbox must not authorize.
func isAuthorizedKeyType(t string) bool {
	switch t {
	case "ssh-ed25519", "ssh-rsa",
		"ecdsa-sha2-nistp256", "ecdsa-sha2-nistp384", "ecdsa-sha2-nistp521",
		"sk-ssh-ed25519@openssh.com", "sk-ecdsa-sha2-nistp256@openssh.com":
		return true
	}
	return false
}

// installAuthorizedKey ensures a private .ssh directory and one key line,
// rewriting authorized_keys only when the key is not authorized yet.
func (s *Service) installAuthorizedKey(ctx context.Context, line string, key ssh.PublicKey) error {
	if err := s.ensureSSHDir(ctx); err != nil {
		return err
	}
	if err := s.checkAuthorizedKeysEntry(ctx); err != nil {
		return err
	}
	data, info, err := s.readAuthorizedKeys(ctx)
	if err != nil {
		return err
	}
	if info != nil && info.GetMode() != sshAuthorizedKeysMode {
		// sshd's StrictModes refuses a group/world-writable key file; tighten
		// it as the sandbox user. An already-authorized file keeps its bytes.
		if _, err := s.files.chmodFile(ctx, sshAuthorizedKeysSpec, sshAuthorizedKeysMode); err != nil {
			return err
		}
	}
	if authorizedKeyPresent(data, key) {
		return nil
	}
	next := appendAuthorizedKey(data, line)
	if len(next) > maxSSHAuthorizedKeysBytes {
		// The bound applies to the rewritten file too: writing past it would
		// succeed once and then make every later call fail to read the file.
		return status.Errorf(codes.OutOfRange, "%s would exceed the %d byte limit", sshAuthorizedKeysSpec, maxSSHAuthorizedKeysBytes)
	}
	return s.writeAuthorizedKeys(ctx, next)
}

// ensureSSHDir makes .ssh an existing, real directory owned by the sandbox
// user with mode 0700. The entry may come from statFile or from makeDir when a
// concurrent creator won the race; both results go through the same
// symlink/type/mode checks, so a just-created 0755 directory is tightened and
// a link is never accepted. Writing a key through a link the sandbox user
// controls could install it outside the expected path, and sshd would not
// honour it there.
func (s *Service) ensureSSHDir(ctx context.Context) error {
	fi, err := s.files.statFile(ctx, sshDirSpec)
	if err != nil {
		if status.Code(err) != codes.NotFound {
			return err
		}
		fi, err = s.files.makeDir(ctx, sshDirSpec, sshDirMode)
		if err != nil {
			return err
		}
	}
	if fi.GetIsSymlink() {
		return status.Errorf(codes.InvalidArgument, "%s is a symbolic link; refusing to install a key through it", sshDirSpec)
	}
	if fi.GetType() != nodev1.FileType_FILE_TYPE_DIRECTORY {
		return status.Errorf(codes.InvalidArgument, "%s is not a directory", sshDirSpec)
	}
	if fi.GetMode() != sshDirMode {
		if _, err := s.files.chmodFile(ctx, sshDirSpec, sshDirMode); err != nil {
			return err
		}
	}
	return nil
}

// checkAuthorizedKeysEntry refuses an authorized_keys entry that is not a real
// regular file before reading it. The entry is inspected with lstat semantics,
// so a symlink is rejected even when it dangles and ReadFile would only see
// NOT_FOUND; a link must never be replaced or followed to install a key.
func (s *Service) checkAuthorizedKeysEntry(ctx context.Context) error {
	fi, err := s.files.statFile(ctx, sshAuthorizedKeysSpec)
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil
		}
		return err
	}
	if fi.GetIsSymlink() {
		return status.Errorf(codes.InvalidArgument, "%s is a symbolic link; refusing to replace it", sshAuthorizedKeysSpec)
	}
	if fi.GetType() != nodev1.FileType_FILE_TYPE_REGULAR {
		return status.Errorf(codes.InvalidArgument, "%s is not a regular file", sshAuthorizedKeysSpec)
	}
	return nil
}

// readAuthorizedKeys returns the current authorized_keys content and info. A
// missing file is (nil, nil, nil): it is created by the first install. The
// info callback rejects a symlink or non-regular entry before any data is
// read, so a link is never followed to install a key elsewhere.
func (s *Service) readAuthorizedKeys(ctx context.Context) ([]byte, *nodev1.FileInfo, error) {
	var (
		data []byte
		info *nodev1.FileInfo
	)
	err := s.files.readFile(ctx, sshAuthorizedKeysSpec, maxSSHAuthorizedKeysBytes, func(resp *nodev1.ReadFileResponse) error {
		switch part := resp.GetPart().(type) {
		case *nodev1.ReadFileResponse_Info:
			if part.Info.GetIsSymlink() {
				return status.Errorf(codes.InvalidArgument, "%s is a symbolic link; refusing to replace it", sshAuthorizedKeysSpec)
			}
			if part.Info.GetType() != nodev1.FileType_FILE_TYPE_REGULAR {
				return status.Errorf(codes.InvalidArgument, "%s is not a regular file", sshAuthorizedKeysSpec)
			}
			info = part.Info
		case *nodev1.ReadFileResponse_Data:
			data = append(data, part.Data...)
		}
		return nil
	})
	if status.Code(err) == codes.NotFound {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	if info == nil {
		return nil, nil, status.Error(codes.Internal, "read "+sshAuthorizedKeysSpec+": no file info")
	}
	return data, info, nil
}

// authorizedKeyPresent reports whether data already authorizes key. Lines are
// parsed with the canonical parser, so a line that restricts the same public
// key with options counts as present: the parsed public material is compared,
// not the text, comment or options.
func authorizedKeyPresent(data []byte, key ssh.PublicKey) bool {
	want := key.Marshal()
	for line := range strings.SplitSeq(string(data), "\n") {
		got, _, _, _, err := ssh.ParseAuthorizedKey([]byte(line))
		if err == nil && got != nil && bytes.Equal(got.Marshal(), want) {
			return true
		}
	}
	return false
}

// appendAuthorizedKey returns existing plus one canonical key line, preserving
// every existing byte (foreign lines, comments, options and spacing) and
// adding the separator only when the file does not already end a line.
func appendAuthorizedKey(existing []byte, line string) []byte {
	out := make([]byte, 0, len(existing)+len(line)+2)
	out = append(out, existing...)
	if len(out) > 0 && out[len(out)-1] != '\n' {
		out = append(out, '\n')
	}
	out = append(out, line...)
	return append(out, '\n')
}

// writeAuthorizedKeys replaces authorized_keys atomically as the sandbox user:
// the staged sibling is written in bounded chunks, synced and renamed over the
// target, then the directory entry is synced. A canceled or failed write
// removes the staged file and leaves the previous keys intact.
func (s *Service) writeAuthorizedKeys(ctx context.Context, content []byte) error {
	up, err := s.files.newUpload(ctx, sshAuthorizedKeysSpec, sshAuthorizedKeysMode)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if committed {
			return
		}
		abortCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), uploadAbortTimeout)
		defer cancel()
		if err := up.abort(abortCtx); err != nil {
			s.log.Warn("AddSSHKey abort cleanup failed", "err", err)
		}
	}()
	for chunk := range slices.Chunk(content, maxUploadChunk) {
		if err := ctx.Err(); err != nil {
			return status.FromContextError(err).Err()
		}
		if err := up.write(chunk); err != nil {
			return err
		}
	}
	if _, err := up.commit(ctx); err != nil {
		return err
	}
	committed = true
	return nil
}

// readSSHHostKey reads and validates the per-boot ed25519 host public key.
// The path is opened O_NOFOLLOW|O_NONBLOCK and must be a regular file: a link
// is never followed and a FIFO or device can neither block nor be served.
// Errors are classified without echoing file content: a missing or unparsable
// key is UNAVAILABLE (the boot unit has not finished; retry), an unusable path
// or read failure is INTERNAL. Only a bare ssh-ed25519 key is accepted.
func readSSHHostKey(path string) (string, error) {
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		switch {
		case errors.Is(err, fs.ErrNotExist):
			return "", status.Error(codes.Unavailable, "guest SSH host key is not ready")
		case errors.Is(err, syscall.ELOOP):
			return "", status.Error(codes.Internal, "guest SSH host key path is a symbolic link")
		default:
			return "", status.Errorf(codes.Internal, "open guest SSH host key: %v", err)
		}
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return "", status.Errorf(codes.Internal, "stat guest SSH host key: %v", err)
	}
	if !fi.Mode().IsRegular() {
		return "", status.Error(codes.Internal, "guest SSH host key is not a regular file")
	}
	raw, err := io.ReadAll(io.LimitReader(f, maxSSHHostKeyBytes+1))
	if err != nil {
		return "", status.Errorf(codes.Internal, "read guest SSH host key: %v", err)
	}
	if len(raw) > maxSSHHostKeyBytes {
		return "", status.Error(codes.Internal, "guest SSH host key is too large")
	}
	// Only the file's own line termination counts as whitespace. An internal
	// CR, LF or NUL means this is not one host key line at all: the parser
	// would otherwise skip leading garbage or comment lines and happily find a
	// key further down, accepting a file the boot unit never wrote.
	line := strings.TrimSpace(string(raw))
	if strings.ContainsAny(line, "\r\n\x00") {
		return "", status.Error(codes.Unavailable, "guest SSH host key is not one ed25519 public key line")
	}
	key, comment, options, rest, err := ssh.ParseAuthorizedKey([]byte(line))
	valid := err == nil && key != nil && len(options) == 0 &&
		strings.TrimSpace(string(rest)) == "" && key.Type() == "ssh-ed25519"
	if !valid {
		return "", status.Error(codes.Unavailable, "guest SSH host key is not a bare ssh-ed25519 public key")
	}
	// Publish the canonical public material only. ssh-keygen's own comment
	// ("bbraft-guest") is harmless and useful for clients, but the file is
	// still untrusted input to this function: an unsafe comment is dropped
	// rather than echoed, and no file byte ever reaches an error message.
	published := strings.TrimSpace(string(ssh.MarshalAuthorizedKey(key)))
	if comment = strings.TrimSpace(comment); safeSSHComment(comment) {
		published += " " + comment
	}
	return published, nil
}

// safeSSHComment reports whether a host-key comment is safe to publish: a
// short run of printable ASCII. Anything else (control bytes, non-ASCII,
// absurd length) is simply omitted from the response.
func safeSSHComment(comment string) bool {
	if comment == "" || len(comment) > 255 {
		return false
	}
	for i := 0; i < len(comment); i++ {
		if comment[i] < 0x20 || comment[i] > 0x7e {
			return false
		}
	}
	return true
}
