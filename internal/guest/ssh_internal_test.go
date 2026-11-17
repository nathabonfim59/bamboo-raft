package guest

import (
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/proto/guestv1"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// sshTestKey returns a fresh public key of the requested algorithm and its
// canonical authorized_keys line without a comment.
func sshTestKey(t *testing.T, algorithm string) (ssh.PublicKey, string) {
	t.Helper()
	var (
		key ssh.PublicKey
		err error
	)
	switch algorithm {
	case "ssh-ed25519":
		var pub ed25519.PublicKey
		pub, _, err = ed25519.GenerateKey(rand.Reader)
		if err == nil {
			key, err = ssh.NewPublicKey(pub)
		}
	case "ssh-rsa":
		var priv *rsa.PrivateKey
		priv, err = rsa.GenerateKey(rand.Reader, 2048)
		if err == nil {
			key, err = ssh.NewPublicKey(&priv.PublicKey)
		}
	case "ecdsa-sha2-nistp256":
		var priv *ecdsa.PrivateKey
		priv, err = ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err == nil {
			key, err = ssh.NewPublicKey(&priv.PublicKey)
		}
	default:
		t.Fatalf("unsupported test algorithm %q", algorithm)
	}
	if err != nil {
		t.Fatalf("generate %s key: %v", algorithm, err)
	}
	return key, strings.TrimSpace(string(ssh.MarshalAuthorizedKey(key)))
}

// sshTestCertificate returns a signed user certificate public key.
func sshTestCertificate(t *testing.T) ssh.PublicKey {
	t.Helper()
	_, caPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := ssh.NewSignerFromKey(caPriv)
	if err != nil {
		t.Fatal(err)
	}
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	key, err := ssh.NewPublicKey(pub)
	if err != nil {
		t.Fatal(err)
	}
	cert := &ssh.Certificate{
		Key:             key,
		CertType:        ssh.UserCert,
		ValidPrincipals: []string{"user"},
		KeyId:           "ssh-internal-test",
		ValidAfter:      0,
		ValidBefore:     ssh.CertTimeInfinity,
	}
	if err := cert.SignCert(rand.Reader, ca); err != nil {
		t.Fatalf("sign certificate: %v", err)
	}
	return cert
}

// sshTestComment rewrites an authorized_keys line with another comment.
func sshTestComment(t *testing.T, line, comment string) string {
	t.Helper()
	key, _, _, _, err := ssh.ParseAuthorizedKey([]byte(line))
	if err != nil {
		t.Fatalf("parse key line: %v", err)
	}
	out := strings.TrimSpace(string(ssh.MarshalAuthorizedKey(key)))
	if comment != "" {
		out += " " + comment
	}
	return out
}

func TestParseAuthorizedKeyLine(t *testing.T) {
	_, edLine := sshTestKey(t, "ssh-ed25519")
	_, rsaLine := sshTestKey(t, "ssh-rsa")
	_, ecdsaLine := sshTestKey(t, "ecdsa-sha2-nistp256")
	_, otherEd := sshTestKey(t, "ssh-ed25519")
	certLine := strings.TrimSpace(string(ssh.MarshalAuthorizedKey(sshTestCertificate(t))))
	edBlobOf := func(line string) string { return strings.Fields(line)[1] }

	tests := []struct {
		name    string
		in      string
		want    string // canonical form when accepted
		wantErr bool
	}{
		{name: "ed25519", in: edLine, want: edLine},
		{name: "ed25519 with comment", in: sshTestComment(t, edLine, "laptop"), want: sshTestComment(t, edLine, "laptop")},
		{name: "rsa", in: rsaLine, want: rsaLine},
		{name: "ecdsa", in: ecdsaLine, want: ecdsaLine},
		{name: "leading and trailing spaces", in: "  \t" + edLine + "\t  ", want: edLine},
		{name: "comment with inner spaces", in: sshTestComment(t, edLine, "two  words"), want: sshTestComment(t, edLine, "two  words")},
		{name: "certificate", in: certLine, wantErr: true},
		{name: "options", in: `from="192.0.2.7" ` + edLine, wantErr: true},
		{name: "two keys", in: edLine + "\n" + otherEd, wantErr: true},
		{name: "trailing newline", in: edLine + "\n", wantErr: true},
		{name: "carriage return", in: edLine + "\r", wantErr: true},
		{name: "nul byte", in: edLine + "\x00", wantErr: true},
		{name: "empty", in: "", wantErr: true},
		{name: "spaces only", in: " \t ", wantErr: true},
		{name: "not a key", in: "not a key", wantErr: true},
		{name: "type only", in: "ssh-ed25519", wantErr: true},
		{name: "bad base64", in: "ssh-ed25519 !!!notbase64", wantErr: true},
		{name: "type and blob mismatch", in: "ssh-rsa " + edBlobOf(edLine), wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			line, key, err := parseAuthorizedKeyLine(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("parseAuthorizedKeyLine(%q) accepted as %q", tc.in, line)
				}
				if code := status.Code(err); code != codes.InvalidArgument {
					t.Fatalf("parseAuthorizedKeyLine(%q) = %v, want InvalidArgument", tc.in, code)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseAuthorizedKeyLine(%q): %v", tc.in, err)
			}
			if line != tc.want {
				t.Fatalf("canonical line = %q, want %q", line, tc.want)
			}
			if key == nil || key.Type() != strings.Fields(tc.want)[0] {
				t.Fatalf("parsed key = %v, want type %q", key, strings.Fields(tc.want)[0])
			}
		})
	}
}

func TestIsAuthorizedKeyType(t *testing.T) {
	tests := map[string]bool{
		"ssh-ed25519":                              true,
		"ssh-rsa":                                  true,
		"ecdsa-sha2-nistp256":                      true,
		"ecdsa-sha2-nistp384":                      true,
		"ecdsa-sha2-nistp521":                      true,
		"sk-ssh-ed25519@openssh.com":               true,
		"sk-ecdsa-sha2-nistp256@openssh.com":       true,
		"ssh-dss":                                  false,
		"ssh-ed25519-cert-v01@openssh.com":         false,
		"ssh-rsa-cert-v01@openssh.com":             false,
		"ecdsa-sha2-nistp256-cert-v01@openssh.com": false,
		"ssh-xmss@openssh.com":                     false,
		"":                                         false,
	}
	for typ, want := range tests {
		if got := isAuthorizedKeyType(typ); got != want {
			t.Errorf("isAuthorizedKeyType(%q) = %v, want %v", typ, got, want)
		}
	}
}

func TestAuthorizedKeyPresent(t *testing.T) {
	keyA, lineA := sshTestKey(t, "ssh-ed25519")
	keyB, lineB := sshTestKey(t, "ssh-ed25519")
	bareA := strings.TrimSpace(string(ssh.MarshalAuthorizedKey(keyA)))

	tests := []struct {
		name string
		data string
		want bool
	}{
		{name: "empty", data: "", want: false},
		{name: "bare", data: lineA + "\n", want: true},
		{name: "other comment", data: sshTestComment(t, lineA, "elsewhere") + "\n", want: true},
		{name: "restricted from", data: `from="192.0.2.7" ` + lineA + " c\n", want: true},
		{name: "restricted command", data: `command="/bin/true",no-agent-forwarding ` + lineA + "\n", want: true},
		{name: "crlf restricted", data: `from="192.0.2.7" ` + lineA + "\r\n", want: true},
		{name: "different key", data: lineB + "\n", want: false},
		{name: "garbage", data: "not a key\n# comment\n", want: false},
		{name: "garbage then key", data: "not a key\n" + lineA, want: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := authorizedKeyPresent([]byte(tc.data), keyA); got != tc.want {
				t.Fatalf("authorizedKeyPresent = %v, want %v", got, tc.want)
			}
			if got := authorizedKeyPresent([]byte(tc.data), keyB); got == tc.want && tc.name != "different key" && tc.name != "garbage" && tc.name != "empty" {
				t.Fatalf("authorizedKeyPresent(keyB) = %v, want the opposite", got)
			}
		})
	}
	if !strings.Contains(bareA, "ssh-ed25519 ") {
		t.Fatalf("test key line is malformed: %q", bareA)
	}
}

func TestAppendAuthorizedKey(t *testing.T) {
	_, line := sshTestKey(t, "ssh-ed25519")
	tests := []struct {
		name     string
		existing string
		want     string
	}{
		{name: "absent", existing: "", want: line + "\n"},
		{name: "ends a line", existing: "# keep\n", want: "# keep\n" + line + "\n"},
		{name: "no trailing newline", existing: "garbage", want: "garbage\n" + line + "\n"},
		{name: "keeps every byte", existing: "# a\n\n  from=\"192.0.2.7\" ssh-ed25519 AAAA c\r\n", want: "# a\n\n  from=\"192.0.2.7\" ssh-ed25519 AAAA c\r\n" + line + "\n"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := string(appendAuthorizedKey([]byte(tc.existing), line)); got != tc.want {
				t.Fatalf("appendAuthorizedKey = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestReadSSHHostKey(t *testing.T) {
	_, hostLine := sshTestKey(t, "ssh-ed25519")
	_, rsaLine := sshTestKey(t, "ssh-rsa")
	// The image's bbraft-ssh-host-key service runs ssh-keygen -t ed25519 -C
	// bbraft-guest, so the trusted .pub is exactly this shape: type, base64,
	// the image comment and one trailing LF.
	keygenShape := sshTestComment(t, hostLine, "bbraft-guest") + "\n"
	if got := strings.Fields(keygenShape)[1]; len(got) != 68 {
		t.Fatalf("ed25519 base64 field is %d bytes, want the ssh-keygen shape (68)", len(got))
	}
	dir := t.TempDir()

	tests := []struct {
		name    string
		content string // written as a regular file; "" writes nothing
		setup   func(t *testing.T, path string)
		want    string
		wantErr codes.Code
		leak    string // a file byte sequence no error message may contain
	}{
		{name: "ssh-keygen shape", content: keygenShape, want: strings.TrimSpace(keygenShape)},
		{name: "bare", content: hostLine + "\n", want: hostLine},
		{name: "trailing whitespace", content: "  " + hostLine + "  \n", want: hostLine},
		{name: "unsafe control comment", content: sshTestComment(t, hostLine, "bad\x01comment") + "\n", want: hostLine},
		{name: "unsafe non-ascii comment", content: sshTestComment(t, hostLine, "café") + "\n", want: hostLine},
		{name: "missing", wantErr: codes.Unavailable},
		{name: "empty", content: "", wantErr: codes.Unavailable},
		{name: "garbage", content: "GARBAGE-HOSTFILE-MARKER not a key\n", wantErr: codes.Unavailable, leak: "GARBAGE-HOSTFILE-MARKER"},
		{name: "leading comment then key", content: "# generated by ssh-keygen\n" + hostLine + "\n", wantErr: codes.Unavailable, leak: "generated by ssh-keygen"},
		{name: "leading garbage then key", content: "GARBAGE-LEAD-MARKER not a key\n" + hostLine + "\n", wantErr: codes.Unavailable, leak: "GARBAGE-LEAD-MARKER"},
		{name: "blank lines around the key", content: "\n\n" + hostLine + "\n\n", want: hostLine},
		{name: "truncated", content: hostLine[:40], wantErr: codes.Unavailable, leak: strings.Fields(hostLine)[1][:12]},
		{name: "rsa", content: rsaLine + "\n", wantErr: codes.Unavailable},
		{name: "options", content: `from="192.0.2.7" ` + hostLine + "\n", wantErr: codes.Unavailable},
		{name: "two keys", content: hostLine + "\n" + hostLine + "\n", wantErr: codes.Unavailable},
		{name: "oversized", content: strings.Repeat("#", maxSSHHostKeyBytes+1), wantErr: codes.Internal},
		{
			name:    "symlink",
			wantErr: codes.Internal,
			setup: func(t *testing.T, path string) {
				target := filepath.Join(dir, "target.pub")
				if err := os.WriteFile(target, []byte(keygenShape), 0o644); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(target, path); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name:    "directory",
			wantErr: codes.Internal,
			setup: func(t *testing.T, path string) {
				t.Helper()
				if err := os.Mkdir(path, 0o755); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name:    "fifo",
			wantErr: codes.Internal,
			setup: func(t *testing.T, path string) {
				t.Helper()
				if err := syscall.Mkfifo(path, 0o600); err != nil {
					t.Fatal(err)
				}
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(dir, strings.ReplaceAll(tc.name, " ", "-")+".pub")
			if tc.setup != nil {
				tc.setup(t, path)
			} else if tc.content != "" || tc.wantErr == codes.Unavailable && tc.name == "empty" {
				if err := os.WriteFile(path, []byte(tc.content), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			got, err := readSSHHostKey(path)
			if tc.wantErr != codes.OK {
				if status.Code(err) != tc.wantErr {
					t.Fatalf("readSSHHostKey(%s) = %q, %v; want code %v", tc.name, got, err, tc.wantErr)
				}
				if got != "" {
					t.Fatalf("readSSHHostKey(%s) returned %q with an error", tc.name, got)
				}
				if tc.leak != "" && strings.Contains(err.Error(), tc.leak) {
					t.Fatalf("readSSHHostKey(%s) error echoes file content: %v", tc.name, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("readSSHHostKey(%s): %v", tc.name, err)
			}
			if got != tc.want {
				t.Fatalf("readSSHHostKey(%s) = %q, want %q", tc.name, got, tc.want)
			}
		})
	}
}

func TestSSHDirAndAuthorizedKeysSpec(t *testing.T) {
	if sshDirSpec != homeSpec+"/.ssh" {
		t.Fatalf("sshDirSpec = %q", sshDirSpec)
	}
	if sshAuthorizedKeysSpec != sshDirSpec+"/authorized_keys" {
		t.Fatalf("sshAuthorizedKeysSpec = %q", sshAuthorizedKeysSpec)
	}
	if sshDirMode != 0o700 || sshAuthorizedKeysMode != 0o600 {
		t.Fatalf("modes = %#o/%#o, want 0700/0600", sshDirMode, sshAuthorizedKeysMode)
	}
}

// sshFakeOps is a fileOps that answers only what a test stages (the .ssh
// directory, the authorized_keys file, reads, mkdir/chmod and the upload) and
// panics on the rest, so an unexpected dependency fails loudly instead of
// passing silently.
type sshFakeOps struct {
	dir       *nodev1.FileInfo // returned for .ssh; nil: a 0700 directory
	dirErr    error
	mkdirInfo *nodev1.FileInfo // returned by makeDir; nil: the .ssh info
	mkdirErr  error
	chmodErr  error
	file      *nodev1.FileInfo // returned for authorized_keys; nil: 0600 regular
	fileErr   error
	readData  []byte
	readInfo  bool // report an info frame before data
	readErr   error

	upload    fileUpload
	uploadErr error

	statCalls   int
	mkdirCalls  int
	chmodCalls  int
	uploadCalls int
}

func (f *sshFakeOps) statFile(_ context.Context, path string) (*nodev1.FileInfo, error) {
	f.statCalls++
	if path == sshDirSpec {
		if f.dir != nil || f.dirErr != nil {
			return f.dir, f.dirErr
		}
		return &nodev1.FileInfo{Type: nodev1.FileType_FILE_TYPE_DIRECTORY, Mode: sshDirMode}, nil
	}
	if f.file != nil || f.fileErr != nil {
		return f.file, f.fileErr
	}
	return &nodev1.FileInfo{Type: nodev1.FileType_FILE_TYPE_REGULAR, Mode: sshAuthorizedKeysMode}, nil
}

func (f *sshFakeOps) makeDir(context.Context, string, uint32) (*nodev1.FileInfo, error) {
	f.mkdirCalls++
	if f.mkdirInfo != nil || f.mkdirErr != nil {
		return f.mkdirInfo, f.mkdirErr
	}
	return &nodev1.FileInfo{Type: nodev1.FileType_FILE_TYPE_DIRECTORY, Mode: sshDirMode}, nil
}

func (f *sshFakeOps) chmodFile(_ context.Context, path string, _ uint32) (*nodev1.FileInfo, error) {
	f.chmodCalls++
	if f.chmodErr != nil {
		return nil, f.chmodErr
	}
	return &nodev1.FileInfo{Path: path, Type: nodev1.FileType_FILE_TYPE_DIRECTORY, Mode: sshDirMode}, nil
}

func (f *sshFakeOps) readFile(_ context.Context, _ string, _ int64, send func(*nodev1.ReadFileResponse) error) error {
	if f.readErr != nil {
		return f.readErr
	}
	if f.readInfo {
		info := f.file
		if info == nil {
			info = &nodev1.FileInfo{Type: nodev1.FileType_FILE_TYPE_REGULAR, Mode: sshAuthorizedKeysMode}
		}
		if err := send(&nodev1.ReadFileResponse{Part: &nodev1.ReadFileResponse_Info{Info: info}}); err != nil {
			return err
		}
	}
	if len(f.readData) > 0 {
		return send(&nodev1.ReadFileResponse{Part: &nodev1.ReadFileResponse_Data{Data: f.readData}})
	}
	return nil
}

func (f *sshFakeOps) newUpload(context.Context, string, uint32) (fileUpload, error) {
	f.uploadCalls++
	if f.upload != nil || f.uploadErr != nil {
		return f.upload, f.uploadErr
	}
	return &sshFakeUpload{}, nil
}

func (f *sshFakeOps) listFiles(context.Context, string, uint32) (*nodev1.ListFilesResponse, error) {
	panic("unexpected listFiles")
}

func (f *sshFakeOps) deleteFile(context.Context, string, bool) (*nodev1.DeleteFileResponse, error) {
	panic("unexpected deleteFile")
}

func (f *sshFakeOps) moveFile(context.Context, string, string, bool) (*nodev1.MoveFileResponse, error) {
	panic("unexpected moveFile")
}

// sshFakeUpload records the write/commit/abort calls a test triggers.
type sshFakeUpload struct {
	writeErr  error
	commitErr error
	writes    int
	aborts    int
}

func (u *sshFakeUpload) write([]byte) error {
	u.writes++
	return u.writeErr
}

func (u *sshFakeUpload) commit(context.Context) (*nodev1.PutFileResponse, error) {
	return &nodev1.PutFileResponse{}, u.commitErr
}

func (u *sshFakeUpload) abort(context.Context) error {
	u.aborts++
	return nil
}

func TestWriteAuthorizedKeysErrorPaths(t *testing.T) {
	boom := status.Error(codes.Internal, "boom")
	svc := func(ops fileOps) *Service {
		return &Service{log: slog.New(slog.DiscardHandler), files: ops}
	}
	content := []byte("ssh-ed25519 AAAA test\n")

	t.Run("upload start fails", func(t *testing.T) {
		ops := &sshFakeOps{uploadErr: boom}
		if err := svc(ops).writeAuthorizedKeys(t.Context(), content); status.Code(err) != codes.Internal {
			t.Fatalf("writeAuthorizedKeys = %v, want the start error", err)
		}
	})

	t.Run("write fails and aborts", func(t *testing.T) {
		up := &sshFakeUpload{writeErr: boom}
		ops := &sshFakeOps{upload: up}
		if err := svc(ops).writeAuthorizedKeys(t.Context(), content); status.Code(err) != codes.Internal {
			t.Fatalf("writeAuthorizedKeys = %v, want the write error", err)
		}
		if up.writes != 1 || up.aborts != 1 {
			t.Fatalf("write=%d abort=%d, want 1/1", up.writes, up.aborts)
		}
	})

	t.Run("commit fails and aborts", func(t *testing.T) {
		up := &sshFakeUpload{commitErr: boom}
		ops := &sshFakeOps{upload: up}
		if err := svc(ops).writeAuthorizedKeys(t.Context(), content); status.Code(err) != codes.Internal {
			t.Fatalf("writeAuthorizedKeys = %v, want the commit error", err)
		}
		if up.aborts != 1 {
			t.Fatalf("abort=%d, want 1", up.aborts)
		}
	})

	t.Run("commit succeeds without abort", func(t *testing.T) {
		up := &sshFakeUpload{}
		ops := &sshFakeOps{upload: up}
		if err := svc(ops).writeAuthorizedKeys(t.Context(), content); err != nil {
			t.Fatalf("writeAuthorizedKeys: %v", err)
		}
		if up.writes != 1 || up.aborts != 0 {
			t.Fatalf("write=%d abort=%d, want 1/0", up.writes, up.aborts)
		}
	})
}

func TestInstallAuthorizedKeyErrorPaths(t *testing.T) {
	boom := status.Error(codes.Internal, "boom")
	key, _ := sshTestKey(t, "ssh-ed25519")

	t.Run("missing ssh dir and mkdir fails", func(t *testing.T) {
		ops := &sshFakeOps{dirErr: status.Error(codes.NotFound, "missing"), mkdirErr: boom}
		svc := &Service{log: slog.New(slog.DiscardHandler), files: ops}
		if err := svc.installAuthorizedKey(t.Context(), "line", key); status.Code(err) != codes.Internal {
			t.Fatalf("installAuthorizedKey = %v, want the mkdir error", err)
		}
	})

	t.Run("read without info", func(t *testing.T) {
		ops := &sshFakeOps{readData: []byte("data"), readInfo: false}
		svc := &Service{log: slog.New(slog.DiscardHandler), files: ops}
		_, _, err := svc.readAuthorizedKeys(t.Context())
		if status.Code(err) != codes.Internal {
			t.Fatalf("readAuthorizedKeys = %v, want Internal", err)
		}
	})
}

// newSSHTestService builds a Service around a staged fileOps and the host key
// path, so unit tests can drive AddSSHKey without a real filesystem fixture.
func newSSHTestService(ops fileOps, hostKeyPath string) *Service {
	return &Service{
		log:   slog.New(slog.DiscardHandler),
		files: ops,
		ssh:   sshInstaller{gate: newSSHGate(), hostKeyFile: hostKeyPath},
	}
}

// TestEnsureSSHDirUsesMkdirResult covers the concurrent-creator branch: when
// statFile says .ssh is missing, makeDir may return an entry another process
// just created. That entry must go through the same symlink/type/mode checks
// and be tightened, never blindly accepted.
func TestEnsureSSHDirUsesMkdirResult(t *testing.T) {
	missing := status.Error(codes.NotFound, "missing")

	t.Run("existing 0755 directory is tightened", func(t *testing.T) {
		ops := &sshFakeOps{dirErr: missing, mkdirInfo: &nodev1.FileInfo{
			Type: nodev1.FileType_FILE_TYPE_DIRECTORY,
			Mode: 0o755,
		}}
		svc := newSSHTestService(ops, "")
		if err := svc.ensureSSHDir(t.Context()); err != nil {
			t.Fatalf("ensureSSHDir: %v", err)
		}
		if ops.mkdirCalls != 1 || ops.chmodCalls != 1 {
			t.Fatalf("mkdir=%d chmod=%d, want 1/1", ops.mkdirCalls, ops.chmodCalls)
		}
	})

	t.Run("existing symlink is rejected", func(t *testing.T) {
		ops := &sshFakeOps{dirErr: missing, mkdirInfo: &nodev1.FileInfo{
			Type:      nodev1.FileType_FILE_TYPE_DIRECTORY,
			Mode:      sshDirMode,
			IsSymlink: true,
		}}
		svc := newSSHTestService(ops, "")
		err := svc.ensureSSHDir(t.Context())
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("ensureSSHDir = %v, want InvalidArgument", err)
		}
		if ops.chmodCalls != 0 {
			t.Fatalf("chmod calls = %d, want 0", ops.chmodCalls)
		}
	})

	t.Run("existing non-directory is rejected", func(t *testing.T) {
		ops := &sshFakeOps{dirErr: missing, mkdirInfo: &nodev1.FileInfo{
			Type: nodev1.FileType_FILE_TYPE_REGULAR,
			Mode: sshDirMode,
		}}
		svc := newSSHTestService(ops, "")
		err := svc.ensureSSHDir(t.Context())
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("ensureSSHDir = %v, want InvalidArgument", err)
		}
	})
}

// TestInstallAuthorizedKeyTotalBound covers the total-size bound: a rewrite
// may never grow authorized_keys past maxSSHAuthorizedKeysBytes, and the
// refusal happens before the upload starts so no staged file or rename can
// happen. A duplicate at the exact bound still succeeds untouched.
func TestInstallAuthorizedKeyTotalBound(t *testing.T) {
	key, line := sshTestKey(t, "ssh-ed25519")
	limit := maxSSHAuthorizedKeysBytes

	t.Run("duplicate at the exact bound succeeds", func(t *testing.T) {
		padding := strings.Repeat("#", limit-len(line)-2)
		content := padding + "\n" + line + "\n"
		if len(content) != limit {
			t.Fatalf("fixture is %d bytes, want exactly %d", len(content), limit)
		}
		ops := &sshFakeOps{readData: []byte(content), readInfo: true}
		svc := newSSHTestService(ops, "")
		if err := svc.installAuthorizedKey(t.Context(), line, key); err != nil {
			t.Fatalf("installAuthorizedKey at the bound: %v", err)
		}
		if ops.uploadCalls != 0 {
			t.Fatalf("upload calls = %d, want 0 for an already-authorized key", ops.uploadCalls)
		}
	})

	t.Run("overflowing append is refused before the upload", func(t *testing.T) {
		// No key inside, one byte short of the bound: appending a line would
		// cross it.
		ops := &sshFakeOps{readData: []byte(strings.Repeat("#", limit-1)), readInfo: true}
		svc := newSSHTestService(ops, "")
		err := svc.installAuthorizedKey(t.Context(), line, key)
		if status.Code(err) != codes.OutOfRange {
			t.Fatalf("installAuthorizedKey = %v, want OutOfRange", err)
		}
		if ops.uploadCalls != 0 {
			t.Fatalf("upload calls = %d, want 0: the bound must be checked before newUpload", ops.uploadCalls)
		}
	})
}

func TestSSHGateUninitialized(t *testing.T) {
	var gate sshGate
	if code := status.Code(gate.acquire(t.Context())); code != codes.Internal {
		t.Fatalf("acquire on an uninitialized gate = %v, want Internal", code)
	}
}

// sshGateOps stages the first AddSSHKey owner inside statFile and counts every
// file operation, so a waiting caller's cancellation can be proven to happen
// without the waiter reaching the filesystem.
type sshGateOps struct {
	mu          sync.Mutex
	calls       map[string]int
	block       bool
	entered     chan struct{}
	release     chan struct{}
	releaseOnce sync.Once
}

// releaseOwner unblocks a staged owner exactly once; it is safe to call from
// a failure cleanup as well as from the test body.
func (o *sshGateOps) releaseOwner() {
	o.releaseOnce.Do(func() { close(o.release) })
}

func (o *sshGateOps) enter(name string) {
	o.mu.Lock()
	o.calls[name]++
	block := o.block && name == "stat" && o.calls[name] == 1
	o.mu.Unlock()
	if block {
		close(o.entered)
		<-o.release
	}
}

func (o *sshGateOps) count(name string) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.calls[name]
}

func (o *sshGateOps) unblock() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.block = false
}

func (o *sshGateOps) statFile(_ context.Context, path string) (*nodev1.FileInfo, error) {
	o.enter("stat")
	if path == sshDirSpec {
		return &nodev1.FileInfo{Type: nodev1.FileType_FILE_TYPE_DIRECTORY, Mode: sshDirMode}, nil
	}
	return nil, status.Error(codes.NotFound, "no authorized_keys")
}

func (o *sshGateOps) readFile(context.Context, string, int64, func(*nodev1.ReadFileResponse) error) error {
	o.enter("read")
	return status.Error(codes.NotFound, "no authorized_keys")
}

func (o *sshGateOps) newUpload(context.Context, string, uint32) (fileUpload, error) {
	o.enter("upload")
	return &sshFakeUpload{}, nil
}

func (o *sshGateOps) makeDir(context.Context, string, uint32) (*nodev1.FileInfo, error) {
	o.enter("mkdir")
	return &nodev1.FileInfo{Type: nodev1.FileType_FILE_TYPE_DIRECTORY, Mode: sshDirMode}, nil
}

func (o *sshGateOps) chmodFile(context.Context, string, uint32) (*nodev1.FileInfo, error) {
	o.enter("chmod")
	return &nodev1.FileInfo{Type: nodev1.FileType_FILE_TYPE_DIRECTORY, Mode: sshDirMode}, nil
}

func (o *sshGateOps) listFiles(context.Context, string, uint32) (*nodev1.ListFilesResponse, error) {
	panic("unexpected listFiles")
}

func (o *sshGateOps) deleteFile(context.Context, string, bool) (*nodev1.DeleteFileResponse, error) {
	panic("unexpected deleteFile")
}

func (o *sshGateOps) moveFile(context.Context, string, string, bool) (*nodev1.MoveFileResponse, error) {
	panic("unexpected moveFile")
}

// TestAddSSHKeyCanceledWaiterDoesNotBlock proves a waiting AddSSHKey gives up
// as soon as its context ends: a canceled second caller returns Canceled while
// the first owner is still staged inside a file operation, performs no file
// operation itself, and the gate is released afterwards so a later caller
// installs normally.
func TestAddSSHKeyCanceledWaiterDoesNotBlock(t *testing.T) {
	hostKeyPath := filepath.Join(t.TempDir(), "ssh_host_ed25519_key.pub")
	_, hostLine := sshTestKey(t, "ssh-ed25519")
	if err := os.WriteFile(hostKeyPath, []byte(hostLine+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, line := sshTestKey(t, "ssh-ed25519")
	ops := &sshGateOps{
		calls:   map[string]int{},
		block:   true,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	svc := newSSHTestService(ops, hostKeyPath)
	// A red run must never leave the staged owner blocked: cleanup releases it
	// even when a t.Fatal below returns early.
	t.Cleanup(ops.releaseOwner)

	owner := make(chan error, 1)
	go func() {
		_, err := svc.AddSSHKey(context.Background(), &guestv1.AddSSHKeyRequest{PublicKey: line})
		owner <- err
	}()
	select {
	case <-ops.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the gate owner never reached the staged file operation")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	waiter := make(chan error, 1)
	go func() {
		_, err := svc.AddSSHKey(ctx, &guestv1.AddSSHKeyRequest{PublicKey: line})
		waiter <- err
	}()
	select {
	case err := <-waiter:
		if status.Code(err) != codes.Canceled {
			t.Fatalf("canceled waiter = %v, want Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a canceled waiter stayed blocked behind the SSH gate owner")
	}
	if got := ops.count("stat"); got != 1 {
		t.Fatalf("waiter reached the filesystem: %d stat calls while the owner is staged, want 1", got)
	}

	ops.releaseOwner()
	select {
	case err := <-owner:
		if err != nil {
			t.Fatalf("owner AddSSHKey: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the gate owner did not finish")
	}

	ops.unblock()
	if _, err := svc.AddSSHKey(t.Context(), &guestv1.AddSSHKeyRequest{PublicKey: line}); err != nil {
		t.Fatalf("AddSSHKey after the gate was released: %v", err)
	}
}

// sshTestGateContext stages cancellation at the gate's post-token recheck. It
// wraps a real cancellable context and triggers that cancellation on the
// second Err call, so Done and Err stay consistent with the context contract
// while the first check (before the token) still sees a live context.
type sshTestGateContext struct {
	context.Context
	cancel context.CancelFunc
	errs   int
}

func (c *sshTestGateContext) Err() error {
	c.errs++
	if c.errs >= 2 {
		c.cancel()
	}
	return c.Context.Err()
}

// TestSSHGateAcquire covers the cancellation boundaries of the gate: an
// already-canceled caller must never take a free token (with both select cases
// ready the choice is random), and a cancellation that lands after the token
// was taken must return it. A live caller must still acquire afterwards.
func TestSSHGateAcquire(t *testing.T) {
	canceled, cancel := context.WithCancel(t.Context())
	cancel()

	t.Run("already canceled does not take a free token", func(t *testing.T) {
		for i := range 20 {
			gate := newSSHGate()
			if code := status.Code(gate.acquire(canceled)); code != codes.Canceled {
				t.Fatalf("attempt %d: acquire = %v, want Canceled: a canceled caller took the free token", i, code)
			}
			if err := gate.acquire(t.Context()); err != nil {
				t.Fatalf("attempt %d: a live caller cannot acquire after a canceled one: %v", i, err)
			}
			gate.release()
		}
	})

	t.Run("cancellation after taking the token returns it", func(t *testing.T) {
		gate := newSSHGate()
		base, cancel := context.WithCancel(context.Background())
		defer cancel()
		ctx := &sshTestGateContext{Context: base, cancel: cancel}
		if code := status.Code(gate.acquire(ctx)); code != codes.Canceled {
			t.Fatalf("acquire = %v, want Canceled", code)
		}
		if err := ctx.Err(); err == nil {
			t.Fatal("the staged context is not canceled after the acquire")
		}
		if err := gate.acquire(t.Context()); err != nil {
			t.Fatalf("the token was not returned: %v", err)
		}
		gate.release()
	})

	t.Run("canceled AddSSHKey with a free gate touches no file operation", func(t *testing.T) {
		hostKeyPath := filepath.Join(t.TempDir(), "ssh_host_ed25519_key.pub")
		_, hostLine := sshTestKey(t, "ssh-ed25519")
		if err := os.WriteFile(hostKeyPath, []byte(hostLine+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		_, line := sshTestKey(t, "ssh-ed25519")
		ops := &sshGateOps{calls: map[string]int{}}
		svc := newSSHTestService(ops, hostKeyPath)
		if _, err := svc.AddSSHKey(canceled, &guestv1.AddSSHKeyRequest{PublicKey: line}); status.Code(err) != codes.Canceled {
			t.Fatalf("AddSSHKey = %v, want Canceled", err)
		}
		for _, name := range []string{"stat", "read", "upload", "mkdir", "chmod"} {
			if got := ops.count(name); got != 0 {
				t.Fatalf("canceled caller performed %d %s operations, want 0", got, name)
			}
		}
		if _, err := svc.AddSSHKey(t.Context(), &guestv1.AddSSHKeyRequest{PublicKey: line}); err != nil {
			t.Fatalf("a live caller after the canceled one: %v", err)
		}
	})
}
