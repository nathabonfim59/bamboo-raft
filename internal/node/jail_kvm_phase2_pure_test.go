package node

import (
	"io"
	"net"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/objstore/s3"
)

// jailKVMS3Unit is the S3 part of a role-rendered unit (the lines the
// template emits when bbraft_node_s3 is set) around a minimal ExecStart.
const jailKVMS3Unit = `[Service]
ExecStart="/var/lib/bbraft-kt-stage-a1/bin/bbraft-node" \
    -server.addr "127.0.0.1:7701" \
    -node.chunk-cache-bytes "8589934592" \
    -node.jail-io-device "8:0"

EnvironmentFile=/etc/bbraft-kt-a1/s3.env
LoadCredential=s3-secret-access-key:/etc/bbraft-kt-a1/s3-secret-access-key
Environment=BBRAFT_S3_SECRET_ACCESS_KEY_FILE=%d/s3-secret-access-key
Restart=on-failure
`

func TestJailKVMUnitS3Files(t *testing.T) {
	const config = "/etc/bbraft-kt-a1"
	parse := func(text string) jailKVMUnit {
		t.Helper()
		u, err := jailKVMParseUnit(text)
		if err != nil {
			t.Fatal(err)
		}
		return u
	}
	got, err := jailKVMUnitS3Files(parse(jailKVMS3Unit), config)
	if err != nil || got != (jailKVMS3Files{EnvFile: config + "/s3.env", SecretFile: config + "/s3-secret-access-key"}) {
		t.Fatalf("rendered unit = %+v, %v", got, err)
	}
	edits := map[string][2]string{
		"no object store":     {"EnvironmentFile=/etc/bbraft-kt-a1/s3.env\n", ""},
		"no credential":       {"LoadCredential=s3-secret-access-key:/etc/bbraft-kt-a1/s3-secret-access-key\n", ""},
		"no credential var":   {"Environment=BBRAFT_S3_SECRET_ACCESS_KEY_FILE=%d/s3-secret-access-key\n", ""},
		"credential var path": {"%d/s3-secret-access-key", "/etc/bbraft-kt-a1/s3-secret-access-key"},
		"no chunk cache flag": {"    -node.chunk-cache-bytes \"8589934592\" \\\n", ""},
		"env file elsewhere":  {"/etc/bbraft-kt-a1/s3.env", "/etc/other/s3.env"},
		"env file nested":     {"/etc/bbraft-kt-a1/s3.env", "/etc/bbraft-kt-a1/sub/s3.env"},
		"secret elsewhere":    {"/etc/bbraft-kt-a1/s3-secret-access-key", "/root/key"},
		"secret is the env":   {":/etc/bbraft-kt-a1/s3-secret-access-key", ":/etc/bbraft-kt-a1/s3.env"},
		"relative env file":   {"=/etc/bbraft-kt-a1/s3.env", "=s3.env"},
		// The role bug this harness once missed: systemd does not unquote
		// these two settings, so a quoted path is ignored and the node starts
		// without its object store.
		"quoted env file":       {"EnvironmentFile=/etc/bbraft-kt-a1/s3.env", "EnvironmentFile=\"/etc/bbraft-kt-a1/s3.env\""},
		"quoted credential":     {"s3-secret-access-key:/etc/bbraft-kt-a1/s3-secret-access-key", "s3-secret-access-key:\"/etc/bbraft-kt-a1/s3-secret-access-key\""},
		"unclean path":          {"/etc/bbraft-kt-a1/s3.env", "/etc/bbraft-kt-a1/../bbraft-kt-a1/s3.env"},
		"other credential only": {"LoadCredential=s3-secret-access-key:", "LoadCredential=other:"},
	}
	for name, e := range edits {
		t.Run(name, func(t *testing.T) {
			text := strings.Replace(jailKVMS3Unit, e[0], e[1], 1)
			if text == jailKVMS3Unit {
				t.Fatal("the edit changed nothing")
			}
			if files, err := jailKVMUnitS3Files(parse(text), config); err == nil {
				t.Fatalf("accepted: %+v", files)
			}
		})
	}
}

func TestJailKVMS3EnvContent(t *testing.T) {
	cfg := s3.Config{Region: "garage", Bucket: "bbrafttest", PathStyle: true, AccessKeyID: "GK0123"}
	got, err := jailKVMS3EnvContent(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"BBRAFT_S3_ENDPOINT=http://" + jailKVMS3Addr + "\n", "BBRAFT_S3_REGION=garage\n", "BBRAFT_S3_BUCKET=bbrafttest\n",
		"BBRAFT_S3_PATH_STYLE=true\n", "BBRAFT_S3_ACCESS_KEY_ID=GK0123\n",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("env file lacks %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "SECRET") {
		t.Errorf("env file mentions the secret:\n%s", got)
	}
	for name, bad := range map[string]s3.Config{
		"empty region":   {Bucket: "b", AccessKeyID: "k"},
		"newline bucket": {Region: "r", Bucket: "b\nBBRAFT_S3_ENDPOINT=x", AccessKeyID: "k"},
		"space key":      {Region: "r", Bucket: "b", AccessKeyID: "k k"},
	} {
		if _, err := jailKVMS3EnvContent(bad); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestJailKVMCheckMaintenanceArgv(t *testing.T) {
	const id = "bx_cccccccc"
	good := []string{"/usr/bin/qemu-system-x86_64", "-name", "guest=" + id + ",process=" + id + "-maint", "-machine", "q35,accel=kvm", "-nodefaults", "-S",
		"-m", "256M", "-qmp", "unix:/control/qmp.sock,server=on,wait=off"}
	if err := jailKVMCheckMaintenanceArgv(good, id, 256); err != nil {
		t.Fatalf("maintenance argv refused: %v", err)
	}
	with := func(extra ...string) []string { return append(slices.Clone(good), extra...) }
	for name, c := range map[string]struct {
		args []string
		mem  uint64
	}{
		"a device":         {with("-device", "virtio-net-pci,netdev=net0"), 256},
		"a netdev":         {with("-netdev", "tap,id=net0"), 256},
		"a vsock object":   {with("-object", "rng-random,id=r"), 256},
		"not paused":       {slices.DeleteFunc(slices.Clone(good), func(a string) bool { return a == "-S" }), 256},
		"default devices":  {slices.DeleteFunc(slices.Clone(good), func(a string) bool { return a == "-nodefaults" }), 256},
		"VM name":          {slices.Replace(slices.Clone(good), 2, 3, "guest="+id), 256},
		"other VM":         {good, 256}, // checked below against another id
		"memory too large": {good, 128},
		"no memory":        {slices.DeleteFunc(slices.Clone(good), func(a string) bool { return a == "-m" || a == "256M" }), 256},
	} {
		wantID := id
		if name == "other VM" {
			wantID = "bx_dddddddd"
		}
		if err := jailKVMCheckMaintenanceArgv(c.args, wantID, c.mem); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	// A VM smaller than 256 MiB gets its own memory, not 256.
	small := slices.Replace(slices.Clone(good), 8, 9, "128M")
	if err := jailKVMCheckMaintenanceArgv(small, id, 128); err != nil {
		t.Errorf("128 MiB session refused: %v", err)
	}
}

func TestJailKVMCheckMaintenanceFDs(t *testing.T) {
	const chr, sock = 0o020000, 0o140000
	tun, vsock := uint64(10<<8|200), uint64(10<<8|241)
	forbidden := map[string]uint64{"/dev/net/tun": tun, "/dev/vhost-vsock": vsock}
	unixSockets := jailKVMUnixInodes("Num       RefCount Protocol Flags    Type St Inode Path\n" +
		"0000000000000000: 00000002 00000000 00010000 0001 01 777 /control/qmp.sock\n" +
		"0000000000000000: 00000003 00000000 00000000 0001 03 778\n")
	if !unixSockets[777] || !unixSockets[778] || len(unixSockets) != 2 {
		t.Fatalf("unix inodes = %v", unixSockets)
	}
	kvm := jailKVMFD{Num: 9, Link: "/dev/kvm", Mode: chr | 0o600, Rdev: 10<<8 | 232}
	qmp := jailKVMFD{Num: 11, Link: "socket:[777]", Mode: sock | 0o777, ID: jailKVMIdentity{Dev: 8, Ino: 777}}
	if err := jailKVMCheckMaintenanceFDs([]jailKVMFD{kvm, qmp}, forbidden, unixSockets); err != nil {
		t.Fatalf("a QMP socket and /dev/kvm refused: %v", err)
	}
	for name, fd := range map[string]jailKVMFD{
		"tap":          {Num: 3, Link: "/dev/net/tun", Mode: chr | 0o600, Rdev: tun},
		"vsock device": {Num: 4, Link: "/dev/vhost-vsock", Mode: chr | 0o600, Rdev: vsock},
		"tcp socket":   {Num: 5, Link: "socket:[900]", Mode: sock | 0o777, ID: jailKVMIdentity{Dev: 8, Ino: 900}},
	} {
		err := jailKVMCheckMaintenanceFDs([]jailKVMFD{kvm, qmp, fd}, forbidden, unixSockets)
		if err == nil || !strings.Contains(err.Error(), "fd "+strconv.Itoa(fd.Num)) {
			t.Errorf("%s: %v, want an error naming the descriptor", name, err)
		}
	}
}

// TestJailKVMForwarderHold: the forwarder pipes bytes both ways, Hold keeps a
// request back until Release, and Close drops everything and joins.
func TestJailKVMForwarderHold(t *testing.T) {
	echo, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer echo.Close()
	go func() {
		for {
			c, err := echo.Accept()
			if err != nil {
				return
			}
			go func() { defer c.Close(); _, _ = io.Copy(c, c) }()
		}
	}()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := &jailKVMForwarder{upstream: echo.Addr().String(), lis: lis, conns: map[net.Conn]struct{}{}, done: make(chan struct{})}
	f.wg.Go(f.accept)

	c, err := net.Dial("tcp", lis.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	roundTrip := func(msg string, wait time.Duration) (string, error) {
		if _, err := c.Write([]byte(msg)); err != nil {
			return "", err
		}
		buf := make([]byte, len(msg))
		_ = c.SetReadDeadline(time.Now().Add(wait))
		_, err := io.ReadFull(c, buf)
		return string(buf), err
	}
	if got, err := roundTrip("ping", 5*time.Second); err != nil || got != "ping" {
		t.Fatalf("through the forwarder: %q, %v", got, err)
	}
	f.Hold()
	if got, err := roundTrip("held", 300*time.Millisecond); err == nil {
		t.Fatalf("a request passed while held: %q", got)
	}
	f.Release()
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	buf := make([]byte, len("held"))
	if _, err := io.ReadFull(c, buf); err != nil || string(buf) != "held" {
		t.Fatalf("after release: %q, %v", buf, err)
	}
	f.Hold()
	if err := f.Close(5 * time.Second); err != nil {
		t.Fatalf("Close while held: %v", err)
	}
	if err := f.Close(time.Second); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if _, err := net.DialTimeout("tcp", lis.Addr().String(), time.Second); err == nil {
		t.Fatal("the forwarder still listens after Close")
	}
}

// TestJailKVMDirectPlanChunkCache: the Direct helper takes the rendered chunk
// cache flag, defaults to bbraft-node's 8 GiB without it, and refuses a value
// the node would.
func TestJailKVMDirectPlanChunkCache(t *testing.T) {
	facts := jailKVMHostFacts{VCPUs: 8, MemoryMiB: 16384, DiskGiB: 100}
	plan := func(flags map[string]string) (jailKVMDirectSettings, error) {
		return jailKVMDirectPlan(jailKVMDirectConfig{Flags: flags, Socket: "/var/lib/bbraft-kt-a1/direct.sock"}, facts)
	}
	got, err := plan(jailKVMDirectRendered())
	if err != nil || got.ChunkCacheBytes != 8<<30 {
		t.Fatalf("no flag: %d, %v; want the 8 GiB default", got.ChunkCacheBytes, err)
	}
	flags := jailKVMDirectRendered()
	flags["-node.chunk-cache-bytes"] = "1073741824"
	if got, err := plan(flags); err != nil || got.ChunkCacheBytes != 1<<30 {
		t.Fatalf("flag 1 GiB: %d, %v", got.ChunkCacheBytes, err)
	}
	flags["-node.chunk-cache-bytes"] = "0"
	if got, err := plan(flags); err != nil || got.ChunkCacheBytes != 0 {
		t.Fatalf("flag 0 (cache off): %d, %v", got.ChunkCacheBytes, err)
	}
	for _, bad := range []string{"-1", "x", "9223372036854775808"} {
		flags["-node.chunk-cache-bytes"] = bad
		if _, err := plan(flags); err == nil {
			t.Errorf("chunk cache %q accepted", bad)
		}
	}
}

func TestJailKVMCheckS3Show(t *testing.T) {
	files := jailKVMS3Files{EnvFile: "/etc/bbraft-kt-a1/s3.env", SecretFile: "/etc/bbraft-kt-a1/s3-secret-access-key"}
	good := map[string]string{
		"EnvironmentFiles": "/etc/bbraft-kt-a1/s3.env (ignore_errors=no)",
		"LoadCredential":   "s3-secret-access-key:/etc/bbraft-kt-a1/s3-secret-access-key",
	}
	if err := jailKVMCheckS3Show(good, files); err != nil {
		t.Fatalf("loaded settings refused: %v", err)
	}
	// systemd 255 prints the a(ss) property as "[unprintable]": that half is
	// proven after the start (assertS3Credential), the env file half still is.
	unprintable := map[string]string{"EnvironmentFiles": good["EnvironmentFiles"], "LoadCredential": "[unprintable]"}
	if err := jailKVMCheckS3Show(unprintable, files); err != nil {
		t.Fatalf("the unprintable form refused: %v", err)
	}
	if err := jailKVMCheckS3Show(map[string]string{"EnvironmentFiles": "", "LoadCredential": "[unprintable]"}, files); err == nil ||
		!strings.Contains(err.Error(), "EnvironmentFile") {
		t.Fatalf("an ignored env file with the unprintable credential = %v, want the env file named", err)
	}
	// What systemd shows when it ignored both (a quoted path in the unit).
	if err := jailKVMCheckS3Show(map[string]string{"EnvironmentFiles": "", "LoadCredential": ""}, files); err == nil ||
		!strings.Contains(err.Error(), "EnvironmentFile") || !strings.Contains(err.Error(), "credential") {
		t.Fatalf("ignored settings = %v, want both named", err)
	}
	other := map[string]string{"EnvironmentFiles": good["EnvironmentFiles"], "LoadCredential": "other:/etc/x"}
	if err := jailKVMCheckS3Show(other, files); err == nil {
		t.Fatal("a different credential accepted")
	}
}

// TestJailKVMRedact: the node's journal tail the harness logs on a failure
// never carries the object store secret.
func TestJailKVMRedact(t *testing.T) {
	const secret = "SENTINEL-kvm-harness-s3-secret-77ab"
	journal := "node: object store configured endpoint=http://127.0.0.1:7702\nrequest failed: key " + secret + " rejected\nlast " + secret
	got := jailKVMRedact(journal, secret)
	if strings.Contains(got, secret) || strings.Count(got, "[redacted]") != 2 || !strings.Contains(got, "object store configured") {
		t.Fatalf("redacted journal:\n%s", got)
	}
	if got := jailKVMRedact(journal, ""); got != journal {
		t.Fatal("an empty secret must leave the text unchanged")
	}
	// The env file the harness writes never holds the secret either.
	env, err := jailKVMS3EnvContent(s3.Config{Region: "garage", Bucket: "b", AccessKeyID: "GK1", SecretAccessKey: secret})
	if err != nil || strings.Contains(env, secret) {
		t.Fatalf("env file: %v\n%s", err, env)
	}
}

func TestJailKVMCheckCredentialFile(t *testing.T) {
	const reg, dir = syscall.S_IFREG, syscall.S_IFDIR
	if err := jailKVMCheckCredentialFile(reg|0o400, 0, 0); err != nil {
		t.Fatalf("0400 root file refused: %v", err)
	}
	if err := jailKVMCheckCredentialFile(reg|0o440, 5, 5); err != nil {
		t.Fatalf("0440 file refused: %v", err)
	}
	for name, c := range map[string][3]uint32{
		"world readable": {reg | 0o444, 0, 0},
		"owner writable": {reg | 0o600, 0, 0},
		"other owner":    {reg | 0o400, 1000, 0},
		"a directory":    {dir | 0o400, 0, 0},
		"a symlink":      {syscall.S_IFLNK | 0o400, 0, 0},
	} {
		if err := jailKVMCheckCredentialFile(c[0], c[1], c[2]); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// TestJailKVMCheckCredentialEnviron: only BBRAFT_S3_SECRET_ACCESS_KEY_FILE is
// looked at; nothing else of the node's environment (which may hold other
// secrets) reaches an error.
func TestJailKVMCheckCredentialEnviron(t *testing.T) {
	const svc, sentinel = "bbraft-kt-a1", "SENTINEL-other-secret-in-environ-91c3"
	dir := jailKVMCredentialDir(svc)
	env := func(entries ...string) []byte { return []byte(strings.Join(entries, "\x00") + "\x00") }
	good := env("PATH=/usr/bin", "NODE_TOKEN_SECRET="+sentinel, "BBRAFT_S3_ENDPOINT=http://127.0.0.1:7702",
		"BBRAFT_S3_SECRET_ACCESS_KEY_FILE="+dir+"/s3-secret-access-key")
	if err := jailKVMCheckCredentialEnviron(good, svc); err != nil {
		t.Fatalf("a correct environment refused: %v", err)
	}
	bad := map[string][]byte{
		"variable missing":    env("PATH=/usr/bin", "OTHER="+sentinel),
		"other directory":     env("OTHER="+sentinel, "BBRAFT_S3_SECRET_ACCESS_KEY_FILE=/etc/bbraft/s3-secret-access-key"),
		"other file name":     env("OTHER="+sentinel, "BBRAFT_S3_SECRET_ACCESS_KEY_FILE="+dir+"/other"),
		"defined twice":       env("OTHER="+sentinel, "BBRAFT_S3_SECRET_ACCESS_KEY_FILE="+dir+"/s3-secret-access-key", "BBRAFT_S3_SECRET_ACCESS_KEY_FILE="+dir+"/s3-secret-access-key"),
		"other unit's dir":    env("OTHER="+sentinel, "BBRAFT_S3_SECRET_ACCESS_KEY_FILE="+jailKVMCredentialDir("bbraft-kt-b2")+"/s3-secret-access-key"),
		"secret value in env": env("OTHER="+sentinel, "BBRAFT_S3_SECRET_ACCESS_KEY=hunter2-value", "BBRAFT_S3_SECRET_ACCESS_KEY_FILE="+dir+"/s3-secret-access-key"),
		"empty":               nil,
	}
	for name, environ := range bad {
		err := jailKVMCheckCredentialEnviron(environ, svc)
		if err == nil {
			t.Errorf("%s: accepted", name)
			continue
		}
		for _, leaked := range []string{sentinel, "hunter2-value", "/etc/bbraft/s3-secret-access-key", "bbraft-kt-b2", "PATH="} {
			if strings.Contains(err.Error(), leaked) {
				t.Errorf("%s: the error leaks %q: %v", name, leaked, err)
			}
		}
	}
}
