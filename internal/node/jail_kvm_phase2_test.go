package node

// Phase 2 side of the A10 jailed harness (jail_kvm_test.go): the object store
// the node inside its network namespace reaches, the role-rendered S3
// settings of the Link and Direct units, and the inspection of a maintenance
// QEMU. Test-only; the privileged parts run only under BBRAFT_TEST_KVM_JAIL=1
// as root in a throwaway VM, the parsing and policy checks are pure and run
// in make check (jail_kvm_phase2_pure_test.go).
//
// How the node reaches the object store. The unit runs with
// NetworkNamespacePath=/run/netns/<nodeNS>, so it sees only that namespace's
// loopback. Garage (s3test.StartGarage, unchanged) listens on the host's
// loopback and the test reaches it directly. jailKVMForwarder listens on
// jailKVMS3Addr INSIDE the node namespace (the Hub's pattern:
// jailKVMListenInNetns) and pipes every connection to Garage's host address.
// The unit's S3 endpoint is that forwarder. The forwarder can also hold the
// node's requests, which keeps a maintenance session open while it is
// inspected (see inspectMaintenance).
//
// How the node gets its S3 settings. kvm-harness-render.yml renders the
// attempts listed in kt_s3_attempts (link, direct) with bbraft_node_s3, so the
// ROLE's unit template emits EnvironmentFile=, LoadCredential= and
// BBRAFT_S3_SECRET_ACCESS_KEY_FILE=. The harness writes the two files the
// rendered unit names (s3.env and the secret, both 0600, the role's own
// formats) before the unit starts; the production main and the Direct helper
// then read -s3.* from BBRAFT_S3_* exactly as on a node.

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/nathabonfim59/bamboo-raft/internal/ids"
	"github.com/nathabonfim59/bamboo-raft/internal/node/qemu"
	"github.com/nathabonfim59/bamboo-raft/internal/objstore"
	"github.com/nathabonfim59/bamboo-raft/internal/objstore/s3"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
	"uuid"
)

const (
	// jailKVMS3Addr is where the forwarder listens inside the node namespace.
	// It is the endpoint kt_s3 renders in kvm-harness-vars.yml.
	jailKVMS3Addr = "127.0.0.1:7702"
	// jailKVMS3Credential is the LoadCredential= name the role renders and the
	// secret file's name under the credentials directory.
	jailKVMS3Credential = "s3-secret-access-key"
)

// jailKVMS3Files are the two files the role-rendered unit reads its object
// store settings from, both under the attempt's config dir.
type jailKVMS3Files struct {
	EnvFile    string // EnvironmentFile=: the non-secret BBRAFT_S3_* settings
	SecretFile string // LoadCredential=s3-secret-access-key:<file>
}

// jailKVMUnitS3Files reads the object store wiring out of a rendered unit and
// proves it is the shape the role renders: one EnvironmentFile=, one
// LoadCredential=s3-secret-access-key:, the secret path variable pointing at
// the credentials directory, the chunk cache flag, and both files inside
// configDir (the harness writes nothing elsewhere). A unit without an object
// store is an error: it was not rendered for Phase 2.
func jailKVMUnitS3Files(u jailKVMUnit, configDir string) (jailKVMS3Files, error) {
	// systemd reads EnvironmentFile= and the LoadCredential= path verbatim: a
	// path in quotes is "not absolute" to it and the setting is ignored, so
	// the raw value must itself start with "/" (jailKVMSplitWords would hide
	// exactly that bug by unquoting). Only the %% specifier escape is undone.
	one := func(what string, values []string) (string, error) {
		if len(values) != 1 {
			return "", fmt.Errorf("the rendered unit has %d %s lines, want 1 (is this attempt in kt_s3_attempts of kvm-harness-vars.yml?)", len(values), what)
		}
		raw := strings.TrimSpace(values[0])
		if !strings.HasPrefix(raw, "/") {
			return "", fmt.Errorf("%s value %q is not an absolute path as systemd reads it (it does not unquote; the setting would be ignored)", what, raw)
		}
		return strings.ReplaceAll(raw, "%%", "%"), nil
	}
	inConfig := func(what, p string) error {
		if !filepath.IsAbs(p) || filepath.Clean(p) != p || filepath.Dir(p) != configDir {
			return fmt.Errorf("%s %q is not a file directly in the attempt's config dir %s", what, p, configDir)
		}
		return nil
	}
	var f jailKVMS3Files
	var err error
	if f.EnvFile, err = one("EnvironmentFile", u.Service["EnvironmentFile"]); err != nil {
		return f, err
	}
	var credentials []string
	for _, c := range u.Service["LoadCredential"] {
		if name, rest, ok := strings.Cut(strings.TrimSpace(c), ":"); ok && name == jailKVMS3Credential {
			credentials = append(credentials, rest)
		}
	}
	if f.SecretFile, err = one("LoadCredential="+jailKVMS3Credential, credentials); err != nil {
		return f, err
	}
	if err := errors.Join(inConfig("EnvironmentFile", f.EnvFile), inConfig("LoadCredential file", f.SecretFile)); err != nil {
		return f, err
	}
	if f.EnvFile == f.SecretFile {
		return f, errors.New("the object store env file and secret file are the same path")
	}
	wantVar := "BBRAFT_S3_SECRET_ACCESS_KEY_FILE=%d/" + jailKVMS3Credential
	if !slices.ContainsFunc(u.Service["Environment"], func(e string) bool { return strings.TrimSpace(e) == wantVar }) {
		return f, fmt.Errorf("the rendered unit lacks Environment=%s: the node would not find its credential", wantVar)
	}
	if !slices.Contains(u.ExecStart, "-node.chunk-cache-bytes") {
		return f, errors.New("the rendered ExecStart lacks -node.chunk-cache-bytes (rendered only when the object store is configured)")
	}
	return f, nil
}

// jailKVMS3EnvContent is the env file the role's files.yml writes for a remote
// S3 service, with the endpoint pointed at the in-namespace forwarder. A
// value that could break the file's one-line-per-variable shape is refused.
func jailKVMS3EnvContent(cfg s3.Config) (string, error) {
	for what, v := range map[string]string{"region": cfg.Region, "bucket": cfg.Bucket, "access key id": cfg.AccessKeyID} {
		if v == "" || strings.ContainsAny(v, " \t\r\n\x00\"'\\$#") {
			return "", fmt.Errorf("the object store %s %q cannot go into an env file", what, v)
		}
	}
	return "# Written by the jail KVM harness like roles/bbraft_node/tasks/files.yml.\n" +
		"BBRAFT_S3_ENDPOINT=http://" + jailKVMS3Addr + "\n" +
		"BBRAFT_S3_REGION=" + cfg.Region + "\n" +
		"BBRAFT_S3_BUCKET=" + cfg.Bucket + "\n" +
		"BBRAFT_S3_PATH_STYLE=" + strconv.FormatBool(cfg.PathStyle) + "\n" +
		"BBRAFT_S3_ACCESS_KEY_ID=" + cfg.AccessKeyID + "\n", nil
}

// writeS3 writes the object store settings the rendered unit names, before
// the unit starts. The secret is never logged or put in an error.
func (a *jailKVMAttempt) writeS3(t *testing.T, cfg s3.Config) {
	t.Helper()
	files, err := jailKVMUnitS3Files(a.unit, a.rendered.ConfigDir)
	if err != nil {
		t.Fatalf("attempt %s: %v", a.rendered.Service, err)
	}
	env, err := jailKVMS3EnvContent(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.SecretAccessKey == "" || strings.ContainsAny(cfg.SecretAccessKey, "\r\n\x00") {
		t.Fatal("the object store secret is empty or multi-line")
	}
	a.mu.Lock()
	a.s3secret = cfg.SecretAccessKey
	a.mu.Unlock()
	jailKVMWriteExcl(t, files.EnvFile, []byte(env), 0o600)
	a.own(t, "file", files.EnvFile)
	jailKVMWriteExcl(t, files.SecretFile, []byte(cfg.SecretAccessKey+"\n"), 0o600)
	a.own(t, "file", files.SecretFile)
}

// jailKVMS3Store is the test's own client of the bucket (the node reaches it
// through the forwarder; the test reaches Garage on the host directly).
func jailKVMS3Store(t *testing.T, cfg s3.Config) objstore.Store {
	t.Helper()
	store, err := s3.New(t.Context(), cfg)
	if err != nil {
		t.Fatalf("object store client for the test: %v", err)
	}
	return store
}

// jailKVMForwarder pipes TCP connections accepted on a listener inside the
// node namespace to an upstream address in the host namespace.
type jailKVMForwarder struct {
	upstream string
	lis      net.Listener

	mu     sync.Mutex
	conns  map[net.Conn]struct{}
	hold   chan struct{} // non-nil while held; closed on release
	closed bool
	done   chan struct{}
	wg     sync.WaitGroup
}

// startS3Forwarder listens on jailKVMS3Addr in the node namespace and forwards
// to upstream. The listener's worker is registered with the attempt (its
// teardown joins it before deleting the namespace); the forwarder itself is
// closed in the teardown after the unit is stopped.
func (a *jailKVMAttempt) startS3Forwarder(t *testing.T, upstream string) *jailKVMForwarder {
	t.Helper()
	lis, w, err := jailKVMListenInNetns(t.Context(), a.want.NetnsPath, jailKVMS3Addr, 10*time.Second, jailKVMRealNetnsOps())
	a.mu.Lock()
	a.workers = append(a.workers, w)
	a.mu.Unlock()
	if err != nil {
		t.Fatalf("object store forwarder listener in %s: %v", a.want.NetnsPath, err)
	}
	f := &jailKVMForwarder{upstream: upstream, lis: lis, conns: map[net.Conn]struct{}{}, done: make(chan struct{})}
	f.wg.Go(f.accept)
	a.mu.Lock()
	a.s3fwd = f
	a.mu.Unlock()
	return f
}

func (f *jailKVMForwarder) accept() {
	for {
		c, err := f.lis.Accept()
		if err != nil {
			return
		}
		f.wg.Go(func() { f.pipe(c) })
	}
}

// track registers c, or closes it when the forwarder is already closed.
func (f *jailKVMForwarder) track(c net.Conn) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		_ = c.Close()
		return false
	}
	f.conns[c] = struct{}{}
	return true
}

func (f *jailKVMForwarder) untrack(c net.Conn) {
	f.mu.Lock()
	delete(f.conns, c)
	f.mu.Unlock()
	_ = c.Close()
}

// pipe connects client to the upstream and copies both ways until either ends.
func (f *jailKVMForwarder) pipe(client net.Conn) {
	if !f.track(client) {
		return
	}
	defer f.untrack(client)
	up, err := (&net.Dialer{Timeout: 10 * time.Second}).Dial("tcp", f.upstream)
	if err != nil {
		return
	}
	if !f.track(up) {
		return
	}
	defer f.untrack(up)
	var wg sync.WaitGroup
	wg.Go(func() {
		// Client to upstream: bytes read while held wait here, so a request
		// sent during the hold reaches the store only after release.
		buf := make([]byte, 32<<10)
		for {
			n, err := client.Read(buf)
			if n > 0 {
				f.awaitRelease()
				if _, werr := up.Write(buf[:n]); werr != nil {
					break
				}
			}
			if err != nil {
				break
			}
		}
		if tc, ok := up.(*net.TCPConn); ok {
			_ = tc.CloseWrite()
		}
	})
	_, _ = io.Copy(client, up)
	_ = client.Close()
	_ = up.Close()
	wg.Wait()
}

// awaitRelease blocks while the forwarder is held.
func (f *jailKVMForwarder) awaitRelease() {
	f.mu.Lock()
	h := f.hold
	f.mu.Unlock()
	if h == nil {
		return
	}
	select {
	case <-h:
	case <-f.done:
	}
}

// Hold stops forwarding the node's request bytes until Release (or Close).
func (f *jailKVMForwarder) Hold() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.hold == nil && !f.closed {
		f.hold = make(chan struct{})
	}
}

// Release forwards what Hold kept back and lets new requests through.
func (f *jailKVMForwarder) Release() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.hold != nil {
		close(f.hold)
		f.hold = nil
	}
}

// Close stops listening, drops every connection and joins every goroutine
// within timeout; an error means the join is unproven.
func (f *jailKVMForwarder) Close(timeout time.Duration) error {
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return nil
	}
	f.closed = true
	close(f.done)
	conns := slices.Collect(func(yield func(net.Conn) bool) {
		for c := range f.conns {
			if !yield(c) {
				return
			}
		}
	})
	f.mu.Unlock()
	_ = f.lis.Close()
	for _, c := range conns {
		_ = c.Close()
	}
	joined := make(chan struct{})
	go func() { f.wg.Wait(); close(joined) }()
	select {
	case <-joined:
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("the object store forwarder did not stop within %v", timeout)
	}
}

// jailKVMGarageUpstream is the host address of Garage's S3 endpoint.
func jailKVMGarageUpstream(cfg s3.Config) (string, error) {
	u, err := url.Parse(cfg.Endpoint)
	if err != nil || u.Scheme != "http" || u.Host == "" {
		return "", fmt.Errorf("the Garage endpoint %q is not an http host:port", cfg.Endpoint)
	}
	return u.Host, nil
}

// jailKVMDirectObjectStore builds the Direct helper's store from the unit's
// own environment (EnvironmentFile and the credential directory), through the
// same -s3.* resolution bbraft-node uses.
func jailKVMDirectObjectStore(ctx context.Context) (objstore.Store, error) {
	fs := flag.NewFlagSet("direct-s3", flag.ContinueOnError)
	cfg, configured, err := s3.RegisterFlags(fs).Config(os.LookupEnv)
	if err != nil {
		return nil, fmt.Errorf("object store settings: %w", err)
	}
	if !configured {
		return nil, errors.New("the Direct unit carries no object store (BBRAFT_S3_BUCKET unset): list the attempt in kt_s3_attempts of kvm-harness-vars.yml")
	}
	store, err := s3.New(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("object store: %w", err)
	}
	return store, nil
}

// --- maintenance QEMU inspection --------------------------------------------

// jailKVMCheckMaintenanceArgv checks a maintenance QEMU's argv: the VM's own
// guest name with the "-maint" process marker, paused (-S), no default
// devices, no device, network or RNG option at all, and -m equal to want.
func jailKVMCheckMaintenanceArgv(args []string, id string, wantMemoryMiB uint64) error {
	flagValue := func(name string) (string, bool) {
		i := slices.Index(args, name)
		if i < 0 || i+1 >= len(args) {
			return "", false
		}
		return args[i+1], true
	}
	if v, ok := flagValue("-name"); !ok || v != qemu.MaintenanceName(id) {
		return fmt.Errorf("-name = %q (present %v), want %q", v, ok, qemu.MaintenanceName(id))
	}
	for _, must := range []string{"-S", "-nodefaults"} {
		if !slices.Contains(args, must) {
			return fmt.Errorf("argv lacks %s: %q", must, args)
		}
	}
	for _, forbidden := range []string{"-device", "-netdev", "-nic", "-net", "-object", "-drive", "-usb", "-vnc"} {
		if slices.Contains(args, forbidden) {
			return fmt.Errorf("a maintenance QEMU has %s in its argv (no network, vsock or guest device is allowed): %q", forbidden, args)
		}
	}
	if v, ok := flagValue("-m"); !ok || v != strconv.FormatUint(wantMemoryMiB, 10)+"M" {
		return fmt.Errorf("-m = %q (present %v), want %dM (min(256 MiB, the VM's memory))", v, ok, wantMemoryMiB)
	}
	return nil
}

// jailKVMUnixInodes are the inodes /proc/net/unix lists.
func jailKVMUnixInodes(text string) map[uint64]bool {
	inodes := map[uint64]bool{}
	for line := range strings.Lines(text) {
		f := strings.Fields(line)
		if len(f) < 7 || f[0] == "Num" {
			continue
		}
		if ino, err := strconv.ParseUint(f[6], 10, 64); err == nil {
			inodes[ino] = true
		}
	}
	return inodes
}

// jailKVMCheckMaintenanceFDs refuses a descriptor that would be a network or
// vsock device or socket: any of the forbidden character devices (tun,
// vhost-net, vhost-vsock, vsock; keyed by name for the message) and any
// socket that is not AF_UNIX (TCP, UDP, vsock, netlink, packet), which is
// how a network or vsock endpoint would show up without a device node.
func jailKVMCheckMaintenanceFDs(fds []jailKVMFD, forbiddenRdev map[string]uint64, unixSockets map[uint64]bool) error {
	const chr, sock = 0o020000, 0o140000
	var errs []error
	for _, fd := range fds {
		switch fd.Mode & syscall.S_IFMT {
		case chr:
			for name, rdev := range forbiddenRdev {
				if fd.Rdev == rdev {
					errs = append(errs, fmt.Errorf("fd %d (%s) is %s", fd.Num, fd.Link, name))
				}
			}
		case sock:
			if !unixSockets[fd.ID.Ino] {
				errs = append(errs, fmt.Errorf("fd %d (%s) is a socket that is not AF_UNIX (a network, vsock or netlink endpoint)", fd.Num, fd.Link))
			}
		}
	}
	return errors.Join(errs...)
}

// maintenanceQEMU is a maintenance QEMU found by the inspection.
type jailKVMMaintenanceQEMU struct {
	PID  int
	UID  uint32
	Args []string
}

// waitMaintenanceQEMU polls until the VM's jail record names a live process
// whose argv carries the maintenance marker. The failure names what was last
// seen and the unit's journal, so a session that never started (or ended
// before the inspection) explains itself.
func (a *jailKVMAttempt) waitMaintenanceQEMU(t *testing.T, id string, timeout time.Duration) jailKVMMaintenanceQEMU {
	t.Helper()
	deadline := time.Now().Add(timeout)
	last := "no jail record yet"
	for time.Now().Before(deadline) {
		raw, err := jailKVMReadSmall(filepath.Join(a.rendered.DataDir, "jailer", "records", id+".json"))
		if err != nil {
			last = fmt.Sprintf("jail record: %v", err)
		} else if rec, err := jailKVMDecodeJailRecord(raw, id); err != nil {
			last = fmt.Sprintf("jail record: %v", err)
		} else if cmdline, err := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", rec.PID)); err != nil {
			last = fmt.Sprintf("record pid %d: %v", rec.PID, err)
		} else {
			args := strings.Split(strings.TrimRight(string(cmdline), "\x00"), "\x00")
			if slices.Contains(args, qemu.MaintenanceName(id)) {
				return jailKVMMaintenanceQEMU{PID: rec.PID, UID: rec.UID, Args: args}
			}
			last = fmt.Sprintf("record pid %d runs %q, not a maintenance QEMU", rec.PID, args)
		}
		time.Sleep(50 * time.Millisecond)
	}
	inv := jailKVMShow(t, a.rendered.Service, "InvocationID")["InvocationID"]
	t.Fatalf("no maintenance QEMU for %s within %v (last: %s)\nunit journal tail:\n%s", id, timeout, last, a.journalTail(inv))
	return jailKVMMaintenanceQEMU{}
}

// inspectMaintenance proves, while a STOPPED VM's SnapshotVM holds its
// maintenance session open: the QEMU is jailed like a VM's (unprivileged uid
// of the range, no capabilities, seccomp, private namespaces, read-only
// chroot, its own cgroup), has no network or vsock device or endpoint, and
// its cgroup memory limit is the session's min(256 MiB, VM memory) on one CPU.
func (a *jailKVMAttempt) inspectMaintenance(t *testing.T, id string, res *nodev1.Resources) {
	t.Helper()
	wantMiB := qemu.MaintenanceMemoryMiB(res.GetMemoryMib())
	q := a.waitMaintenanceQEMU(t, id, 90*time.Second)
	pid := strconv.Itoa(q.PID)
	if err := jailKVMCheckMaintenanceArgv(q.Args, id, wantMiB); err != nil {
		t.Errorf("maintenance QEMU of %s (pid %s): %v", id, pid, err)
	}

	status, err := os.ReadFile("/proc/" + pid + "/status")
	if err != nil {
		t.Fatalf("maintenance QEMU of %s ended before it was inspected: %v", id, err)
	}
	lo, hi := a.uidRange(t)
	if err := jailKVMCheckQEMUStatus(string(status), q.UID, lo, hi); err != nil {
		t.Errorf("maintenance QEMU of %s is not confined like a VM's QEMU: %v", id, err)
	}
	mainPID, err := strconv.Atoi(jailKVMShow(t, a.rendered.Service, "MainPID")["MainPID"])
	if err != nil || mainPID <= 0 {
		t.Fatalf("node main pid: %v", err)
	}
	if err := jailKVMCheckNamespaces(jailKVMNamespaces(q.PID), jailKVMNamespaces(mainPID)); err != nil {
		t.Errorf("maintenance QEMU of %s: %v", id, err)
	}
	if mi, err := os.ReadFile("/proc/" + pid + "/mountinfo"); err != nil {
		t.Errorf("maintenance QEMU of %s: mountinfo: %v", id, err)
	} else if err := jailKVMRootMountReadOnly(string(mi)); err != nil {
		t.Errorf("maintenance QEMU of %s: %v", id, err)
	}

	// No network or vsock endpoint: no such device node held, no non-unix socket.
	rdev := func(p string) uint64 {
		var st unix.Stat_t
		if err := unix.Stat(p, &st); err != nil {
			t.Fatalf("stat %s: %v", p, err)
		}
		return st.Rdev
	}
	fds, err := jailKVMProcFDs(q.PID, 1024)
	if err != nil {
		t.Fatalf("maintenance QEMU of %s: descriptors: %v", id, err)
	}
	unixRaw, err := os.ReadFile("/proc/" + pid + "/net/unix")
	if err != nil {
		t.Fatalf("maintenance QEMU of %s: /proc/%s/net/unix: %v", id, pid, err)
	}
	if err := jailKVMCheckMaintenanceFDs(fds, map[string]uint64{
		"/dev/net/tun (a TAP)": rdev("/dev/net/tun"), "/dev/vhost-net": rdev("/dev/vhost-net"), "/dev/vhost-vsock": rdev("/dev/vhost-vsock"),
	}, jailKVMUnixInodes(string(unixRaw))); err != nil {
		t.Errorf("maintenance QEMU of %s holds a network or vsock endpoint:\n%v", id, err)
	}

	// The cgroup limits are the session's, not a VM's.
	want, err := maintenanceLimits(wantMiB, a.jailIO(t))
	if err != nil {
		t.Fatal(err)
	}
	group := filepath.Join(a.cgroupDir(), "vms", "qemu-"+id)
	files := map[string]string{}
	for _, name := range []string{"cpu.max", "memory.max", "pids.max", "io.max"} {
		if raw, err := jailKVMReadSmall(filepath.Join(group, name)); err == nil {
			files[name] = string(raw)
		}
	}
	if err := jailKVMCheckLimits(files, want); err != nil {
		t.Errorf("maintenance QEMU of %s: cgroup %s limits are not the session's (memory.max want %d = %d MiB on one CPU): %v",
			id, group, want.MemoryBytes, wantMiB, err)
	}
	if cg, err := os.ReadFile("/proc/" + pid + "/cgroup"); err != nil || !strings.HasSuffix(strings.TrimSpace(string(cg)), "/vms/qemu-"+id) {
		t.Errorf("maintenance QEMU of %s is in cgroup %q (%v), want .../vms/qemu-%s", id, strings.TrimSpace(string(cg)), err, id)
	}
}

// jailKVMMaintenanceInspection runs the "MaintenanceInspection" subtest body:
// stop a VM that holds data, start its SnapshotVM with the object store held
// (the session cannot finish), inspect its maintenance QEMU, release, and
// require the snapshot to succeed afterwards.
func (a *jailKVMAttempt) maintenanceInspection(t *testing.T, client nodev1.NodeClient, fwd *jailKVMForwarder) {
	t.Helper()
	const id = "bx_cccccccc"
	res := &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: 10}
	jailKVMCreate(t, client, id, res.GetDiskGib())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		if _, err := client.DeleteVM(ctx, &nodev1.DeleteVMRequest{SandboxId: id}); err != nil {
			t.Errorf("maintenance inspection cleanup DeleteVM %s: %v", id, err)
		}
	})
	// Data in the overlay, so the snapshot has chunks to upload (store calls
	// inside the session, which the hold keeps pending).
	jailKVMExec(t, client, id, "dd if=/dev/urandom of=maint.bin bs=1M count=8 conv=fsync status=none && sync")
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	if _, err := client.StopVM(ctx, &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 30}); err != nil {
		t.Fatalf("StopVM: %v", err)
	}

	fwd.Hold()
	defer fwd.Release()
	snapCtx, snapCancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer snapCancel()
	done := make(chan error, 1)
	go func() {
		_, err := client.SnapshotVM(snapCtx, &nodev1.SnapshotVMRequest{SandboxId: id, ChainId: uuid.NewV7().String()})
		done <- err
	}()
	a.inspectMaintenance(t, id, res)
	fwd.Release()
	if err := <-done; err != nil {
		t.Fatalf("SnapshotVM of the STOPPED VM after the inspection: %v", err)
	}
	if vm, err := client.GetVM(ctx, &nodev1.GetVMRequest{SandboxId: id}); err != nil || vm.GetState() != nodev1.VMState_VM_STATE_STOPPED {
		t.Fatalf("VM after the STOPPED snapshot = %v, %v; want STOPPED", vm.GetState(), err)
	}
}

// jailKVMCheckS3Show checks `systemctl show -p EnvironmentFiles -p
// LoadCredential` of the installed unit: systemd itself must have parsed both
// settings (a value it ignores is simply absent from the show output). Some
// systemd versions (255 on Ubuntu 24.04) print LoadCredential as
// "[unprintable]" because the D-Bus a(ss) property has no text form; that
// half cannot be read before the start, so it is accepted here and proven
// after the start by assertS3Credential (the credential file itself).
func jailKVMCheckS3Show(show map[string]string, files jailKVMS3Files) error {
	var errs []error
	if !strings.Contains(show["EnvironmentFiles"], files.EnvFile) {
		errs = append(errs, fmt.Errorf("systemd did not load EnvironmentFile %s (EnvironmentFiles=%q)", files.EnvFile, show["EnvironmentFiles"]))
	}
	lc := strings.TrimSpace(show["LoadCredential"])
	if want := jailKVMS3Credential + ":" + files.SecretFile; lc != "[unprintable]" && !slices.Contains(strings.Split(lc, "\n"), want) {
		errs = append(errs, fmt.Errorf("systemd did not load credential %s (LoadCredential=%q)", want, lc))
	}
	return errors.Join(errs...)
}

// jailKVMCredentialDir is the credentials directory systemd gives a service
// (what %d expands to).
func jailKVMCredentialDir(service string) string {
	return "/run/credentials/" + service + ".service"
}

// jailKVMCheckCredentialFile checks the delivered secret by its stat only (the
// content is never read): a regular file, mode 0400 or 0440, owned by the
// service's uid.
func jailKVMCheckCredentialFile(mode, uid, wantUID uint32) error {
	var errs []error
	if mode&syscall.S_IFMT != syscall.S_IFREG {
		errs = append(errs, fmt.Errorf("it is not a regular file (mode %o)", mode))
	}
	if perm := mode & 0o7777; perm != 0o400 && perm != 0o440 {
		errs = append(errs, fmt.Errorf("mode is %04o, want 0400 or 0440", perm))
	}
	if uid != wantUID {
		errs = append(errs, fmt.Errorf("owner uid is %d, want the service's %d", uid, wantUID))
	}
	return errors.Join(errs...)
}

// jailKVMCheckCredentialEnviron checks the node's initial environment
// (/proc/<pid>/environ, NUL-separated): BBRAFT_S3_SECRET_ACCESS_KEY_FILE
// points at the credential in the service's credentials directory, and the
// secret itself is not in the environment. Errors name the variable and the
// directory only.
func jailKVMCheckCredentialEnviron(environ []byte, service string) error {
	const name = "BBRAFT_S3_SECRET_ACCESS_KEY_FILE"
	want := jailKVMCredentialDir(service) + "/" + jailKVMS3Credential
	// Only this one variable is kept (and the secret variable's presence is
	// noted by name); every other entry, which may hold other secrets, is
	// dropped here and can never reach an error or a log.
	var values []string
	secretInEnv := false
	for entry := range strings.SplitSeq(string(environ), "\x00") {
		if v, ok := strings.CutPrefix(entry, name+"="); ok {
			values = append(values, v)
		} else if strings.HasPrefix(entry, "BBRAFT_S3_SECRET_ACCESS_KEY=") {
			secretInEnv = true
		}
	}
	if secretInEnv {
		return errors.New("BBRAFT_S3_SECRET_ACCESS_KEY (the secret itself) is in the node's environment; it must only arrive as a credential file")
	}
	if len(values) != 1 || values[0] != want {
		return fmt.Errorf("%s is not set once to a file in %s (found %d definitions, %s)", name, jailKVMCredentialDir(service), len(values), jailKVMEnvVerdict(values, want))
	}
	return nil
}

// jailKVMEnvVerdict says whether a differing value is at least in the
// credentials directory, without printing it.
func jailKVMEnvVerdict(got []string, want string) string {
	if len(got) == 1 && strings.HasPrefix(got[0], filepath.Dir(want)+"/") {
		return "its file name differs"
	}
	return "its directory differs"
}

// assertS3Credential proves, after the start, that the credential really
// arrived: the file exists in the service's credentials directory with the
// right type, mode and owner (stat only), and the main process's environment
// names it. The secret is never read or printed.
//
// It reads the environment of the process the main-process proof matched
// (its /proc/<pid>/exe is the staged executable): right after the start the
// unit's main pid is still systemd-executor, whose environment is not the
// node's.
func (a *jailKVMAttempt) assertS3Credential(t *testing.T, inv string) {
	t.Helper()
	svc := a.rendered.Service
	path := jailKVMCredentialDir(svc) + "/" + jailKVMS3Credential
	var st unix.Stat_t
	if err := unix.Lstat(path, &st); err != nil {
		t.Fatalf("attempt %s: the S3 credential did not arrive: lstat %s: %v (LoadCredential was not applied; unit journal tail follows)\n%s",
			svc, path, err, a.journalTail(jailKVMShow(t, svc, "InvocationID")["InvocationID"]))
	}
	proof, err := jailKVMAwaitMainExe(t.Context(), inv, a.mainExe, time.Minute, 50*time.Millisecond, jailKVMMainProcOps(svc))
	if err != nil {
		t.Fatalf("attempt %s: the main process never ran the staged executable: %v", svc, err)
	}
	mainPID := proof.Matched.PID
	t.Logf("S3 credential check: node main process pid %d (exe %q)", mainPID, proof.Matched.ExeLink)
	status, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", mainPID))
	if err != nil {
		t.Fatal(err)
	}
	var uid uint32
	for line := range strings.SplitSeq(string(status), "\n") {
		if f := strings.Fields(strings.TrimPrefix(line, "Uid:")); strings.HasPrefix(line, "Uid:") && len(f) > 0 {
			n, err := strconv.ParseUint(f[0], 10, 32)
			if err != nil {
				t.Fatalf("attempt %s: %v", svc, err)
			}
			uid = uint32(n)
		}
	}
	if err := jailKVMCheckCredentialFile(st.Mode, st.Uid, uid); err != nil {
		t.Errorf("attempt %s: credential %s: %v", svc, path, err)
	}
	environ, err := os.ReadFile(fmt.Sprintf("/proc/%d/environ", mainPID))
	if err != nil {
		t.Fatalf("attempt %s: node environment: %v", svc, err)
	}
	if err := jailKVMCheckCredentialEnviron(environ, svc); err != nil {
		t.Errorf("attempt %s: %v", svc, err)
	}
}

// assertS3Loaded proves the installed unit's object store settings were
// accepted by systemd, before the unit starts.
func (a *jailKVMAttempt) assertS3Loaded(t *testing.T) {
	t.Helper()
	files, err := jailKVMUnitS3Files(a.unit, a.rendered.ConfigDir)
	if err != nil {
		t.Fatalf("attempt %s: %v", a.rendered.Service, err)
	}
	show := jailKVMShow(t, a.rendered.Service, "EnvironmentFiles", "LoadCredential")
	if err := jailKVMCheckS3Show(show, files); err != nil {
		t.Fatalf("attempt %s: %v", a.rendered.Service, err)
	}
}

// jailKVMRedact removes secret from text. Nothing the harness logs should
// hold it (the unit only ever receives it through LoadCredential), but a
// journal tail is node output and is redacted on principle.
func jailKVMRedact(text, secret string) string {
	if secret == "" {
		return text
	}
	return strings.ReplaceAll(text, secret, "[redacted]")
}

// logNodeJournal logs the tail (80 lines) of the unit's current invocation,
// redacted, so a failed Phase 2 subtest explains itself (the node's own
// errors, such as a refused object store, are in its log).
func (a *jailKVMAttempt) logNodeJournal(t *testing.T) {
	t.Helper()
	inv, err := jailKVMOutput(30*time.Second, "systemctl", "show", a.rendered.Service+".service", "-p", "InvocationID", "--value")
	if err != nil {
		t.Logf("node journal: %v", err)
		return
	}
	lines, err := a.journal(strings.TrimSpace(inv))
	if err != nil {
		t.Logf("node journal: %v", err)
		return
	}
	a.mu.Lock()
	secret := a.s3secret
	a.mu.Unlock()
	t.Logf("node journal tail of %s (last 80 lines):\n%s", a.rendered.Service, jailKVMRedact(strings.Join(lines[max(0, len(lines)-80):], "\n"), secret))
}

// restoreSteps runs, under the jail, the steps nodetest's SnapshotRestore
// takes in one piece, each as its own subtest with the numbers logged, so a
// failure says which step the jail breaks. Like nodetest it keeps THREE VMs
// alive at once (the source, a restore with a larger disk, a restore of the
// last generation) and snapshots the restored VM, and it logs the node's Info
// (offer and free disk) before every create and snapshot, so a refused create
// shows whether the node's capacity is the limit. The node's journal tail is
// logged after a failed step.
func (a *jailKVMAttempt) restoreSteps(t *testing.T, client nodev1.NodeClient, store objstore.Store) {
	t.Helper()
	parent := t
	res := &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: 10}
	larger := &nodev1.Resources{Vcpus: res.GetVcpus(), MemoryMib: res.GetMemoryMib(), DiskGib: res.GetDiskGib() + 2}
	src := "bx_dddddddd"
	var shaBig string
	var g0, g1, g2 *nodev1.Generation
	var fork0 string
	chain := uuid.NewV7().String()
	var info func(t *testing.T, when string)
	step := func(name string, fn func(t *testing.T)) bool {
		ok := t.Run(name, fn)
		if !ok {
			info(t, "after the failed step "+name)
			a.logNodeJournal(t)
		}
		return ok
	}
	info = func(t *testing.T, when string) {
		t.Helper()
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()
		i, err := client.Info(ctx, &nodev1.InfoRequest{})
		if err != nil {
			t.Logf("node Info %s: %v", when, err)
			return
		}
		list, err := client.ListVMs(ctx, &nodev1.ListVMsRequest{})
		vms := "?"
		if err == nil {
			vms = strconv.Itoa(len(list.GetVms()))
		}
		t.Logf("node %s: offer %d vCPUs, %d MiB memory; disk free %d MiB (%d GiB); %s VMs", when, i.GetVcpus(), i.GetMemoryMib(), i.GetDiskFreeMib(), i.GetDiskFreeMib()>>10, vms)
	}
	snap := func(t *testing.T, id, chain string, gen uint32) *nodev1.Generation {
		t.Helper()
		info(t, "before SnapshotVM "+id)
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
		defer cancel()
		g, err := client.SnapshotVM(ctx, &nodev1.SnapshotVMRequest{SandboxId: id, ChainId: chain, Generation: gen})
		if err != nil {
			t.Fatalf("SnapshotVM %s %s/%d: %v", id, chain, gen, err)
		}
		t.Logf("gen %d of %s: %d chunks, %d uploaded, %d content bytes, frozen %v", gen, chain, g.GetChunks(), g.GetChunksUploaded(), g.GetContentBytes(), g.GetFrozen())
		return g
	}
	// restore creates a VM from key that stays alive until the whole
	// RestoreSteps ends (the cleanup is the parent's), as nodetest's does.
	restore := func(t *testing.T, key string, r *nodev1.Resources) string {
		t.Helper()
		id := ids.Sandbox()
		info(t, fmt.Sprintf("before CreateVM %s (%d GiB disk) from %s", id, r.GetDiskGib(), key))
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
		defer cancel()
		vm, err := client.CreateVM(ctx, &nodev1.CreateVMRequest{SandboxId: id, FromManifest: key, Resources: r})
		parent.Cleanup(func() {
			dctx, dcancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer dcancel()
			if _, err := client.DeleteVM(dctx, &nodev1.DeleteVMRequest{SandboxId: id}); err != nil {
				parent.Errorf("restore steps cleanup DeleteVM %s: %v", id, err)
			}
		})
		if err != nil {
			t.Fatalf("CreateVM %s from %s with %+v: %v", id, key, r, err)
		}
		h := vm.GetHydration()
		t.Logf("restored %s: state %v, hydration %+v, resources %+v", id, vm.GetState(), h, vm.GetResources())
		if !h.GetHydrated() || h.GetChunksDone() != h.GetChunksTotal() {
			t.Fatalf("restored READY VM hydration = %+v, want hydrated (Phase 2a restores are eager)", h)
		}
		return id
	}
	sha := func(t *testing.T, id string) string {
		t.Helper()
		return strings.TrimSpace(jailKVMExec(t, client, id, "sha256sum p2/big.bin | cut -d' ' -f1"))
	}
	if !step("Snapshot", func(t *testing.T) {
		info(t, "before CreateVM "+src)
		jailKVMCreate(t, client, src, res.GetDiskGib())
		parent.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			if _, err := client.DeleteVM(ctx, &nodev1.DeleteVMRequest{SandboxId: src}); err != nil {
				parent.Errorf("restore steps cleanup DeleteVM %s: %v", src, err)
			}
		})
		shaBig = strings.TrimSpace(jailKVMExec(t, client, src, "mkdir -p p2 && dd if=/dev/urandom of=p2/big.bin bs=1M count=9 conv=fsync status=none && echo 'generation zero' > p2/note.txt && sync && sha256sum p2/big.bin | cut -d' ' -f1"))
		g0 = snap(t, src, chain, 0)
		if g0.GetContentBytes() < 9<<20 {
			t.Errorf("content_bytes %d < the 9 MiB written", g0.GetContentBytes())
		}
		if _, err := store.Head(t.Context(), g0.GetManifestKey()); err != nil {
			t.Errorf("manifest %s is not in the test's view of the bucket: %v", g0.GetManifestKey(), err)
		}
	}) {
		return
	}
	step("UnchangedSecondGeneration", func(t *testing.T) {
		g1 = snap(t, src, chain, 1)
		if g1.GetChunksUploaded() >= g0.GetChunksUploaded() {
			t.Errorf("unchanged disk: generation 1 uploaded %d chunks, generation 0 uploaded %d (the guest writes a little between cuts; this says how much)",
				g1.GetChunksUploaded(), g0.GetChunksUploaded())
		}
	})
	step("SecondChange", func(t *testing.T) {
		jailKVMExec(t, client, src, "echo 'generation two' > p2/note.txt && echo 'only in two' > p2/later.txt && sync")
		g2 = snap(t, src, chain, 2)
	})
	if g2 == nil {
		return
	}
	// Source alive; now a restore with a larger disk alongside it.
	step("RestoreLargerDiskAlongsideSource", func(t *testing.T) {
		fork0 = restore(t, g0.GetManifestKey(), larger)
		if got := sha(t, fork0); got != shaBig {
			t.Fatalf("restored big.bin sha %s, want %s", got, shaBig)
		}
		if got := jailKVMExec(t, client, fork0, "cat p2/note.txt"); got != "generation zero\n" {
			t.Errorf("generation 0 note = %q", got)
		}
		if size := jailKVMRootfsBytes(t, client, fork0); size < larger.GetDiskGib()<<30*95/100 {
			t.Errorf("restored filesystem is %d bytes, want about %d GiB", size, larger.GetDiskGib())
		}
	})
	if fork0 != "" {
		step("SnapshotOfRestoredVM", func(t *testing.T) {
			g := snap(t, fork0, uuid.NewV7().String(), 0)
			if g.GetChunksUploaded() >= g.GetChunks() {
				t.Errorf("the restored VM's first generation uploaded %d of %d chunks: it should reuse the restored manifest's chunks", g.GetChunksUploaded(), g.GetChunks())
			}
		})
	}
	// Third VM alive at once: the last generation.
	step("RestoreLastGenerationThirdVM", func(t *testing.T) {
		fork2 := restore(t, g2.GetManifestKey(), res)
		if got := jailKVMExec(t, client, fork2, "cat p2/note.txt p2/later.txt"); got != "generation two\nonly in two\n" {
			t.Errorf("generation 2 files = %q", got)
		}
		if got := jailKVMExec(t, client, src, "cat p2/note.txt"); got != "generation two\n" {
			t.Errorf("source note after snapshots = %q", got)
		}
		info(t, "with three VMs alive")
	})
}

// logOffer logs the node's offer, free disk and the VMs it still holds, and
// fails when a VM is left (a leaked VM would eat the capacity the next group
// needs).
func (a *jailKVMAttempt) logOffer(t *testing.T, client nodev1.NodeClient, when string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	i, err := client.Info(ctx, &nodev1.InfoRequest{})
	if err != nil {
		t.Fatalf("node Info %s: %v", when, err)
	}
	list, err := client.ListVMs(ctx, &nodev1.ListVMsRequest{})
	if err != nil {
		t.Fatalf("node ListVMs %s: %v", when, err)
	}
	t.Logf("node %s: offer %d vCPUs, %d MiB memory; disk free %d GiB; %d VMs alive", when, i.GetVcpus(), i.GetMemoryMib(), i.GetDiskFreeMib()>>10, len(list.GetVms()))
	for _, vm := range list.GetVms() {
		t.Errorf("node %s: VM %s (%v, %d vCPUs) is still alive", when, vm.GetSandboxId(), vm.GetState(), vm.GetResources().GetVcpus())
	}
}
