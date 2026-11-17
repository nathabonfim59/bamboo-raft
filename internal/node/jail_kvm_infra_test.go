package node

// Infrastructure of the A10 final jailed harness (jail_kvm_test.go): the
// attempt-unique runtime unit, its staged binaries and image, the Hub in the
// owned node namespace, and the bounded, identity-proven teardown. Test-only;
// it runs only under BBRAFT_TEST_KVM_JAIL=1 as root in a throwaway VM.

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/nathabonfim59/bamboo-raft/internal/nodelink"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

const (
	// jailKVMHubAddr is the rendered -server.addr inside the node namespace.
	jailKVMHubAddr = "127.0.0.1:7701"
	// jailKVMMaxCgroups bounds the unit cgroup subtree walk.
	jailKVMMaxCgroups = 256
)

// jailKVMRequire validates the gate (skip unless BBRAFT_TEST_KVM_JAIL=1;
// with it set every missing prerequisite fails).
func jailKVMRequire(t *testing.T) *jailKVMEnv {
	t.Helper()
	if os.Getenv(jailKVMGateEnv) != "1" {
		t.Skipf("set %s=1 (root throwaway VM only, lead-owned)", jailKVMGateEnv)
	}
	if os.Geteuid() != 0 {
		t.Fatal("run the built test binary as root inside the throwaway VM")
	}
	for _, binary := range []string{"systemctl", "journalctl", "ip", "setpriv"} {
		if _, err := exec.LookPath(binary); err != nil {
			t.Fatalf("%s is required: %v", binary, err)
		}
	}
	out := jailKVMRun(t, 10*time.Second, "systemctl", "--version")
	fields := strings.Fields(out)
	if v, err := strconv.Atoi(slices.Concat(fields, []string{"", ""})[1]); len(fields) < 2 || fields[0] != "systemd" || err != nil || v < 254 {
		t.Fatalf("systemd 254 or newer is required (DelegateSubgroup=): %q", strings.SplitN(out, "\n", 2)[0])
	}
	units, bin, image := os.Getenv(jailKVMUnitsEnv), os.Getenv(jailKVMBinEnv), os.Getenv("BBRAFT_TEST_IMAGE_DIR")
	if units == "" || bin == "" || image == "" {
		t.Fatalf("%s, %s and BBRAFT_TEST_IMAGE_DIR (the image version dir given to kvm-harness-render.yml) are required", jailKVMUnitsEnv, jailKVMBinEnv)
	}
	raw, err := os.ReadFile(filepath.Join(units, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	env := &jailKVMEnv{binDir: bin, imageDir: image, imageID: filepath.Base(image)}
	if err := json.Unmarshal(raw, &env.manifest, json.RejectUnknownMembers(true)); err != nil {
		t.Fatalf("units manifest: %v", err)
	}
	var m struct {
		ID string `json:"id"`
	}
	if raw, err := os.ReadFile(filepath.Join(image, "manifest.json")); err != nil || json.Unmarshal(raw, &m) != nil || m.ID != env.imageID {
		t.Fatalf("BBRAFT_TEST_IMAGE_DIR %s is not an image version dir named after its manifest id (%v)", image, err)
	}
	return env
}

// jailKVMRun runs a host command bounded by timeout and returns its stdout;
// failure is fatal.
func jailKVMRun(t *testing.T, timeout time.Duration, name string, args ...string) string {
	t.Helper()
	out, err := jailKVMOutput(timeout, name, args...)
	if err != nil {
		t.Fatalf("%s %s: %v", name, strings.Join(args, " "), err)
	}
	return out
}

// jailKVMOutput runs a host command bounded by timeout.
func jailKVMOutput(timeout time.Duration, name string, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var stdout, stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		return stdout.String(), fmt.Errorf("%w: %s", err, strings.TrimSpace(stderr.String()))
	}
	return stdout.String(), nil
}

// jailKVMAttempt is one attempt-unique unit: its rendered identity, the
// owned objects created so far (in creation order) and the systemd/Hub
// state. Its teardown is registered before anything is created.
type jailKVMAttempt struct {
	t        *testing.T
	env      *jailKVMEnv
	rendered jailKVMRendered
	unit     jailKVMUnit
	want     jailKVMBoundaryWant
	tp       *kvmNetTopology
	token    string

	mu        sync.Mutex
	owned     []jailKVMOwned
	workers   []*jailKVMListenWorker
	installed bool
	// mainExe is the identity of the executable the unit's ExecStart runs
	// (the staged bbraft-node, or the Direct helper), recorded when it is
	// staged or installed; the main process is asserted only once its
	// /proc/<pid>/exe is this inode.
	mainExe jailKVMIdentity
	hub     *jailKVMHub
	client  nodev1.NodeClient
	// lastDone is the Done of the newest link this attempt used; the next
	// link must be a different one (jailKVMWaitNewLink).
	lastDone <-chan struct{}
	// reconnect reaches a (re)started unit for the teardown recovery
	// without failing the test (Link: the Hub's next new link; Direct: a
	// fresh dial). nil means no recovery is possible.
	reconnect func(ctx context.Context) (nodev1.NodeClient, error)
	// conns closes every Direct client connection this attempt opened;
	// teardown runs them after the recovery, whatever it did.
	conns []func() error
}

// jailKVMHub is the test Hub whose TCP listener lives in nodeNS.
type jailKVMHub struct {
	hub    *nodelink.Hub
	nodeID string
	cancel context.CancelFunc
	done   chan struct{}
}

// jailKVMLink is one accepted nodelink connection.
type jailKVMLink struct {
	Client nodev1.NodeClient
	Done   <-chan struct{}
}

// attempt refuses an existing identity, parses and checks the rendered unit
// and registers the bounded teardown (jailKVMCleanupPlan) on t.
func (e *jailKVMEnv) attempt(t *testing.T, name string, tp *kvmNetTopology) *jailKVMAttempt {
	t.Helper()
	r, ok := e.manifest.Attempts[name]
	if !ok {
		t.Fatalf("the units manifest has no %q attempt", name)
	}
	text, err := os.ReadFile(r.Unit)
	if err != nil {
		t.Fatal(err)
	}
	u, err := jailKVMParseUnit(string(text))
	if err != nil {
		t.Fatal(err)
	}
	a := &jailKVMAttempt{t: t, env: e, rendered: r, unit: u, tp: tp,
		want: jailKVMBoundaryWant{NetnsPath: "/run/netns/" + tp.nodeNS, DataDir: r.DataDir, Service: r.Service}}
	if _, err := jailKVMImmutable(u, a.want); err != nil {
		t.Fatalf("rendered unit %s: %v", r.Unit, err)
	}
	if err := jailKVMCheckExec(u.ExecStart, jailKVMExpect{Binary: r.BinaryPath, Flags: map[string]string{
		"-node.id": r.NodeID, "-node.data-dir": r.DataDir, "-node.image-dir": r.ImageDir,
		"-node.token-file": r.ConfigDir + "/node.token", "-server.addr": jailKVMHubAddr,
		"-node.jail-cgroup": a.cgroupDir() + "/vms", "-node.default-image": e.imageID,
	}}); err != nil {
		t.Fatalf("rendered unit %s: %v", r.Unit, err)
	}
	// Refuse any existing object of this identity: never adopt or delete it.
	if show := jailKVMShow(t, r.Service, "LoadState"); show["LoadState"] != "not-found" {
		t.Fatalf("unit %s already exists (LoadState=%s); refusing to adopt it", r.Service, show["LoadState"])
	}
	for _, p := range []string{r.UnitPath, r.UnitPath + ".d", "/etc/systemd/system/" + r.Service + ".service",
		"/etc/systemd/system/" + r.Service + ".service.d", a.cgroupDir(), r.DataDir, r.ConfigDir, r.StageDir} {
		if _, err := os.Lstat(p); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("%s already exists (%v); refusing to adopt or remove it", p, err)
		}
	}
	t.Cleanup(a.teardown)
	return a
}

func (a *jailKVMAttempt) cgroupDir() string {
	return "/sys/fs/cgroup/system.slice/" + a.rendered.Service + ".service"
}

// own records an object this attempt just created, by identity.
func (a *jailKVMAttempt) own(t *testing.T, kind, path string) {
	t.Helper()
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatalf("pin %s: %v", path, err)
	}
	st := info.Sys().(*syscall.Stat_t)
	a.mu.Lock()
	a.owned = append(a.owned, jailKVMOwned{Kind: kind, Path: path, ID: jailKVMIdentity{Dev: uint64(st.Dev), Ino: st.Ino}})
	a.mu.Unlock()
}

// mkdirOwned creates one new directory exclusively (EEXIST is fatal) and
// records it.
func (a *jailKVMAttempt) mkdirOwned(t *testing.T, path string, mode os.FileMode) {
	t.Helper()
	if err := os.Mkdir(path, mode); err != nil {
		t.Fatalf("create %s: %v", path, err)
	}
	a.own(t, "dir", path)
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
}

// writeExcl creates one new file exclusively with data and mode.
func jailKVMWriteExcl(t *testing.T, path string, data []byte, mode os.FileMode) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, mode)
	if err != nil {
		t.Fatalf("create %s: %v", path, err)
	}
	_, werr := f.Write(data)
	serr := f.Sync()
	if err := errors.Join(werr, serr, f.Close(), os.Chmod(path, mode)); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// copyExcl copies src into the new file dst (exclusive) and returns the
// sha256 of what was written.
func jailKVMCopyExcl(t *testing.T, src, dst string, mode os.FileMode) string {
	t.Helper()
	in, err := os.Open(src)
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, mode)
	if err != nil {
		t.Fatalf("create %s: %v", dst, err)
	}
	h := sha256.New()
	_, cerr := io.Copy(io.MultiWriter(out, h), in)
	if err := errors.Join(cerr, out.Sync(), out.Close(), os.Chmod(dst, mode)); err != nil {
		t.Fatalf("copy %s -> %s: %v", src, dst, err)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// stage creates the attempt's stage, config and data dirs, copies the built
// node binary and the verified image version into them (outside ProtectHome
// and PrivateTmp, root-owned), writes a fresh token and records every
// created top-level object.
func (a *jailKVMAttempt) stage(t *testing.T) {
	t.Helper()
	r := a.rendered
	a.mkdirOwned(t, r.StageDir, 0o755)
	if err := os.Mkdir(filepath.Dir(r.BinaryPath), 0o755); err != nil {
		t.Fatal(err)
	}
	got := jailKVMCopyExcl(t, filepath.Join(a.env.binDir, "bbraft-node"), r.BinaryPath, 0o755)
	if got != r.BinarySHA256 {
		t.Fatalf("staged bbraft-node sha256 %s, the unit was rendered for %s", got, r.BinarySHA256)
	}
	a.mainExe = jailKVMExeIdentity(t, r.BinaryPath)
	a.mkdirOwned(t, r.ConfigDir, 0o750)
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		t.Fatal(err)
	}
	a.token = hex.EncodeToString(buf)
	jailKVMWriteExcl(t, r.ConfigDir+"/node.token", []byte(a.token), 0o600)
	// The data root as the role makes it: 0700 with the v1 marker, images
	// below it read-only.
	a.mkdirOwned(t, r.DataDir, 0o700)
	jailKVMWriteExcl(t, r.DataDir+"/.bbraft-node-data-v1", []byte("bbraft-node data root v1\n"), 0o600)
	if err := os.Mkdir(r.ImageDir, 0o755); err != nil {
		t.Fatal(err)
	}
	version := filepath.Join(r.ImageDir, a.env.imageID)
	if err := os.Mkdir(version, 0o755); err != nil {
		t.Fatal(err)
	}
	var m struct {
		Artifacts map[string]struct {
			SHA256 string `json:"sha256"`
		} `json:"artifacts"`
	}
	raw, err := os.ReadFile(filepath.Join(a.env.imageDir, "manifest.json"))
	if err != nil || json.Unmarshal(raw, &m) != nil {
		t.Fatalf("image manifest: %v", err)
	}
	for _, name := range []string{"kernel", "initrd", "root.qcow2", "packages.json", "manifest.json"} {
		sum := jailKVMCopyExcl(t, filepath.Join(a.env.imageDir, name), filepath.Join(version, name), 0o444)
		if want, ok := m.Artifacts[name]; ok && sum != want.SHA256 {
			t.Fatalf("staged image %s sha256 %s, manifest %s", name, sum, want.SHA256)
		}
	}
	for _, dir := range []string{version, r.ImageDir} {
		if err := os.Chmod(dir, 0o555); err != nil {
			t.Fatal(err)
		}
	}
}

// install writes the unit and its drop-in (NetworkNamespacePath, plus the
// Direct ExecStart/Environment when direct is set), daemon-reloads and
// records identities. It refuses if the unit is already loaded.
func (a *jailKVMAttempt) install(t *testing.T, direct *jailKVMDirect) {
	t.Helper()
	r := a.rendered
	dropIn, err := jailKVMDropIn(a.want.NetnsPath, direct)
	if err != nil {
		t.Fatal(err)
	}
	if direct != nil {
		a.mainExe = jailKVMExeIdentity(t, direct.Exec[0])
	}
	unitText, err := os.ReadFile(r.Unit)
	if err != nil {
		t.Fatal(err)
	}
	jailKVMWriteExcl(t, r.UnitPath, unitText, 0o644)
	a.own(t, "file", r.UnitPath)
	a.mkdirOwned(t, r.UnitPath+".d", 0o755)
	jailKVMWriteExcl(t, r.UnitPath+".d/10-kvmtest.conf", []byte(dropIn), 0o644)
	a.mu.Lock()
	a.installed = true
	a.mu.Unlock()
	jailKVMRun(t, time.Minute, "systemctl", "daemon-reload")
	show := jailKVMShow(t, r.Service, "LoadState", "FragmentPath", "DropInPaths")
	if show["LoadState"] != "loaded" || show["FragmentPath"] != r.UnitPath || show["DropInPaths"] != r.UnitPath+".d/10-kvmtest.conf" {
		t.Fatalf("installed unit %s did not load as written: %v", r.Service, show)
	}
}

// jailKVMShow returns `systemctl show -p ...` as a map (repeated keys joined
// by newlines).
func jailKVMShow(t *testing.T, service string, props ...string) map[string]string {
	t.Helper()
	args := []string{"show", service + ".service"}
	for _, p := range props {
		args = append(args, "-p", p)
	}
	out := jailKVMRun(t, 30*time.Second, "systemctl", args...)
	got := map[string]string{}
	for line := range strings.SplitSeq(out, "\n") {
		if k, v, ok := strings.Cut(line, "="); ok {
			if prev, seen := got[k]; seen {
				v = prev + "\n" + v
			}
			got[k] = v
		}
	}
	return got
}

// start runs systemctl start and returns the InvocationID.
func (a *jailKVMAttempt) start(t *testing.T) string {
	t.Helper()
	jailKVMRun(t, 2*time.Minute, "systemctl", "start", a.rendered.Service+".service")
	show := jailKVMShow(t, a.rendered.Service, "InvocationID", "ActiveState", "MainPID")
	if show["InvocationID"] == "" || show["ActiveState"] != "active" || show["MainPID"] == "0" {
		t.Fatalf("unit %s did not start: %v\n%s", a.rendered.Service, show, a.journalTail(show["InvocationID"]))
	}
	return show["InvocationID"]
}

// journal returns one invocation's journal lines.
func (a *jailKVMAttempt) journal(inv string) ([]string, error) {
	out, err := jailKVMOutput(30*time.Second, "journalctl", "--no-pager", "-o", "cat", "_SYSTEMD_INVOCATION_ID="+inv)
	if err != nil {
		return nil, err
	}
	return strings.Split(strings.TrimRight(out, "\n"), "\n"), nil
}

func (a *jailKVMAttempt) journalTail(inv string) string {
	lines, err := a.journal(inv)
	if err != nil {
		return fmt.Sprintf("(journal: %v)", err)
	}
	return strings.Join(lines[max(0, len(lines)-40):], "\n")
}

// jailKVMExeIdentity is the (device, inode) of path, never following a final
// symlink.
func jailKVMExeIdentity(t *testing.T, path string) jailKVMIdentity {
	t.Helper()
	var st unix.Stat_t
	if err := unix.Lstat(path, &st); err != nil || st.Mode&unix.S_IFMT != unix.S_IFREG {
		t.Fatalf("executable %s: %v (mode %o): want a regular file", path, err, st.Mode)
	}
	return jailKVMIdentity{Dev: st.Dev, Ino: st.Ino}
}

// jailKVMMainProcOps reads one unit's InvocationID/MainPID and the main
// process's /proc facts.
func jailKVMMainProcOps(service string) jailKVMMainOps {
	return jailKVMMainOps{
		Show: func(ctx context.Context) (string, int, error) {
			out, err := jailKVMOutput(30*time.Second, "systemctl", "show", service+".service", "-p", "InvocationID", "-p", "MainPID")
			if err != nil {
				return "", 0, err
			}
			got := map[string]string{}
			for line := range strings.SplitSeq(out, "\n") {
				if k, v, ok := strings.Cut(line, "="); ok {
					got[k] = v
				}
			}
			pid, err := strconv.Atoi(got["MainPID"])
			if err != nil {
				return "", 0, fmt.Errorf("MainPID %q: %w", got["MainPID"], err)
			}
			return got["InvocationID"], pid, nil
		},
		Exe: func(pid int) (jailKVMIdentity, string, error) {
			exe := "/proc/" + strconv.Itoa(pid) + "/exe"
			var st unix.Stat_t
			if err := unix.Stat(exe, &st); err != nil {
				return jailKVMIdentity{}, "", err
			}
			link, err := os.Readlink(exe)
			return jailKVMIdentity{Dev: st.Dev, Ino: st.Ino}, link, err
		},
		StartTime: func(pid int) (string, error) {
			raw, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
			if err != nil {
				return "", err
			}
			return jailKVMStatStartTime(string(raw))
		},
		Status: func(pid int) (string, error) {
			raw, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/status")
			return string(raw), err
		},
	}
}

// assertBoundary checks systemctl show against jailKVMImmutable, then waits
// (bounded) until the invocation's main process runs a.mainExe and checks
// that process's status (jailKVMCheckMainStatus) and cgroup
// (<unit>/supervisor). The early systemd-executor sample and the matched
// one are logged as gate evidence.
func (a *jailKVMAttempt) assertBoundary(t *testing.T, inv string) {
	t.Helper()
	want, err := jailKVMImmutable(a.unit, a.want)
	if err != nil {
		t.Fatal(err)
	}
	keys := slices.Sorted(maps.Keys(want))
	args := []string{"show", a.rendered.Service + ".service"}
	for _, k := range keys {
		args = append(args, "-p", k)
	}
	if err := jailKVMCheckShow(want, jailKVMRun(t, 30*time.Second, "systemctl", args...)); err != nil {
		t.Fatalf("live unit boundary: %v", err)
	}
	if a.mainExe == (jailKVMIdentity{}) {
		t.Fatal("no staged executable identity recorded")
	}
	ops := jailKVMMainProcOps(a.rendered.Service)
	proof, err := jailKVMAwaitMainExe(t.Context(), inv, a.mainExe, time.Minute, 50*time.Millisecond, ops)
	t.Logf("main process: %d samples; initial %v", proof.Samples, proof.Initial)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("main process: matched %v", proof.Matched)
	if err := jailKVMCheckMainStatus(proof.Matched.Status); err != nil {
		t.Fatal(err)
	}
	pid := strconv.Itoa(proof.Matched.PID)
	cg, err := os.ReadFile("/proc/" + pid + "/cgroup")
	if want := "0::/system.slice/" + a.rendered.Service + ".service/supervisor\n"; err != nil || string(cg) != want {
		t.Fatalf("node main cgroup %q, %v; want %q", cg, err, want)
	}
	// The cgroup read is bracketed too: the same process still runs the
	// staged executable.
	if again, err := jailKVMAwaitMainExe(t.Context(), inv, a.mainExe, 10*time.Second, 50*time.Millisecond, ops); err != nil ||
		again.Matched.PID != proof.Matched.PID || again.Matched.StartTime != proof.Matched.StartTime {
		t.Fatalf("main process changed after its cgroup read: %v then %v, %v", proof.Matched, again.Matched, err)
	}
}

// assertStartupLog waits (bounded, jailKVMAwaitReady) for exactly
// invocation inv's "bbraft-node ready", refusing at once when the unit
// restarted, then runs jailKVMCheckStartupLog and the local-CID oracle on
// that invocation's whole journal.
func (a *jailKVMAttempt) assertStartupLog(t *testing.T, inv string) {
	t.Helper()
	service := a.rendered.Service + ".service"
	ops := jailKVMReadyOps{
		Show: func(ctx context.Context) (string, string, string, error) {
			out, err := jailKVMOutput(30*time.Second, "systemctl", "show", service, "-p", "InvocationID", "-p", "ActiveState", "-p", "NRestarts")
			if err != nil {
				return "", "", "", err
			}
			got := map[string]string{}
			for line := range strings.SplitSeq(out, "\n") {
				if k, v, ok := strings.Cut(line, "="); ok {
					got[k] = v
				}
			}
			return got["InvocationID"], got["ActiveState"], got["NRestarts"], nil
		},
		Journal: func(_ context.Context, inv string) ([]string, error) { return a.journal(inv) },
	}
	lines, err := jailKVMAwaitReady(t.Context(), inv, 5*time.Minute, 500*time.Millisecond, ops)
	if err != nil {
		t.Fatalf("unit %s: %v", service, err)
	}
	if err := jailKVMCheckStartupLog(lines); err != nil {
		t.Fatalf("%v\n%s", err, strings.Join(lines, "\n"))
	}
	// The harness probes the local CID itself (root, outside the unit's
	// device policy); an error fails the gate, never skips.
	local, err := probeLocalVsockCID(t.Context(), localVsockDevice)
	if err != nil {
		t.Fatalf("harness local CID probe: %v", err)
	}
	t.Logf("harness probed local vsock CID %d", local)
	if err := jailKVMCheckStartupCID(lines, local); err != nil {
		t.Fatalf("%v\n%s", err, strings.Join(lines, "\n"))
	}
}

// startHub creates the Hub listener in nodeNS (jailKVMListenInNetns) at the
// rendered -server.addr and serves it until teardown. It registers the
// listener worker with the attempt before anything else; teardown joins
// every registered worker (bounded) before deleting the namespace and
// preserves the whole attempt when a worker is not proven finished.
func (a *jailKVMAttempt) startHub(t *testing.T) *jailKVMHub {
	t.Helper()
	lis, w, err := jailKVMListenInNetns(t.Context(), a.want.NetnsPath, jailKVMHubAddr, 10*time.Second, jailKVMRealNetnsOps())
	a.mu.Lock()
	a.workers = append(a.workers, w)
	a.mu.Unlock()
	if err != nil {
		t.Fatalf("hub listener in %s: %v", a.want.NetnsPath, err)
	}
	token, nodeID := a.token, a.rendered.NodeID
	hub := nodelink.NewHub(func(_ context.Context, h nodelink.Hello) error {
		if h.NodeID != nodeID || subtle.ConstantTimeCompare([]byte(h.Token), []byte(token)) != 1 || token == "" {
			return errors.New("unknown node or token")
		}
		return nil
	}, slog.New(slog.DiscardHandler))
	ctx, cancel := context.WithCancel(context.Background())
	h := &jailKVMHub{hub: hub, nodeID: nodeID, cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(h.done)
		_ = hub.Serve(ctx, lis)
	}()
	a.mu.Lock()
	a.hub = h
	a.reconnect = func(ctx context.Context) (nodev1.NodeClient, error) {
		a.mu.Lock()
		old := a.lastDone
		a.mu.Unlock()
		link, err := jailKVMWaitNewLink(ctx, old, h.next, 100*time.Millisecond)
		if err != nil {
			return nil, err
		}
		a.mu.Lock()
		a.client, a.lastDone = link.Client, link.Done
		a.mu.Unlock()
		return link.Client, nil
	}
	a.mu.Unlock()
	return h
}

// waitLink returns the attempt's next new link (never the one it used
// last; after a restart the Hub may still hold that one), records it for
// the teardown and returns it.
func (a *jailKVMAttempt) waitLink(t *testing.T, h *jailKVMHub) jailKVMLink {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	a.mu.Lock()
	old := a.lastDone
	a.mu.Unlock()
	link, err := jailKVMWaitNewLink(ctx, old, h.next, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("node %s never connected through a new nodelink link: %v", h.nodeID, err)
	}
	a.mu.Lock()
	a.client, a.lastDone = link.Client, link.Done
	a.mu.Unlock()
	return link
}

// next is the Hub's current link of the node id.
func (h *jailKVMHub) next(ctx context.Context) (jailKVMLink, error) {
	link, err := h.hub.Wait(ctx, h.nodeID)
	if err != nil {
		return jailKVMLink{}, err
	}
	return jailKVMLink{Client: link.Client, Done: link.Done()}, nil
}

// teardown is the attempt's bounded cleanup, registered before anything was
// created: delete sandboxes through the API, stop the unit and prove it
// stopped, prove the whole unit cgroup subtree empty, close the Hub and join
// every listener worker, then remove exactly the recorded still-matching
// objects (jailKVMCleanupPlan) or preserve everything, the namespaces
// included.
func (a *jailKVMAttempt) teardown() {
	t := a.t
	r := a.rendered
	a.mu.Lock()
	client, hub, installed, workers, owned := a.client, a.hub, a.installed, slices.Clone(a.workers), slices.Clone(a.owned)
	a.mu.Unlock()
	if client != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		if list, err := client.ListVMs(ctx, &nodev1.ListVMsRequest{}); err == nil {
			for _, vm := range list.GetVms() {
				if _, err := client.DeleteVM(ctx, &nodev1.DeleteVMRequest{SandboxId: vm.GetSandboxId()}); err != nil {
					t.Errorf("teardown DeleteVM %s: %v", vm.GetSandboxId(), err)
				}
			}
		} else {
			t.Errorf("teardown ListVMs: %v", err)
		}
		cancel()
	}
	td := jailKVMTeardown{UnitStopped: !installed, Owned: owned}
	stop := func(context.Context) (bool, error) {
		_, err := jailKVMOutput(2*time.Minute, "systemctl", "stop", r.Service+".service")
		show := map[string]string{}
		if out, serr := jailKVMOutput(30*time.Second, "systemctl", "show", r.Service+".service", "-p", "ActiveState", "-p", "MainPID"); serr == nil {
			for line := range strings.SplitSeq(out, "\n") {
				if k, v, ok := strings.Cut(line, "="); ok {
					show[k] = v
				}
			}
		} else {
			err = errors.Join(err, serr)
		}
		return (show["ActiveState"] == "inactive" || show["ActiveState"] == "failed") && show["MainPID"] == "0", err
	}
	subtree := func(ctx context.Context) error {
		if _, err := os.Lstat(a.cgroupDir()); err == nil {
			return jailKVMSubtreeEmpty(ctx, a.cgroupDir(), jailKVMMaxCgroups)
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		return nil
	}
	if installed {
		var err error
		if td.UnitStopped, err = stop(context.Background()); err != nil {
			t.Errorf("teardown stop %s: %v", r.Service, err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	td.SubtreeEmpty = subtree(ctx)
	cancel()
	// KillMode=process leaves jailed QEMUs running when the unit stops with
	// VMs left (a failed test or a failed delete): one bounded recovery
	// restarts the unit, deletes every VM through the API and stops it
	// again. Only a proven stop with a well-formed populated subtree
	// qualifies; an unreadable or malformed subtree is never "recovered".
	a.mu.Lock()
	reconnect := a.reconnect
	a.mu.Unlock()
	if installed && td.UnitStopped && errors.Is(td.SubtreeEmpty, errJailKVMPopulated) && reconnect != nil {
		t.Logf("teardown: %s stopped with a populated subtree (%v); running the one bounded recovery", r.Service, td.SubtreeEmpty)
		rctx, rcancel := context.WithTimeout(context.Background(), 8*time.Minute)
		td.UnitStopped, td.SubtreeEmpty, td.Recovery = jailKVMRecoverOnce(rctx, jailKVMRecoverOps{
			Start: func(context.Context) error {
				_, err := jailKVMOutput(2*time.Minute, "systemctl", "start", r.Service+".service")
				return err
			},
			Connect: reconnect,
			Stop:    stop,
			Subtree: subtree,
		})
		rcancel()
		t.Logf("teardown recovery: stopped %v, subtree %v, err %v", td.UnitStopped, td.SubtreeEmpty, td.Recovery)
	}
	a.mu.Lock()
	conns := slices.Clone(a.conns)
	a.conns = nil
	a.mu.Unlock()
	for _, closeConn := range conns {
		if err := closeConn(); err != nil {
			t.Logf("teardown: close a direct connection: %v", err)
		}
	}
	if hub != nil {
		hub.cancel()
		select {
		case <-hub.done:
		case <-time.After(30 * time.Second):
			td.WorkersDone = errors.New("the hub did not stop within 30s")
		}
	}
	for _, w := range workers {
		if err := w.Wait(10 * time.Second); err != nil {
			td.WorkersDone = errors.Join(td.WorkersDone, err)
		}
	}
	for _, o := range owned {
		info, err := os.Lstat(o.Path)
		switch {
		case errors.Is(err, os.ErrNotExist):
			td.Observed = append(td.Observed, jailKVMObserved{})
		case err != nil:
			td.Observed = append(td.Observed, jailKVMObserved{Err: err})
		default:
			st := info.Sys().(*syscall.Stat_t)
			td.Observed = append(td.Observed, jailKVMObserved{Exists: true, ID: jailKVMIdentity{Dev: uint64(st.Dev), Ino: st.Ino}})
		}
	}
	plan := jailKVMCleanupPlan(td)
	if len(plan.Preserve) > 0 {
		t.Errorf("preserving attempt %s (unit, files, cgroup and namespaces) for inspection: %s", r.Attempt, strings.Join(plan.Preserve, "; "))
		a.tp.ownedNS = map[string]uint64{} // the topology must not delete namespaces the attempt may still use
		return
	}
	for _, o := range plan.Remove {
		// Re-check right before removing: the identity must still match.
		info, err := os.Lstat(o.Path)
		if err != nil {
			t.Errorf("teardown %s: %v; stopping cleanup", o.Path, err)
			a.tp.ownedNS = map[string]uint64{}
			return
		}
		st := info.Sys().(*syscall.Stat_t)
		if (jailKVMIdentity{Dev: uint64(st.Dev), Ino: st.Ino}) != o.ID {
			t.Errorf("teardown %s changed identity; stopping cleanup and preserving the rest", o.Path)
			a.tp.ownedNS = map[string]uint64{}
			return
		}
		var rerr error
		if o.Kind == "dir" {
			rerr = os.RemoveAll(o.Path)
		} else {
			rerr = os.Remove(o.Path)
		}
		if rerr != nil {
			t.Errorf("teardown remove %s: %v; preserving the rest", o.Path, rerr)
			a.tp.ownedNS = map[string]uint64{}
			return
		}
	}
	if installed {
		_, _ = jailKVMOutput(time.Minute, "systemctl", "daemon-reload")
		_, _ = jailKVMOutput(30*time.Second, "systemctl", "reset-failed", r.Service+".service")
	}
}

// TestKVMJailStartup is the minimal actual-main checkpoint: the role-rendered
// unit starts the built production bbraft-node in the owned node namespace;
// the live systemd boundary, the main process's capabilities and cgroup, the
// exact startup log order and the Hub link are proven; then teardown stops
// the unit and removes exactly what the attempt created. No VM is created.
func TestKVMJailStartup(t *testing.T) {
	env := jailKVMRequire(t)
	tp := kvmNetNewTopology(t, "jx")
	tp.createNamespaces(t, true)
	a := env.attempt(t, "startup", tp)
	a.stage(t)
	hub := a.startHub(t)
	a.install(t, nil)
	inv := a.start(t)
	a.assertBoundary(t, inv)
	a.assertStartupLog(t, inv)
	link := a.waitLink(t, hub)
	info, err := link.Client.Info(t.Context(), &nodev1.InfoRequest{})
	if err != nil {
		t.Fatalf("Info over the link: %v", err)
	}
	if info.GetNodeId() != a.rendered.NodeID || info.GetDefaultImage() != env.imageID {
		t.Fatalf("Info = node %q image %q, want %q and %q", info.GetNodeId(), info.GetDefaultImage(), a.rendered.NodeID, env.imageID)
	}
}
