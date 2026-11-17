package server_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc"
	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/compat"
	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/db/dbtest"
	"github.com/nathabonfim59/bamboo-raft/internal/nodefake"
	"github.com/nathabonfim59/bamboo-raft/internal/nodelink"
)

// daemonProcessEnvSentinel is a sandbox env value that must never appear in a
// response body or a daemon log.
const daemonProcessEnvSentinel = "daemon-sentinel-env-value"

// daemonProcessLogLimit bounds one child's captured output. Daemons are
// expected to be quiet; the bound keeps a misbehaving child from consuming
// test memory.
const daemonProcessLogLimit = 256 << 10

// daemonProcessBuildTimeout bounds the one real binary build.
const daemonProcessBuildTimeout = 180 * time.Second

// daemonProcessStopTimeout bounds one daemon's SIGTERM-to-reap window.
const daemonProcessStopTimeout = 15 * time.Second

// daemonProcessInventoryTimeout bounds the nodehub inventory poll, whose
// production interval is nodegateway.DefaultInventoryInterval (30s).
const daemonProcessInventoryTimeout = 45 * time.Second

// daemonProcessKey is one entry of the configured API key file.
type daemonProcessKey struct {
	ID    string `json:"id"`
	Owner string `json:"owner"`
	Key   string `json:"key"`
}

// daemonProcessKeysFile is the configured API key file shape.
type daemonProcessKeysFile struct {
	Keys []daemonProcessKey `json:"keys"`
}

// daemonProcessNodeToken is one entry of the configured node token file.
type daemonProcessNodeToken struct {
	ID          string `json:"id"`
	TokenSHA256 string `json:"tokenSha256"`
}

// daemonProcessTokensFile is the configured node token file shape.
type daemonProcessTokensFile struct {
	Nodes []daemonProcessNodeToken `json:"nodes"`
}

// daemonProcessFiles are the fixed config files a child may read. Their
// contents are never printed.
type daemonProcessFiles struct {
	master      string
	keys        string
	badKeys     string
	tokens      string
	gateway     string
	dbAPI       string
	dbLifecycle string
	dbNodeHub   string
	dbAll       string
	dbBadAPI    string
}

// daemonProcessSecret is one value that must not reach child logs or API
// response bodies.
type daemonProcessSecret struct {
	label string
	value string
}

// daemonProcessSecrets holds the per-fixture sentinel lists. The label is
// safe to print; the value never is.
type daemonProcessSecrets struct {
	logs   []daemonProcessSecret
	bodies []daemonProcessSecret
}

func (s *daemonProcessSecrets) addLog(label, value string) {
	if value != "" {
		s.logs = append(s.logs, daemonProcessSecret{label: label, value: value})
	}
}

func (s *daemonProcessSecrets) addBody(label, value string) {
	if value != "" {
		s.bodies = append(s.bodies, daemonProcessSecret{label: label, value: value})
	}
}

func (s *daemonProcessSecrets) hit(list []daemonProcessSecret, data []byte) (string, bool) {
	for _, secret := range list {
		if bytes.Contains(data, []byte(secret.value)) {
			return secret.label, true
		}
	}
	return "", false
}

// daemonProcessLog is a race-safe bounded child output sink. It never dumps
// itself: callers sweep it for sentinels first and prefer fixed failures.
type daemonProcessLog struct {
	mu  sync.Mutex
	buf []byte
}

func (l *daemonProcessLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.buf) < daemonProcessLogLimit {
		n := min(len(p), daemonProcessLogLimit-len(l.buf))
		l.buf = append(l.buf, p[:n]...)
	}
	return len(p), nil
}

func (l *daemonProcessLog) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return string(l.buf)
}

// daemonProcess is one real child process. Wait is called exactly once in a
// goroutine, so done closing means the child is reaped.
type daemonProcess struct {
	name string
	cmd  *exec.Cmd
	log  *daemonProcessLog
	done chan struct{}
	pid  int
	// expectCleanExit requires exit code 0 during cleanup. Expected startup
	// failures opt out and assert their own nonzero code.
	expectCleanExit bool
}

func (p *daemonProcess) wait() {
	_ = p.cmd.Wait()
	close(p.done)
}

func (p *daemonProcess) exited() bool {
	select {
	case <-p.done:
		return true
	default:
		return false
	}
}

func (p *daemonProcess) exitCode() int {
	if p.cmd.ProcessState == nil {
		return -1
	}
	return p.cmd.ProcessState.ExitCode()
}

// stopAndWait signals SIGTERM, waits for a bounded clean exit and kills plus
// reaps on every error path. WaitDelay bounds post-exit pipe cleanup, so the
// final receive is unconditional.
func (p *daemonProcess) stopAndWait(timeout time.Duration) error {
	if p.cmd.Process == nil {
		return nil
	}
	signalErr := p.cmd.Process.Signal(syscall.SIGTERM)
	signalFailed := signalErr != nil && !errors.Is(signalErr, os.ErrProcessDone)
	if signalFailed {
		_ = p.cmd.Process.Kill()
	}
	select {
	case <-p.done:
		if signalFailed {
			return errors.New("could not signal the child; killed and reaped")
		}
		return nil
	case <-time.After(timeout):
		_ = p.cmd.Process.Kill()
		// WaitDelay bounds the post-exit pipe copy, so Wait must return.
		<-p.done
		return errors.New("did not stop after SIGTERM")
	}
}

// waitExit waits for a child that is expected to exit on its own. A timeout
// kills and reaps it before failing.
func (p *daemonProcess) waitExit(timeout time.Duration) error {
	select {
	case <-p.done:
		return nil
	case <-time.After(timeout):
		_ = p.cmd.Process.Kill()
		// WaitDelay bounds the post-exit pipe copy, so Wait must return.
		<-p.done
		return errors.New("did not exit")
	}
}

// daemonProcessResponse is one recorded HTTP answer. The body is kept for the
// v1/extension validators but never printed on failure.
type daemonProcessResponse struct {
	status int
	header http.Header
	body   []byte
}

// daemonProcessFixture is one isolated schema, inspection store, config file
// set and node identity shared by the processes of one subtest.
type daemonProcessFixture struct {
	binary    string
	root      string
	v1        *compat.Validator
	extension *compat.Validator

	owner      string
	apiKey     string
	otherKey   string
	nodeID     string
	nodeToken  string
	files      *daemonProcessFiles
	inspection *db.Store
	secrets    *daemonProcessSecrets

	apiAddr     string
	nodeAddr    string
	gatewayAddr string
	apiPort     uint16
	nodePort    uint16
	gatewayPort uint16

	apiApp       string
	lifecycleApp string
	nodehubApp   string
	allApp       string

	nodeRoot   string
	httpClient *http.Client

	processes map[string]*daemonProcess
	requests  int
	ownerID   uuid.UUID
}

// newDaemonProcessFixture creates one isolated schema with a known install
// master, one inspection store and the fixed config files. Cleanup is
// registered in the order required by the brief: schema last, inspection store
// before it, log sweep after the node link and every child.
func newDaemonProcessFixture(t *testing.T, binary, root string, v1, extension *compat.Validator) *daemonProcessFixture {
	t.Helper()
	schema, base := daemonProcessNewSchema(t)
	suffix := strings.TrimPrefix(schema, "bbraft_daemontest_")

	var master [32]byte
	if _, err := rand.Read(master[:]); err != nil {
		t.Fatal("master key randomness failed")
	}
	dbSecrets, err := db.NewSecrets(master[:])
	if err != nil {
		t.Fatal("master key setup failed")
	}
	inspectDSN := daemonProcessDSN(t, base, schema, "bbsmoke_inspect_"+suffix)
	inspection, err := db.Open(t.Context(), inspectDSN, dbSecrets)
	if err != nil {
		t.Fatal("inspection store open failed")
	}
	t.Cleanup(inspection.Close)
	if err := inspection.Migrate(t.Context()); err != nil {
		t.Fatal("inspection store migrate failed")
	}

	dir := t.TempDir()
	files := &daemonProcessFiles{}
	files.master = daemonProcessWriteFile(t, dir, "master.key", master[:])

	owner := "owner-" + suffix
	otherOwner := "other-owner-" + suffix
	apiKey := "bmb_" + suffix + "-main"
	otherKey := "bmb_" + suffix + "-other"
	badKey := "wrong-prefix-key"

	keys, err := json.Marshal(daemonProcessKeysFile{Keys: []daemonProcessKey{
		{ID: "main", Owner: owner, Key: apiKey},
		{ID: "other", Owner: otherOwner, Key: otherKey},
	}})
	if err != nil {
		t.Fatal("api key file encode failed")
	}
	files.keys = daemonProcessWriteFile(t, dir, "keys.json", keys)
	badKeys, err := json.Marshal(daemonProcessKeysFile{Keys: []daemonProcessKey{
		{ID: "bad", Owner: "bad-owner-" + suffix, Key: badKey},
	}})
	if err != nil {
		t.Fatal("bad api key file encode failed")
	}
	files.badKeys = daemonProcessWriteFile(t, dir, "bad-keys.json", badKeys)

	nodeID := "daemon-smoke-" + suffix
	nodeToken := "node-" + suffix
	gatewayToken := "gateway-" + suffix
	sum := sha256.Sum256([]byte(nodeToken))
	tokens, err := json.Marshal(daemonProcessTokensFile{Nodes: []daemonProcessNodeToken{
		{ID: nodeID, TokenSHA256: hex.EncodeToString(sum[:])},
	}})
	if err != nil {
		t.Fatal("node token file encode failed")
	}
	files.tokens = daemonProcessWriteFile(t, dir, "tokens.json", tokens)
	files.gateway = daemonProcessWriteFile(t, dir, "gateway.token", []byte(gatewayToken))

	files.dbAPI = daemonProcessWriteFile(t, dir, "db-api", []byte(daemonProcessDSN(t, base, schema, "bbsmoke_api_"+suffix)))
	files.dbLifecycle = daemonProcessWriteFile(t, dir, "db-lifecycle", []byte(daemonProcessDSN(t, base, schema, "bbsmoke_lifecycle_"+suffix)))
	files.dbNodeHub = daemonProcessWriteFile(t, dir, "db-nodehub", []byte(daemonProcessDSN(t, base, schema, "bbsmoke_nodehub_"+suffix)))
	files.dbAll = daemonProcessWriteFile(t, dir, "db-all", []byte(daemonProcessDSN(t, base, schema, "bbsmoke_all_"+suffix)))
	files.dbBadAPI = daemonProcessWriteFile(t, dir, "db-badapi", []byte(daemonProcessDSN(t, base, schema, "bbsmoke_badapi_"+suffix)))

	addrs := daemonProcessFreeAddrs(t, 3)
	apiAddr, nodeAddr, gatewayAddr := addrs[0], addrs[1], addrs[2]

	secrets := &daemonProcessSecrets{}
	for _, secret := range []daemonProcessSecret{
		{label: "api key", value: apiKey},
		{label: "other api key", value: otherKey},
		{label: "node token", value: nodeToken},
		{label: "gateway token", value: gatewayToken},
		{label: "database URL", value: base},
		{label: "master key", value: string(master[:])},
		{label: "sandbox env value", value: daemonProcessEnvSentinel},
		{label: "refused key", value: badKey},
	} {
		secrets.addLog(secret.label, secret.value)
	}
	for _, dsn := range []string{inspectDSN, readDaemonProcessFile(t, files.dbAPI), readDaemonProcessFile(t, files.dbLifecycle), readDaemonProcessFile(t, files.dbNodeHub), readDaemonProcessFile(t, files.dbAll), readDaemonProcessFile(t, files.dbBadAPI)} {
		secrets.addLog("database URL", dsn)
	}
	for _, secret := range []daemonProcessSecret{
		{label: "api key", value: apiKey},
		{label: "other api key", value: otherKey},
		{label: "node token", value: nodeToken},
		{label: "gateway token", value: gatewayToken},
		{label: "database URL", value: base},
		{label: "master key", value: string(master[:])},
		{label: "sandbox env value", value: daemonProcessEnvSentinel},
	} {
		secrets.addBody(secret.label, secret.value)
	}

	transport := http.DefaultTransport.(*http.Transport).Clone()
	// The API is loopback-only; an inherited proxy must never intercept it.
	transport.Proxy = nil
	httpClient := &http.Client{Timeout: 20 * time.Second, Transport: transport}
	t.Cleanup(transport.CloseIdleConnections)

	f := &daemonProcessFixture{
		binary:       binary,
		root:         root,
		v1:           v1,
		extension:    extension,
		owner:        owner,
		apiKey:       apiKey,
		otherKey:     otherKey,
		nodeID:       nodeID,
		nodeToken:    nodeToken,
		files:        files,
		inspection:   inspection,
		secrets:      secrets,
		apiAddr:      apiAddr,
		nodeAddr:     nodeAddr,
		gatewayAddr:  gatewayAddr,
		apiPort:      daemonProcessPort(t, apiAddr),
		nodePort:     daemonProcessPort(t, nodeAddr),
		gatewayPort:  daemonProcessPort(t, gatewayAddr),
		apiApp:       "bbsmoke_api_" + suffix,
		lifecycleApp: "bbsmoke_lifecycle_" + suffix,
		nodehubApp:   "bbsmoke_nodehub_" + suffix,
		allApp:       "bbsmoke_all_" + suffix,
		nodeRoot:     t.TempDir(),
		httpClient:   httpClient,
		processes:    map[string]*daemonProcess{},
	}
	t.Cleanup(func() { f.sweep(t) })
	return f
}

// readDaemonProcessFile reads one fixture file only for the sentinel list. Its
// contents never reach test output.
func readDaemonProcessFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal("fixture file read failed")
	}
	return string(data)
}

// sweep runs after every child and the node link are stopped. It proves the
// children were reaped and checks every captured log for credentials or
// sentinels before the inspection store closes.
func (f *daemonProcessFixture) sweep(t *testing.T) {
	t.Helper()
	for name, process := range f.processes {
		if !process.exited() {
			t.Errorf("%s: process was not reaped", name)
			continue
		}
		if label, hit := f.secrets.hit(f.secrets.logs, []byte(process.log.String())); hit {
			t.Errorf("%s: log leaked %s", name, label)
		}
	}
}

// apiBase is the trusted API URL, also used as the signed upload base URL.
func (f *daemonProcessFixture) apiBase() string { return "http://" + f.apiAddr }

// args builds the fixed accepted flags for one role. Secret values are only
// referenced through files.
func (f *daemonProcessFixture) args(role, dbFile string) []string {
	return f.argsWithKeys(role, dbFile, f.files.keys)
}

func (f *daemonProcessFixture) argsWithKeys(role, dbFile, keysFile string) []string {
	args := []string{
		"-roles", role,
		"-server.listen", f.apiAddr,
		"-server.public-url", f.apiBase(),
		"-node.listen", f.nodeAddr,
		"-node.gateway-listen", f.gatewayAddr,
		"-node.gateway-dial", f.gatewayAddr,
		"-database.url-file", dbFile,
		"-secrets.master-key-file", f.files.master,
		"-node.gateway-token-file", f.files.gateway,
		"-commands.stream-max-seconds", "901",
		"-lifecycle.retry-seconds", "1",
		"-lifecycle.reaper-seconds", "1",
		"-lifecycle.job-poll-seconds", "1",
	}
	switch role {
	case "api":
		args = append(args, "-auth.keys-file", keysFile)
	case "nodehub":
		args = append(args, "-node.tokens-file", f.files.tokens)
	case "all":
		args = append(args, "-auth.keys-file", keysFile, "-node.tokens-file", f.files.tokens)
	}
	return args
}

// startProcess launches one actual daemon with a sanitized environment and
// registers bounded cleanup.
func (f *daemonProcessFixture) startProcess(t *testing.T, name string, args []string) *daemonProcess {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), f.binary, args...)
	cmd.Dir = f.root
	cmd.Env = daemonProcessEnv()
	var log daemonProcessLog
	cmd.Stdout = io.Discard
	cmd.Stderr = &log
	p := &daemonProcess{name: name, cmd: cmd, log: &log, done: make(chan struct{}), expectCleanExit: true}
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		err := cmd.Process.Signal(syscall.SIGTERM)
		if errors.Is(err, os.ErrProcessDone) {
			return nil
		}
		return err
	}
	cmd.WaitDelay = 10 * time.Second
	if err := cmd.Start(); err != nil {
		t.Fatalf("%s: start failed", name)
	}
	p.pid = cmd.Process.Pid
	go p.wait()
	t.Cleanup(func() {
		if err := p.stopAndWait(daemonProcessStopTimeout); err != nil {
			t.Errorf("%s: %v", name, err)
			return
		}
		if p.expectCleanExit {
			if code := p.exitCode(); code != 0 {
				t.Errorf("%s exited with code %d", name, code)
			}
		}
	})
	f.processes[name] = p
	return p
}

func (f *daemonProcessFixture) startNodeHub(t *testing.T) *daemonProcess {
	return f.startProcess(t, "nodehub", f.args("nodehub", f.files.dbNodeHub))
}

func (f *daemonProcessFixture) startLifecycle(t *testing.T) *daemonProcess {
	return f.startProcess(t, "lifecycle", f.args("lifecycle", f.files.dbLifecycle))
}

func (f *daemonProcessFixture) startAPI(t *testing.T, name string) *daemonProcess {
	return f.startProcess(t, name, f.args("api", f.files.dbAPI))
}

func (f *daemonProcessFixture) startAll(t *testing.T) *daemonProcess {
	return f.startProcess(t, "all", f.args("all", f.files.dbAll))
}

// startNode runs the real nodefake in the parent and dials the nodehub
// subprocess over the accepted nodelink transport.
func (f *daemonProcessFixture) startNode(t *testing.T) {
	t.Helper()
	node := nodefake.New(f.nodeID, f.nodeRoot)
	server := grpc.NewServer(nodelink.ServerOptions()...)
	node.Register(server)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = nodelink.Serve(ctx, nodelink.DialConfig{
			Addr:       f.nodeAddr,
			NodeID:     f.nodeID,
			Token:      f.nodeToken,
			Log:        slog.New(slog.DiscardHandler),
			MinBackoff: 20 * time.Millisecond,
			MaxBackoff: 100 * time.Millisecond,
		}, server)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
			return
		case <-time.After(10 * time.Second):
		}
		// The link ignored cancellation: force the gRPC server down and
		// join the Serve goroutine rather than abandoning it.
		server.Stop()
		<-done
	})
}

// waitFor is a bounded poll; it never sleeps before the first check and never
// uses a fixed readiness delay.
func (f *daemonProcessFixture) waitFor(t *testing.T, timeout time.Duration, what string, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if t.Context().Err() != nil {
			t.Fatalf("waiting for %s: test context ended", what)
		}
		if ready() {
			return
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// waitAPI blocks until the API process serves its liveness endpoint.
func (f *daemonProcessFixture) waitAPI(t *testing.T) {
	t.Helper()
	f.waitFor(t, 15*time.Second, "api readiness", func() bool {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		defer cancel()
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, f.apiBase()+"/healthz", nil)
		if err != nil {
			return false
		}
		response, err := f.httpClient.Do(request)
		if err != nil {
			return false
		}
		defer response.Body.Close()
		_, _ = io.Copy(io.Discard, response.Body)
		return response.StatusCode == http.StatusOK
	})
}

// waitDBBackend proves the named role opened its own pool to this schema.
func (f *daemonProcessFixture) waitDBBackend(t *testing.T, app string) {
	t.Helper()
	f.waitFor(t, 15*time.Second, "database backend", func() bool {
		var count int
		err := f.inspection.Pool().QueryRow(t.Context(),
			`SELECT count(*) FROM pg_stat_activity
			 WHERE datname = current_database() AND application_name = $1`, app).Scan(&count)
		return err == nil && count > 0
	})
}

// waitNodeFresh waits for a real inventory write. after may be zero for the
// initial barrier or the API-stop observation time for the survival proof.
func (f *daemonProcessFixture) waitNodeFresh(t *testing.T, after time.Time) {
	t.Helper()
	f.waitFor(t, daemonProcessInventoryTimeout, "node inventory", func() bool {
		node, err := f.inspection.Node(t.Context(), f.nodeID)
		return err == nil && node.LastSeenAt.After(after) && time.Since(node.LastSeenAt) < time.Minute
	})
}

// assertListenerOwnership proves one process owns exactly the listeners of
// its role. The API process must never own the node or gateway listeners.
func (f *daemonProcessFixture) assertListenerOwnership(t *testing.T, process *daemonProcess, want, forbid []uint16) {
	t.Helper()
	f.waitFor(t, 15*time.Second, "listener ownership", func() bool {
		ports, err := daemonProcessListeningPorts(process.pid)
		if err != nil {
			return false
		}
		for _, port := range want {
			if !ports[port] {
				return false
			}
		}
		return true
	})
	ports, err := daemonProcessListeningPorts(process.pid)
	if err != nil {
		t.Fatalf("%s: listener inspection failed", process.name)
	}
	for _, port := range forbid {
		if ports[port] {
			t.Fatalf("%s: owns a listener it does not run", process.name)
		}
	}
}

// assertDistinctProcesses proves the named roles are separate live OS
// processes, not goroutines of one process.
func (f *daemonProcessFixture) assertDistinctProcesses(t *testing.T, names ...string) {
	t.Helper()
	seen := map[int]string{}
	for _, name := range names {
		process := f.processes[name]
		if process == nil {
			t.Fatalf("%s: process was not started", name)
		}
		if !daemonProcessAlive(process.pid) {
			t.Fatalf("%s: process is not alive", name)
		}
		if other, ok := seen[process.pid]; ok {
			t.Fatalf("%s and %s share a process", name, other)
		}
		seen[process.pid] = name
	}
}

// waitPortClosed proves a reserved loopback address has no listener.
func (f *daemonProcessFixture) waitPortClosed(t *testing.T, addr, what string) {
	t.Helper()
	f.waitFor(t, 5*time.Second, what, func() bool {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err != nil {
			return true
		}
		_ = conn.Close()
		return false
	})
}

// stopAPI sends SIGTERM to the API child only, requires a clean exit and
// returns the time after the listener was observed closed.
func (f *daemonProcessFixture) stopAPI(t *testing.T) time.Time {
	t.Helper()
	process := f.processes["api"]
	if process == nil {
		t.Fatal("api process was not started")
	}
	if err := process.stopAndWait(daemonProcessStopTimeout); err != nil {
		t.Fatalf("api: %v", err)
	}
	if code := process.exitCode(); code != 0 {
		t.Fatalf("api exited with code %d after SIGTERM", code)
	}
	f.waitPortClosed(t, f.apiAddr, "api listener closed")
	if !daemonProcessAlive(f.processes["nodehub"].pid) {
		t.Fatal("nodehub died with the api")
	}
	if !daemonProcessAlive(f.processes["lifecycle"].pid) {
		t.Fatal("lifecycle died with the api")
	}
	return time.Now()
}

// doJSON sends one authenticated JSON request. label names the operation and
// never contains a secret.
func (f *daemonProcessFixture) doJSON(t *testing.T, label, method, path, key string, body []byte, headers map[string]string) daemonProcessResponse {
	t.Helper()
	contentType := ""
	if body != nil {
		contentType = "application/json"
	}
	return f.doURL(t, label, method, f.apiBase()+path, key, body, contentType, headers)
}

// doURL sends one request to an absolute URL. It never prints the URL: a
// signed upload URL carries a bearer-equivalent token.
func (f *daemonProcessFixture) doURL(t *testing.T, label, method, rawURL, key string, body []byte, contentType string, headers map[string]string) daemonProcessResponse {
	t.Helper()
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	request, err := http.NewRequestWithContext(t.Context(), method, rawURL, reader)
	if err != nil {
		t.Fatalf("%s: request could not be built", label)
	}
	if key != "" {
		request.Header.Set("Authorization", "Bearer "+key)
	}
	if contentType != "" {
		request.Header.Set("Content-Type", contentType)
	}
	for name, value := range headers {
		request.Header.Set(name, value)
	}
	response, err := f.httpClient.Do(request)
	if err != nil {
		t.Fatalf("%s: request failed", label)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("%s: response could not be read", label)
	}
	f.requests++
	if secret, hit := f.secrets.hit(f.secrets.bodies, data); hit {
		t.Fatalf("%s: response leaked %s", label, secret)
	}
	return daemonProcessResponse{status: response.StatusCode, header: response.Header.Clone(), body: data}
}

// check validates one response against a loaded document. Validator errors
// are not printed because they can echo response values.
func daemonProcessCheck(t *testing.T, validator *compat.Validator, operation string, response daemonProcessResponse) {
	t.Helper()
	if err := validator.Response(operation, response.status, response.header, response.body); err != nil {
		t.Errorf("%s response %d failed spec validation", operation, response.status)
	}
}

// waitJobDone polls the newest owner-scoped job row until it is durably done.
// The ready state and the completed deletion operation are both published
// before CompleteJob, so reservation assertions must wait on this barrier.
func (f *daemonProcessFixture) waitJobDone(t *testing.T, sandboxID string, kind db.JobKind) {
	t.Helper()
	f.waitFor(t, 15*time.Second, "job done", func() bool {
		var state string
		err := f.inspection.Pool().QueryRow(t.Context(),
			`SELECT state FROM jobs
			 WHERE sandbox_id = $1 AND owner_id = $2 AND kind = $3
			 ORDER BY created_at DESC LIMIT 1`, sandboxID, f.ownerID, string(kind)).Scan(&state)
		return err == nil && state == string(db.JobDone)
	})
}

// waitDeletion polls the owner-scoped deletion operation until completed.
func (f *daemonProcessFixture) waitDeletion(t *testing.T, operationID string) daemonProcessResponse {
	t.Helper()
	var last daemonProcessResponse
	f.waitFor(t, 20*time.Second, "deletion completed", func() bool {
		last = f.doJSON(t, "deletion operation", http.MethodGet, "/api/v1/deletion-operations/"+operationID, f.apiKey, nil, nil)
		if last.status != http.StatusOK {
			return false
		}
		var body struct {
			Operation struct {
				Status string `json:"status"`
			} `json:"operation"`
		}
		if json.Unmarshal(last.body, &body) != nil {
			return false
		}
		return body.Operation.Status == "completed"
	})
	return last
}

// daemonProcessNewSchema creates one isolated schema and registers its drop
// as the earliest cleanup, so it runs after every child and store is gone.
func daemonProcessNewSchema(t *testing.T) (schema, base string) {
	t.Helper()
	cfg := dbtest.PoolConfig(t)
	schema = "bbraft_daemontest_" + daemonProcessRandomHex(t, 6)
	base = os.Getenv("BBRAFT_TEST_DATABASE_URL")
	ctx := t.Context()
	admin, err := pgx.ConnectConfig(ctx, cfg.ConnConfig)
	if err != nil {
		t.Fatal("schema connect failed")
	}
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); err != nil {
		admin.Close(context.WithoutCancel(ctx))
		t.Fatal("schema create failed")
	}
	admin.Close(context.WithoutCancel(ctx))
	t.Cleanup(func() {
		dropCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		drop, err := pgx.ConnectConfig(dropCtx, cfg.ConnConfig)
		if err != nil {
			t.Error("schema drop connect failed")
			return
		}
		_, dropErr := drop.Exec(dropCtx, "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
		drop.Close(context.WithoutCancel(dropCtx))
		if dropErr != nil {
			t.Error("schema drop failed")
		}
	})
	return schema, base
}

// daemonProcessDSN points one pool at the isolated schema and gives it a
// role-specific application_name for independent-backend assertions. The
// result is secret: it can carry a password.
func daemonProcessDSN(t *testing.T, base, schema, app string) string {
	t.Helper()
	if strings.Contains(base, "://") {
		parsed, err := url.Parse(base)
		if err != nil {
			t.Fatal("test database URL is not a URL")
		}
		values := parsed.Query()
		values.Set("search_path", schema)
		values.Set("application_name", app)
		parsed.RawQuery = values.Encode()
		return parsed.String()
	}
	return base + " search_path=" + schema + " application_name=" + app
}

// daemonProcessFreeAddrs reserves n distinct loopback addresses at once and
// then releases them. The children bind them as their real TCP listeners.
func daemonProcessFreeAddrs(t *testing.T, n int) []string {
	t.Helper()
	listeners := make([]net.Listener, 0, n)
	addrs := make([]string, 0, n)
	for range n {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			for _, open := range listeners {
				_ = open.Close()
			}
			t.Fatal("free loopback address failed")
		}
		listeners = append(listeners, listener)
		addrs = append(addrs, listener.Addr().String())
	}
	var closeErr error
	for _, listener := range listeners {
		if err := listener.Close(); err != nil && closeErr == nil {
			closeErr = err
		}
	}
	if closeErr != nil {
		t.Fatal("free loopback address close failed")
	}
	return addrs
}

func daemonProcessPort(t *testing.T, addr string) uint16 {
	t.Helper()
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatal("test address is malformed")
	}
	value, err := strconv.ParseUint(port, 10, 16)
	if err != nil {
		t.Fatal("test address port is malformed")
	}
	return uint16(value)
}

// daemonProcessWriteFile writes one fixed config or secret file with mode
// 0600 and never prints its contents.
func daemonProcessWriteFile(t *testing.T, dir, name string, data []byte) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write %s failed", name)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatalf("chmod %s failed", name)
	}
	stat, err := os.Stat(path)
	if err != nil || !stat.Mode().IsRegular() || stat.Mode().Perm() != 0o600 {
		t.Fatalf("%s is not a 0600 regular file", name)
	}
	return path
}

// daemonProcessRandomHex returns n random bytes as lowercase hex.
func daemonProcessRandomHex(t *testing.T, n int) string {
	t.Helper()
	data := make([]byte, n)
	if _, err := rand.Read(data); err != nil {
		t.Fatal("randomness failed")
	}
	return hex.EncodeToString(data)
}

// daemonProcessEnv strips inherited BBRAFT_* and libpq settings so a child
// sees only its flags and files. HOME is deliberately preserved.
func daemonProcessEnv() []string {
	base := os.Environ()
	env := make([]string, 0, len(base))
	for _, entry := range base {
		name, _, _ := strings.Cut(entry, "=")
		if strings.HasPrefix(name, "BBRAFT_") {
			continue
		}
		switch name {
		case "PGPASSWORD", "PGSERVICE", "PGSERVICEFILE", "PGPASSFILE",
			"PGHOST", "PGPORT", "PGUSER", "PGDATABASE", "PGSSLMODE",
			"PGSSLROOTCERT", "PGSSLCERT", "PGSSLKEY":
			continue
		}
		env = append(env, entry)
	}
	return env
}

// daemonProcessBuild builds the real server binary once for the gated parent
// test. It is the only place the suite compiles a command.
func daemonProcessBuild(t *testing.T, root string) string {
	t.Helper()
	binary := filepath.Join(t.TempDir(), "bbraft-server")
	ctx, cancel := context.WithTimeout(t.Context(), daemonProcessBuildTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "go", "build", "-buildvcs=false", "-trimpath", "-o", binary, "./cmd/bbraft-server")
	cmd.Dir = root
	cmd.Env = daemonProcessEnv()
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			t.Fatal("bbraft-server build timed out")
		}
		code := -1
		if cmd.ProcessState != nil {
			code = cmd.ProcessState.ExitCode()
		}
		t.Fatalf("bbraft-server build failed (exit %d)", code)
	}
	stat, err := os.Stat(binary)
	if err != nil || !stat.Mode().IsRegular() {
		t.Fatal("bbraft-server build produced no binary")
	}
	return binary
}

// daemonProcessListeningPorts maps the listening TCP ports owned by pid. It
// reads the child's socket inodes and the shared TCP tables, which proves
// role-specific listener ownership without external tools.
func daemonProcessListeningPorts(pid int) (map[uint16]bool, error) {
	fdDir := fmt.Sprintf("/proc/%d/fd", pid)
	entries, err := os.ReadDir(fdDir)
	if err != nil {
		return nil, err
	}
	inodes := make(map[string]bool, len(entries))
	for _, entry := range entries {
		target, err := os.Readlink(filepath.Join(fdDir, entry.Name()))
		if err != nil {
			continue
		}
		inode, ok := strings.CutPrefix(target, "socket:[")
		if !ok {
			continue
		}
		inode, ok = strings.CutSuffix(inode, "]")
		if !ok {
			continue
		}
		inodes[inode] = true
	}
	ports := map[uint16]bool{}
	for _, path := range []string{"/proc/net/tcp", "/proc/net/tcp6"} {
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		for line := range strings.SplitSeq(string(data), "\n") {
			fields := strings.Fields(line)
			if len(fields) < 10 || fields[3] != "0A" {
				continue
			}
			if !inodes[fields[9]] {
				continue
			}
			_, portHex, ok := strings.Cut(fields[1], ":")
			if !ok {
				continue
			}
			port, err := strconv.ParseUint(portHex, 16, 16)
			if err != nil {
				continue
			}
			ports[uint16(port)] = true
		}
	}
	return ports, nil
}

// daemonProcessAlive reports whether pid still exists.
func daemonProcessAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}
