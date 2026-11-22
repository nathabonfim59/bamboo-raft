// Package s3test starts the real S3 services the objstore conformance suite
// runs against. It is integration-test support: nothing here runs unless the
// test's environment gate is set.
package s3test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/objstore/s3"
)

const (
	// garageZone and garageCapacity describe the single-node layout.
	garageZone     = "dc1"
	garageCapacity = "1G"
	// garageRegion is the S3 region Garage serves.
	garageRegion = "garage"
	// garageBucket and garageKeyName are per-instance names; every StartGarage
	// call gets a fresh data directory.
	garageBucket  = "bbrafttest"
	garageKeyName = "bbrafttest"
	// garageStartTimeout bounds how long Garage may take to answer.
	garageStartTimeout = 60 * time.Second
)

// StartGarage starts a single-node Garage in temporary directories and
// returns a Config pointing at a fresh bucket and key.
//
// It is an integration test: it skips unless BBRAFT_TEST_S3_GARAGE=1, and
// skips when the garage binary is not in PATH. With BBRAFT_TEST_REQUIRE_ALL=1
// either a missing gate variable or a missing binary is a failure instead.
// The server runs in its own process group and is killed and reaped in
// t.Cleanup.
func StartGarage(t *testing.T) s3.Config {
	t.Helper()
	if os.Getenv("BBRAFT_TEST_S3_GARAGE") != "1" {
		if os.Getenv("BBRAFT_TEST_REQUIRE_ALL") == "1" {
			t.Fatal("BBRAFT_TEST_REQUIRE_ALL=1 but BBRAFT_TEST_S3_GARAGE is not 1")
		}
		t.Skip("set BBRAFT_TEST_S3_GARAGE=1 to run against a local garage binary")
	}
	bin, err := exec.LookPath("garage")
	if err != nil {
		if os.Getenv("BBRAFT_TEST_REQUIRE_ALL") == "1" {
			t.Fatalf("BBRAFT_TEST_REQUIRE_ALL=1 but the garage binary is not in PATH: %v", err)
		}
		t.Skipf("garage binary is not in PATH: %v", err)
	}

	dir := t.TempDir()
	metaDir := filepath.Join(dir, "meta")
	dataDir := filepath.Join(dir, "data")
	for _, d := range []string{metaDir, dataDir} {
		if err := os.MkdirAll(d, 0o750); err != nil {
			t.Fatalf("s3test: create %s: %v", d, err)
		}
	}
	secretsDir := filepath.Join(dir, "secrets")
	if err := os.MkdirAll(secretsDir, 0o700); err != nil {
		t.Fatalf("s3test: create secrets dir: %v", err)
	}
	rpcSecret := filepath.Join(secretsDir, "rpc_secret")
	adminToken := filepath.Join(secretsDir, "admin_token")
	metricsToken := filepath.Join(secretsDir, "metrics_token")
	for _, path := range []string{rpcSecret, adminToken, metricsToken} {
		writeSecret(t, path, randomHex(t, 32))
	}
	ports := freePorts(t, 3)
	cfgPath := filepath.Join(dir, "garage.toml")
	toml := fmt.Sprintf(`metadata_dir = %q
data_dir = %q
db_engine = "lmdb"
replication_factor = 1
metadata_fsync = false
data_fsync = false
compression_level = "none"
rpc_bind_addr = "127.0.0.1:%d"
rpc_public_addr = "127.0.0.1:%d"
rpc_secret_file = %q

[s3_api]
s3_region = %q
api_bind_addr = "127.0.0.1:%d"

[admin]
api_bind_addr = "127.0.0.1:%d"
admin_token_file = %q
metrics_token_file = %q
`, metaDir, dataDir, ports[0], ports[0], rpcSecret, garageRegion, ports[1], ports[2], adminToken, metricsToken)
	if err := os.WriteFile(cfgPath, []byte(toml), 0o600); err != nil {
		t.Fatalf("s3test: write garage.toml: %v", err)
	}

	logPath := filepath.Join(dir, "garage.log")
	proc := startGarageServer(t, bin, cfgPath, logPath)
	waitGarage(t, bin, cfgPath, proc, logPath)

	nodeID := strings.TrimSpace(runGarage(t, bin, cfgPath, "node", "id", "-q"))
	nodeID, _, _ = strings.Cut(nodeID, "@")
	if nodeID == "" {
		t.Fatal("s3test: garage did not report a node id")
	}
	runGarage(t, bin, cfgPath, "layout", "assign", "-z", garageZone, "-c", garageCapacity, nodeID)
	runGarage(t, bin, cfgPath, "layout", "apply", "--version", "1")
	keyOutput := runGarageRedacted(t, bin, cfgPath, "key", "create", garageKeyName)
	accessKey, secretKey := parseKeyOutput(keyOutput)
	if accessKey == "" || secretKey == "" {
		t.Fatal("s3test: garage key create did not print a key id and secret")
	}
	// Bucket operations can race the layout apply on a slow disk.
	for attempt := 1; ; attempt++ {
		_, err := garageOutput(t, bin, cfgPath, "bucket", "create", garageBucket)
		if err == nil {
			break
		}
		if attempt == 20 {
			t.Fatalf("s3test: garage bucket create kept failing: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	runGarage(t, bin, cfgPath, "bucket", "allow", "--read", "--write", "--owner", garageBucket, "--key", garageKeyName)

	return s3.Config{
		Endpoint:        fmt.Sprintf("http://127.0.0.1:%d", ports[1]),
		Region:          garageRegion,
		Bucket:          garageBucket,
		PathStyle:       true,
		AccessKeyID:     accessKey,
		SecretAccessKey: secretKey,
	}
}

// garageProcess is one garage server and its log.
type garageProcess struct {
	cmd     *exec.Cmd
	exited  chan struct{}
	mu      sync.Mutex
	waitErr error
}

// startGarageServer starts "garage server" in its own process group with its
// output in logPath.
func startGarageServer(t *testing.T, bin, cfgPath, logPath string) *garageProcess {
	t.Helper()
	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("s3test: create garage log: %v", err)
	}
	cmd := exec.Command(bin, "-c", cfgPath, "server")
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		t.Fatalf("s3test: start garage: %v", err)
	}
	proc := &garageProcess{cmd: cmd, exited: make(chan struct{})}
	go func() {
		err := cmd.Wait()
		proc.mu.Lock()
		proc.waitErr = err
		proc.mu.Unlock()
		close(proc.exited)
	}()
	t.Cleanup(func() {
		// The whole process group, so no helper child outlives the test.
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
		select {
		case <-proc.exited:
		case <-time.After(10 * time.Second):
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
			<-proc.exited
		}
		_ = logFile.Close()
	})
	return proc
}

// waitGarage polls "garage status" until the server answers.
func waitGarage(t *testing.T, bin, cfgPath string, proc *garageProcess, logPath string) {
	t.Helper()
	deadline := time.Now().Add(garageStartTimeout)
	for {
		select {
		case <-proc.exited:
			proc.mu.Lock()
			err := proc.waitErr
			proc.mu.Unlock()
			t.Fatalf("s3test: garage exited before becoming ready (%v)\n%s", err, logTail(logPath, 16<<10))
		default:
		}
		if _, err := garageOutput(t, bin, cfgPath, "status"); err == nil {
			return
		}
		if time.Now().After(deadline) {
			status, _ := garageOutput(t, bin, cfgPath, "status")
			t.Fatalf("s3test: garage did not become ready within %v; garage status:\n%s\ngarage log:\n%s",
				garageStartTimeout, status, logTail(logPath, 16<<10))
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// runGarage runs a garage command and fails the test with its output on
// error.
func runGarage(t *testing.T, bin, cfgPath string, args ...string) string {
	t.Helper()
	out, err := garageOutput(t, bin, cfgPath, args...)
	if err != nil {
		t.Fatalf("s3test: garage %s failed: %v\n%s", strings.Join(args, " "), err, out)
	}
	return out
}

// runGarageRedacted is runGarage for a command whose output carries a secret
// (key create): a failure reports the command, never the output.
func runGarageRedacted(t *testing.T, bin, cfgPath string, args ...string) string {
	t.Helper()
	out, err := garageOutput(t, bin, cfgPath, args...)
	if err != nil {
		t.Fatalf("s3test: garage %s failed: %v", strings.Join(args, " "), err)
	}
	return out
}

// garageOutput runs one garage command with a timeout.
func garageOutput(t *testing.T, bin, cfgPath string, args ...string) (string, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, append([]string{"-c", cfgPath}, args...)...)
	out, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		return string(out), fmt.Errorf("timed out: %w", ctx.Err())
	}
	return string(out), err
}

// parseKeyOutput extracts the key id and secret from garage's key create
// output.
func parseKeyOutput(out string) (accessKey, secretKey string) {
	for line := range strings.Lines(out) {
		line = strings.TrimSpace(line)
		if v, ok := strings.CutPrefix(line, "Key ID:"); ok && accessKey == "" {
			accessKey = strings.TrimSpace(v)
		}
		if v, ok := strings.CutPrefix(line, "Secret key:"); ok && secretKey == "" {
			secretKey = strings.TrimSpace(v)
		}
	}
	return accessKey, secretKey
}

// freePorts reserves n distinct loopback ports. The listeners are closed
// before Garage starts; Garage binds them immediately, so the race window is
// small and a collision shows up as a startup failure.
func freePorts(t *testing.T, n int) []int {
	t.Helper()
	ports := make([]int, 0, n)
	seen := map[int]bool{}
	for len(ports) < n {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("s3test: reserve a port: %v", err)
		}
		port := l.Addr().(*net.TCPAddr).Port
		_ = l.Close()
		if !seen[port] {
			seen[port] = true
			ports = append(ports, port)
		}
	}
	return ports
}

// randomHex returns n random bytes as hex.
func randomHex(t *testing.T, n int) string {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("s3test: randomness: %v", err)
	}
	return hex.EncodeToString(b)
}

// writeSecret writes a Garage secret file readable only by its owner.
func writeSecret(t *testing.T, path, value string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(value), 0o600); err != nil {
		t.Fatalf("s3test: write %s: %v", path, err)
	}
}

// logTail returns the last n bytes of a file.
func logTail(path string, n int) string {
	b, err := os.ReadFile(path)
	if err != nil {
		return "(no garage log)"
	}
	if len(b) > n {
		b = b[len(b)-n:]
	}
	return string(b)
}
