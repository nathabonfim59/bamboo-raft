package cli

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json/v2"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	// sshTestSandboxID and sshTestSandboxID2 are valid pinned-shaped ids.
	sshTestSandboxID  = "bx_23456789"
	sshTestSandboxID2 = "bx_87654322"
	// sshTestUser is the pinned sandbox SSH user.
	sshTestUser = "user"
	// sshTestPublicKey is the managed public key line shape sent to the API.
	sshTestPublicKey = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIExampleKeyMaterialExampleKeyMaterialExample bbraft"
)

// sshTestHostKey builds a valid ssh-ed25519 host key line with distinct key
// material per seed.
func sshTestHostKey(seed byte) string {
	publicKey := bytes.Repeat([]byte{seed}, 32)
	return "ssh-ed25519 " + base64.StdEncoding.EncodeToString(sshTestHostKeyBlob(publicKey)) + " host"
}

// sshTestHostKeyBlob builds the SSH wire blob for an ed25519 public key.
func sshTestHostKeyBlob(publicKey []byte) []byte {
	var writer sshWireWriter
	writer.stringBytes([]byte("ssh-ed25519"))
	writer.stringBytes(publicKey)
	return writer.bytes()
}

// sshTestResponse builds an sshKey success body. machineIP and endpoint may be
// nil for an explicit JSON null.
func sshTestResponse(machineIP, endpoint *string, hostKey string) map[string]any {
	response := map[string]any{
		"ok":        true,
		"type":      sshKeySuccessType,
		"sshUser":   sshTestUser,
		"hostKey":   hostKey,
		"machineIp": machineIP,
	}
	response["sshEndpoint"] = endpoint
	return response
}

// sshTestInstall runs installSSHKey against a temporary handler.
func sshTestInstall(t *testing.T, handler http.HandlerFunc, sandboxID, publicKey string) (sshTarget, error) {
	t.Helper()
	server := newSSHTestServer(t, handler)
	client, err := NewClient(server.URL, "test-key", server.Client())
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	return installSSHKey(t.Context(), client, sandboxID, publicKey)
}

// newSSHTestServer starts an httptest server closed with the test.
func newSSHTestServer(t *testing.T, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return server
}

// sshTestApp builds a cliTestEnv whose HOME points at home and whose process
// runner is injected.
func sshTestApp(t *testing.T, handler http.HandlerFunc, home string, runner ProcessRunner) *cliTestEnv {
	t.Helper()
	return cliTestNew(t, handler, func(options *Options) {
		previous := options.Getenv
		options.Getenv = func(key string) string {
			if key == "HOME" {
				return home
			}
			return previous(key)
		}
		options.RunProcess = runner
	})
}

// sshTestRecorder returns a runner that records invocations and a pointer to
// the recorded slice. It is safe for one command at a time.
// sshTestCall returns the only recorded call.
func sshTestCall(t *testing.T, calls *[]ProcessCommand) ProcessCommand {
	t.Helper()
	if len(*calls) != 1 {
		t.Fatalf("recorded %d process calls, want 1", len(*calls))
	}
	return (*calls)[0]
}

// sshTestHome prepares a temporary HOME with a private .ssh directory.
func sshTestHome(t *testing.T) string {
	t.Helper()
	home := t.TempDir()
	if err := os.MkdirAll(filepath.Join(home, ".ssh"), 0o700); err != nil {
		t.Fatalf("create home .ssh: %v", err)
	}
	return home
}

// sshTestOptionValue returns the value of the -o option starting with prefix.
func sshTestOptionValue(t *testing.T, args []string, prefix string) string {
	t.Helper()
	for i := 0; i+1 < len(args); i++ {
		if args[i] != "-o" {
			continue
		}
		if value, ok := strings.CutPrefix(args[i+1], prefix); ok {
			return value
		}
	}
	t.Fatalf("option %q not found in %q", prefix, args)
	return ""
}

// sshTestReadFile reads a small text file.
func sshTestReadFile(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(raw)
}

// sshTestKeyHandler answers one successful sshKey request.
func sshTestKeyHandler(t *testing.T, machineIP, endpoint *string, hostKey string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		cliTestWriteJSON(t, w, http.StatusOK, sshTestResponse(machineIP, endpoint, hostKey))
	}
}

// sshTestCaptured runs ssh/scp with a runner that inspects the invocation and
// its per-invocation known_hosts file while the child is "running".
type sshTestCaptured struct {
	command        ProcessCommand
	knownHosts     string
	knownHostsMode os.FileMode
	stdout         *bytes.Buffer
	stderr         *bytes.Buffer
	home           string
	env            *cliTestEnv
}

// sshTestCaptureApp builds an App whose runner records the invocation and the
// known-hosts file content, then returns code.
func sshTestCaptureApp(t *testing.T, handler http.HandlerFunc, code int) (*sshTestCaptured, *[]ProcessCommand) {
	t.Helper()
	home := sshTestHome(t)
	captured := &sshTestCaptured{home: home}
	var calls []ProcessCommand
	runner := func(_ context.Context, command ProcessCommand) (int, error) {
		captured.command = command
		if option := sshTestOptionValue(t, command.Args, "UserKnownHostsFile="); option != "" {
			path := strings.Trim(option, `"`)
			captured.knownHosts = sshTestReadFile(t, path)
			info, err := os.Stat(path)
			if err != nil {
				t.Fatalf("stat known hosts: %v", err)
			}
			captured.knownHostsMode = info.Mode().Perm()
		}
		calls = append(calls, command)
		return code, nil
	}
	env := sshTestApp(t, handler, home, runner)
	captured.env = env
	captured.stdout, captured.stderr = env.stdout, env.stderr
	return captured, &calls
}

// TestSSHInvocationPinsKeyHostAndCommand checks the exact ssh argv, the
// per-invocation known_hosts content and the pinning options.
func TestSSHInvocationPinsKeyHostAndCommand(t *testing.T) {
	hostKey := sshTestHostKey(8)
	captured, calls := sshTestCaptureApp(t, sshTestKeyHandler(t, sshTestString("203.0.113.5"), nil, hostKey), 0)
	status := captured.env.run(t, "ssh", sshTestSandboxID, "echo hi")
	if status != 0 {
		t.Fatalf("exit status = %d, want 0; stderr=%q", status, captured.stderr.String())
	}
	command := sshTestCall(t, calls)
	if command.Path != "ssh" {
		t.Fatalf("path = %q, want ssh", command.Path)
	}
	keyPath := filepath.Join(captured.home, ".ssh", sshKeyFileName)
	knownHostsOption := sshTestOptionValue(t, command.Args, "UserKnownHostsFile=")
	if !strings.HasPrefix(knownHostsOption, `"`) || !strings.HasSuffix(knownHostsOption, `"`) {
		t.Fatalf("known hosts option is not config-quoted: %q", knownHostsOption)
	}
	expected := []string{
		"-i", keyPath,
		"-o", "IdentitiesOnly=yes",
		"-o", "StrictHostKeyChecking=yes",
		"-o", "UserKnownHostsFile=" + knownHostsOption,
		"-o", "GlobalKnownHostsFile=/dev/null",
		"-o", "HostKeyAlias=" + sshTestSandboxID,
		"-o", "CheckHostIP=no",
		"-o", "VerifyHostKeyDNS=no",
		"-o", "UpdateHostKeys=no",
		"user@203.0.113.5",
		"echo hi",
	}
	if !slices.Equal(command.Args, expected) {
		t.Fatalf("args = %q, want %q", command.Args, expected)
	}
	if command.Foreground {
		t.Fatal("piped stdin marked the child foreground")
	}
	if captured.knownHostsMode != 0o600 {
		t.Fatalf("known hosts mode = %o, want 600", captured.knownHostsMode)
	}
	prefix := sshTestSandboxID + " ssh-ed25519 "
	if !strings.HasPrefix(captured.knownHosts, prefix) || !strings.HasSuffix(captured.knownHosts, "\n") {
		t.Fatalf("known hosts content = %q", captured.knownHosts)
	}
	if strings.Contains(captured.knownHosts, "host\nssh") {
		t.Fatalf("known hosts content has an injected line: %q", captured.knownHosts)
	}
	knownHostsPath := strings.Trim(knownHostsOption, `"`)
	if _, err := os.Lstat(knownHostsPath); !os.IsNotExist(err) {
		t.Fatalf("known hosts file survived the command (%v)", err)
	}
}

// TestSSHInvocationPortsAndTTY covers port flags, IPv6 brackets and -t rules.
func TestSSHInvocationPortsAndTTY(t *testing.T) {
	hostKey := sshTestHostKey(9)
	cases := []struct {
		name     string
		args     []string
		machine  *string
		endpoint *string
		wantArgs func(home string) []string
	}{
		{
			name:     "endpoint port",
			args:     []string{"ssh", sshTestSandboxID, "true"},
			endpoint: sshTestString("203.0.113.10:22001"),
			wantArgs: func(home string) []string {
				return []string{"-i", filepath.Join(home, ".ssh", sshKeyFileName), "-p", "22001",
					"-o", "IdentitiesOnly=yes", "-o", "StrictHostKeyChecking=yes",
					"-o", `UserKnownHostsFile=PLACEHOLDER`, "-o", "GlobalKnownHostsFile=/dev/null",
					"-o", "HostKeyAlias=" + sshTestSandboxID,
					"-o", "CheckHostIP=no", "-o", "VerifyHostKeyDNS=no", "-o", "UpdateHostKeys=no", "user@203.0.113.10", "true"}
			},
		},
		{
			name:     "ipv6 endpoint",
			args:     []string{"ssh", sshTestSandboxID, "true"},
			endpoint: sshTestString("[2001:db8::1]:2222"),
			wantArgs: func(home string) []string {
				return []string{"-i", filepath.Join(home, ".ssh", sshKeyFileName), "-p", "2222",
					"-o", "IdentitiesOnly=yes", "-o", "StrictHostKeyChecking=yes",
					"-o", `UserKnownHostsFile=PLACEHOLDER`, "-o", "GlobalKnownHostsFile=/dev/null",
					"-o", "HostKeyAlias=" + sshTestSandboxID,
					"-o", "CheckHostIP=no", "-o", "VerifyHostKeyDNS=no", "-o", "UpdateHostKeys=no", "user@[2001:db8::1]", "true"}
			},
		},
		{
			name: "interactive forces tty",
			args: []string{"ssh", sshTestSandboxID},
			wantArgs: func(home string) []string {
				return []string{"-t", "-i", filepath.Join(home, ".ssh", sshKeyFileName),
					"-o", "IdentitiesOnly=yes", "-o", "StrictHostKeyChecking=yes",
					"-o", `UserKnownHostsFile=PLACEHOLDER`, "-o", "GlobalKnownHostsFile=/dev/null",
					"-o", "HostKeyAlias=" + sshTestSandboxID,
					"-o", "CheckHostIP=no", "-o", "VerifyHostKeyDNS=no", "-o", "UpdateHostKeys=no", "user@203.0.113.5"}
			},
		},
		{
			name: "flag tty with command",
			args: []string{"ssh", "-t", sshTestSandboxID, "tmux attach"},
			wantArgs: func(home string) []string {
				return []string{"-t", "-i", filepath.Join(home, ".ssh", sshKeyFileName),
					"-o", "IdentitiesOnly=yes", "-o", "StrictHostKeyChecking=yes",
					"-o", `UserKnownHostsFile=PLACEHOLDER`, "-o", "GlobalKnownHostsFile=/dev/null",
					"-o", "HostKeyAlias=" + sshTestSandboxID,
					"-o", "CheckHostIP=no", "-o", "VerifyHostKeyDNS=no", "-o", "UpdateHostKeys=no", "user@203.0.113.5", "tmux attach"}
			},
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			machine := testCase.machine
			if machine == nil && testCase.endpoint == nil {
				machine = sshTestString("203.0.113.5")
			}
			captured, calls := sshTestCaptureApp(t, sshTestKeyHandler(t, machine, testCase.endpoint, hostKey), 0)
			if status := captured.env.run(t, testCase.args...); status != 0 {
				t.Fatalf("exit status = %d; stderr=%q", status, captured.stderr.String())
			}
			command := sshTestCall(t, calls)
			option := sshTestOptionValue(t, command.Args, "UserKnownHostsFile=")
			want := testCase.wantArgs(captured.home)
			for i := range want {
				if want[i] == `UserKnownHostsFile=PLACEHOLDER` {
					want[i] = "UserKnownHostsFile=" + option
				}
			}
			if !slices.Equal(command.Args, want) {
				t.Fatalf("args = %q, want %q", command.Args, want)
			}
		})
	}
}

// TestSSHCommandLineQuoting pins the remote command semantics: one argument is
// unchanged; several are POSIX-quoted into one string for the remote shell.
func TestSSHCommandLineQuoting(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want string
	}{
		{name: "empty", args: nil, want: ""},
		{name: "one shell line", args: []string{"cd /home/user/my-repo && npm test"}, want: "cd /home/user/my-repo && npm test"},
		{name: "one with quotes", args: []string{`echo "a b"`}, want: `echo "a b"`},
		{name: "argv", args: []string{"bash", "-lc", "echo $HOME"}, want: `'bash' '-lc' 'echo $HOME'`},
		{name: "embedded single quote", args: []string{"echo", "it's"}, want: `'echo' 'it'\''s'`},
		{name: "metacharacters", args: []string{"echo", "$(rm -rf /)", "`whoami`", ";", "a b"},
			want: `'echo' '$(rm -rf /)' '` + "`whoami`" + `' ';' 'a b'`},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			got, err := sshCommandLine(testCase.args)
			if err != nil {
				t.Fatalf("command line: %v", err)
			}
			if got != testCase.want {
				t.Fatalf("command line = %q, want %q", got, testCase.want)
			}
		})
	}
	t.Run("empty command word rejected", func(t *testing.T) {
		if _, err := sshCommandLine([]string{"  "}); err == nil {
			t.Fatal("whitespace command accepted")
		}
	})
	t.Run("NUL rejected", func(t *testing.T) {
		if _, err := sshCommandLine([]string{"echo", "a\x00b"}); err == nil {
			t.Fatal("NUL command accepted")
		}
	})
}

// TestSSHMultiArgumentIsOneRemoteArg proves several words reach ssh as exactly
// one argv element after the destination.
func TestSSHMultiArgumentIsOneRemoteArg(t *testing.T) {
	captured, calls := sshTestCaptureApp(t, sshTestKeyHandler(t, sshTestString("203.0.113.5"), nil, sshTestHostKey(10)), 0)
	status := captured.env.run(t, "ssh", sshTestSandboxID, "--", "bash", "-lc", "echo $HOME")
	if status != 0 {
		t.Fatalf("exit status = %d", status)
	}
	command := sshTestCall(t, calls)
	want := execCommandLine([]string{"bash", "-lc", "echo $HOME"})
	if got := command.Args[len(command.Args)-1]; got != want {
		t.Fatalf("remote command = %q, want %q", got, want)
	}
	if command.Args[len(command.Args)-2] != "user@203.0.113.5" {
		t.Fatalf("destination is not before the single remote command: %q", command.Args)
	}
}

// TestSSHCancellationPhases pins cancellation at entry, after a successful
// API answer and in safe API errors with no-API/no-child counters.
func TestSSHCancellationPhases(t *testing.T) {
	hostKey := sshTestHostKey(26)
	t.Run("before API", func(t *testing.T) {
		var apiCalls, childCalls atomic.Int64
		handler := func(w http.ResponseWriter, _ *http.Request) {
			apiCalls.Add(1)
			cliTestWriteJSON(t, w, http.StatusOK, sshTestResponse(sshTestString("203.0.113.5"), nil, hostKey))
		}
		runner := func(context.Context, ProcessCommand) (int, error) {
			childCalls.Add(1)
			return 0, nil
		}
		env := sshTestApp(t, handler, sshTestHome(t), runner)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if status := env.runContext(ctx, "ssh", sshTestSandboxID, "true"); status != 130 {
			t.Fatalf("exit status = %d, want 130", status)
		}
		if apiCalls.Load() != 0 {
			t.Fatalf("API calls = %d, want 0", apiCalls.Load())
		}
		if childCalls.Load() != 0 {
			t.Fatalf("child calls = %d, want 0", childCalls.Load())
		}
	})
	t.Run("cancel then API 200", func(t *testing.T) {
		var apiCalls, childCalls atomic.Int64
		ctx, cancel := context.WithCancel(t.Context())
		handler := func(w http.ResponseWriter, _ *http.Request) {
			apiCalls.Add(1)
			cancel()
			body, err := json.Marshal(sshTestResponse(sshTestString("203.0.113.5"), nil, hostKey))
			if err != nil {
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(body)
		}
		runner := func(context.Context, ProcessCommand) (int, error) {
			childCalls.Add(1)
			return 0, nil
		}
		env := sshTestApp(t, handler, sshTestHome(t), runner)
		if status := env.runContext(ctx, "ssh", sshTestSandboxID, "true"); status != 130 {
			t.Fatalf("exit status = %d, want 130", status)
		}
		if apiCalls.Load() != 1 {
			t.Fatalf("API calls = %d, want 1", apiCalls.Load())
		}
		if childCalls.Load() != 0 {
			t.Fatalf("child calls = %d, want 0", childCalls.Load())
		}
	})
	t.Run("rsa exponent one response", func(t *testing.T) {
		var childCalls atomic.Int64
		handler := func(w http.ResponseWriter, _ *http.Request) {
			cliTestWriteJSON(t, w, http.StatusOK,
				sshTestResponse(sshTestString("203.0.113.5"), nil, sshTestRSAHostKeyExponentOne()))
		}
		runner := func(context.Context, ProcessCommand) (int, error) {
			childCalls.Add(1)
			return 0, nil
		}
		env := sshTestApp(t, handler, sshTestHome(t), runner)
		if status := env.run(t, "ssh", sshTestSandboxID, "true"); status != 1 {
			t.Fatalf("exit status = %d, want 1", status)
		}
		if childCalls.Load() != 0 {
			t.Fatalf("child calls = %d, want 0", childCalls.Load())
		}
	})
	t.Run("safe API error", func(t *testing.T) {
		var childCalls atomic.Int64
		handler := func(w http.ResponseWriter, _ *http.Request) {
			cliTestWriteJSON(t, w, http.StatusInternalServerError,
				cliTestErrorEnvelope(http.StatusInternalServerError, "server_error", "key installation failed"))
		}
		runner := func(context.Context, ProcessCommand) (int, error) {
			childCalls.Add(1)
			return 0, nil
		}
		env := sshTestApp(t, handler, sshTestHome(t), runner)
		if status := env.run(t, "ssh", sshTestSandboxID, "true"); status != 1 {
			t.Fatalf("exit status = %d, want 1", status)
		}
		if childCalls.Load() != 0 {
			t.Fatalf("child calls = %d, want 0", childCalls.Load())
		}
		output := env.stdout.String() + env.stderr.String()
		for _, secret := range []string{sshTestPublicKey, "PRIVATE KEY", "bbraft_ed25519"} {
			if strings.Contains(output, secret) {
				t.Fatalf("error output contains %q: %q", secret, output)
			}
		}
	})
}

// TestSSHExitCodes passes the child status through without a second message.
func TestSSHExitCodes(t *testing.T) {
	for _, code := range []int{0, 42, 255} {
		t.Run(strconv.Itoa(code), func(t *testing.T) {
			captured, _ := sshTestCaptureApp(t, sshTestKeyHandler(t, sshTestString("203.0.113.5"), nil, sshTestHostKey(11)), code)
			status := captured.env.run(t, "ssh", sshTestSandboxID, "true")
			if status != code {
				t.Fatalf("exit status = %d, want %d", status, code)
			}
			if captured.stderr.Len() != 0 {
				t.Fatalf("stderr = %q, want empty", captured.stderr.String())
			}
			if captured.stdout.Len() != 0 {
				t.Fatalf("stdout = %q, want empty", captured.stdout.String())
			}
		})
	}
}

// TestSSHUsageAndAPIFailures rejects bad input before spawning and reports API
// failures without a child.
func TestSSHUsageAndAPIFailures(t *testing.T) {
	home := sshTestHome(t)
	t.Run("explicit json", func(t *testing.T) {
		called := false
		runner := func(context.Context, ProcessCommand) (int, error) {
			called = true
			return 0, nil
		}
		env := sshTestApp(t, sshTestKeyHandler(t, sshTestString("203.0.113.5"), nil, sshTestHostKey(12)), home, runner)
		if status := env.run(t, "ssh", "--json", sshTestSandboxID, "true"); status != 2 {
			t.Fatalf("exit status = %d, want 2", status)
		}
		if called {
			t.Fatal("process ran after a usage error")
		}
		if !strings.Contains(env.stderr.String(), "--json") {
			t.Fatalf("stderr = %q", env.stderr.String())
		}
	})
	t.Run("invalid id", func(t *testing.T) {
		called := false
		runner := func(context.Context, ProcessCommand) (int, error) {
			called = true
			return 0, nil
		}
		env := sshTestApp(t, sshTestKeyHandler(t, sshTestString("203.0.113.5"), nil, sshTestHostKey(13)), home, runner)
		if status := env.run(t, "ssh", "not-a-sandbox", "true"); status != 2 {
			t.Fatalf("exit status = %d, want 2", status)
		}
		if called {
			t.Fatal("process ran for an invalid id")
		}
	})
	t.Run("child output is never json", func(t *testing.T) {
		var captured ProcessCommand
		runner := func(_ context.Context, command ProcessCommand) (int, error) {
			captured = command
			return 0, nil
		}
		env := sshTestApp(t, sshTestKeyHandler(t, sshTestString("203.0.113.5"), nil, sshTestHostKey(14)), home, runner)
		if status := env.run(t, "ssh", sshTestSandboxID, "true"); status != 0 {
			t.Fatalf("exit status = %d", status)
		}
		if captured.Stdout != env.stdout || captured.Stderr != env.stderr {
			t.Fatal("child streams are not the command streams")
		}
		if env.stdout.Len() != 0 {
			t.Fatalf("stdout = %q, want raw pass-through only", env.stdout.String())
		}
	})
	t.Run("stdin pass-through", func(t *testing.T) {
		stdin := strings.NewReader("payload")
		var captured ProcessCommand
		runner := func(_ context.Context, command ProcessCommand) (int, error) {
			captured = command
			return 0, nil
		}
		env := cliTestNew(t, sshTestKeyHandler(t, sshTestString("203.0.113.5"), nil, sshTestHostKey(25)), func(options *Options) {
			previous := options.Getenv
			options.Getenv = func(key string) string {
				if key == "HOME" {
					return sshTestHome(t)
				}
				return previous(key)
			}
			options.Stdin = stdin
			options.RunProcess = runner
		})
		if status := env.run(t, "ssh", sshTestSandboxID, "true"); status != 0 {
			t.Fatalf("exit status = %d", status)
		}
		if captured.Stdin != stdin {
			t.Fatal("child stdin is not the command stdin")
		}
	})
	t.Run("api failure", func(t *testing.T) {
		called := false
		runner := func(context.Context, ProcessCommand) (int, error) {
			called = true
			return 0, nil
		}
		handler := func(w http.ResponseWriter, _ *http.Request) {
			cliTestWriteJSON(t, w, http.StatusNotFound,
				cliTestErrorEnvelope(http.StatusNotFound, "sandbox_not_found", "sandbox not found"))
		}
		env := sshTestApp(t, handler, home, runner)
		if status := env.run(t, "ssh", sshTestSandboxID, "true"); status != 1 {
			t.Fatalf("exit status = %d, want 1", status)
		}
		if called {
			t.Fatal("process ran after an API failure")
		}
		if !strings.Contains(env.stdout.String(), `"code":"sandbox_not_found"`) {
			t.Fatalf("stdout = %q, want JSON error event", env.stdout.String())
		}
	})
	t.Run("start failure", func(t *testing.T) {
		runner := func(context.Context, ProcessCommand) (int, error) {
			return 0, &CLIError{Code: "process_start_failed", Message: "the external program is not installed"}
		}
		env := sshTestApp(t, sshTestKeyHandler(t, sshTestString("203.0.113.5"), nil, sshTestHostKey(15)), home, runner)
		if status := env.run(t, "ssh", sshTestSandboxID, "true"); status != 1 {
			t.Fatalf("exit status = %d, want 1", status)
		}
	})
}

// TestSSHCancellation maps a cancelled child to a silent 130.
func TestSSHCancellation(t *testing.T) {
	started := make(chan struct{})
	runner := func(ctx context.Context, _ ProcessCommand) (int, error) {
		close(started)
		<-ctx.Done()
		return -1, ctx.Err()
	}
	env := sshTestApp(t, sshTestKeyHandler(t, sshTestString("203.0.113.5"), nil, sshTestHostKey(16)), sshTestHome(t), runner)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan int, 1)
	finished := false
	defer func() {
		cancel()
		if !finished {
			select {
			case <-done:
			case <-time.After(5 * time.Second):
			}
		}
	}()
	go func() { done <- env.runContext(ctx, "ssh", sshTestSandboxID, "true") }()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("process did not start")
	}
	cancel()
	select {
	case status := <-done:
		finished = true
		if status != 130 {
			t.Fatalf("exit status = %d, want 130", status)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("command did not return after cancellation")
	}
	if env.stdout.Len() != 0 || env.stderr.Len() != 0 {
		t.Fatalf("stdout=%q stderr=%q, want silent cancellation", env.stdout.String(), env.stderr.String())
	}
}

// TestSSHConcurrentInvocationsEachKeepTheirPin proves two simultaneous commands
// use distinct known_hosts files and leave the user's known_hosts alone.
func TestSSHConcurrentInvocationsEachKeepTheirPin(t *testing.T) {
	home := sshTestHome(t)
	userKnownHosts := filepath.Join(home, ".ssh", "known_hosts")
	sentinel := "github.com ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIUserSentinel\n"
	if err := os.WriteFile(userKnownHosts, []byte(sentinel), 0o600); err != nil {
		t.Fatalf("write user known_hosts: %v", err)
	}
	hostKeys := map[string]string{
		sshTestSandboxID:  sshTestHostKey(17),
		sshTestSandboxID2: sshTestHostKey(18),
	}
	expectedPins := map[string]string{}
	for id, hostKey := range hostKeys {
		parsed, err := parseSSHHostKey(hostKey)
		if err != nil {
			t.Fatalf("parse host key: %v", err)
		}
		expectedPins[id] = parsed.knownHostsLine(id)
	}
	handler := func(w http.ResponseWriter, r *http.Request) {
		id := sshTestSandboxID
		if strings.Contains(r.URL.Path, sshTestSandboxID2) {
			id = sshTestSandboxID2
		}
		cliTestWriteJSON(t, w, http.StatusOK, sshTestResponse(sshTestString("203.0.113.5"), nil, hostKeys[id]))
	}
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseChildren := func() { releaseOnce.Do(func() { close(release) }) }
	ctx, cancel := context.WithCancel(t.Context())
	var children sync.WaitGroup
	defer func() {
		cancel()
		releaseChildren()
		joined := make(chan struct{})
		go func() { children.Wait(); close(joined) }()
		select {
		case <-joined:
		case <-time.After(5 * time.Second):
		}
	}()
	var reads sync.WaitGroup
	var mutex sync.Mutex
	pins := map[string]string{}
	var runnerErr error
	runner := func(_ context.Context, command ProcessCommand) (int, error) {
		path := ""
		for i := 0; i+1 < len(command.Args); i++ {
			if command.Args[i] == "-o" {
				if value, ok := strings.CutPrefix(command.Args[i+1], "UserKnownHostsFile="); ok {
					path = strings.Trim(value, `"`)
					break
				}
			}
		}
		content, err := os.ReadFile(path)
		mutex.Lock()
		if err != nil {
			runnerErr = err
		}
		pins[path] = string(content)
		mutex.Unlock()
		reads.Done()
		<-release
		return 0, nil
	}
	first := sshTestApp(t, handler, home, runner)
	second := sshTestApp(t, handler, home, runner)
	reads.Add(2)
	children.Add(2)
	statuses := make(chan int, 2)
	go func() { defer children.Done(); statuses <- first.runContext(ctx, "ssh", sshTestSandboxID, "true") }()
	go func() { defer children.Done(); statuses <- second.runContext(ctx, "ssh", sshTestSandboxID2, "true") }()
	allRead := make(chan struct{})
	go func() { reads.Wait(); close(allRead) }()
	select {
	case <-allRead:
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent children did not both read their pin")
	}
	releaseChildren()
	for range 2 {
		select {
		case status := <-statuses:
			if status != 0 {
				t.Fatalf("exit status = %d", status)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("concurrent invocation did not finish")
		}
	}
	mutex.Lock()
	failure := runnerErr
	mutex.Unlock()
	if failure != nil {
		t.Fatalf("read concurrent pin: %v", failure)
	}
	if len(pins) != 2 {
		t.Fatalf("pins = %d, want 2 distinct files", len(pins))
	}
	for path, content := range pins {
		switch {
		case content == expectedPins[sshTestSandboxID]:
		case content == expectedPins[sshTestSandboxID2]:
		default:
			t.Fatalf("pin %s = %q does not match its alias/host key", path, content)
		}
		if _, err := os.Lstat(path); !os.IsNotExist(err) {
			t.Fatalf("pin %s survived the command", path)
		}
	}
	if got := sshTestReadFile(t, userKnownHosts); got != sentinel {
		t.Fatalf("user known_hosts changed: %q", got)
	}
	if _, err := os.Lstat(filepath.Join(home, ".ssh", "bbraft_known_hosts")); !os.IsNotExist(err) {
		t.Fatal("a shared bbraft_known_hosts file was created")
	}
}
