//go:build linux

package cli

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"testing"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
)

// TestRunSubprocessHelper is the child half of the subprocess tests. It runs a
// real `bbraft run` against the parent's fake API and exits with its status.
func TestRunSubprocessHelper(t *testing.T) {
	if os.Getenv("BBRAFT_RUN_HELPER_MODE") == "" {
		return
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	app := NewApp(Options{
		Stdin:        strings.NewReader(""),
		Stdout:       os.Stdout,
		Stderr:       os.Stderr,
		ConfigDir:    os.Getenv("BBRAFT_RUN_HELPER_CONFIG"),
		IsTTY:        func(io.Writer) bool { return false },
		PollInterval: time.Millisecond,
		WaitTimeout:  2 * time.Second,
	})
	app.SetArgs(func() []string {
		args := []string{"run", "--timeout", "30"}
		if os.Getenv("BBRAFT_RUN_HELPER_KEEP") == "1" {
			args = append(args, "--keep")
		}
		return append(args, "--", "true")
	}())
	os.Exit(app.Run(ctx))
}

// TestRunSubprocessSIGINTCleansUp proves a real process exits 130 after
// Ctrl-C and still deletes the sandbox through the confirmation header.
func TestRunSubprocessSIGINTCleansUp(t *testing.T) {
	fake := runTestNewAPI(t)
	started := make(chan struct{})
	fake.commandFunc = func(w http.ResponseWriter, r *http.Request, _ api.CommandRequest) bool {
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.WriteHeader(http.StatusOK)
		if _, err := io.WriteString(w, "{\"type\":\"started\"}\n"); err != nil {
			t.Errorf("write started frame: %v", err)
			return true
		}
		w.(http.Flusher).Flush()
		close(started)
		<-r.Context().Done()
		return true
	}
	server := httptest.NewServer(http.HandlerFunc(fake.serveHTTP))
	t.Cleanup(server.Close)

	cmd, waitForExit := runTestStartSubprocess(t, server.URL)
	waitRunTestSignal(t, started, "command")
	if err := cmd.Process.Signal(os.Interrupt); err != nil {
		t.Fatalf("signal child: %v", err)
	}
	if code := waitForExit(); code != 130 {
		t.Fatalf("child exit status = %d, want 130", code)
	}
	assertRunTestSubprocessCleanup(t, fake)
}

// TestRunSubprocessSIGINTWithKeepCleansUp proves Ctrl-C deletes a --keep
// sandbox rather than taking the keep path.
func TestRunSubprocessSIGINTWithKeepCleansUp(t *testing.T) {
	fake := runTestNewAPI(t)
	runTestSetRolling(t, fake)
	started := make(chan struct{})
	fake.commandFunc = func(w http.ResponseWriter, r *http.Request, _ api.CommandRequest) bool {
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.WriteHeader(http.StatusOK)
		if _, err := io.WriteString(w, "{\"type\":\"started\"}\n"); err != nil {
			t.Errorf("write started frame: %v", err)
			return true
		}
		w.(http.Flusher).Flush()
		close(started)
		<-r.Context().Done()
		return true
	}
	server := httptest.NewServer(http.HandlerFunc(fake.serveHTTP))
	t.Cleanup(server.Close)

	cmd, waitForExit := runTestStartSubprocess(t, server.URL, "BBRAFT_RUN_HELPER_KEEP=1")
	waitRunTestSignal(t, started, "command")
	if err := cmd.Process.Signal(os.Interrupt); err != nil {
		t.Fatalf("signal child: %v", err)
	}
	if code := waitForExit(); code != 130 {
		t.Fatalf("child exit status = %d, want 130", code)
	}
	assertRunTestSubprocessCleanup(t, fake)
	if fake.patchRequestCount() != 1 {
		t.Fatalf("PATCH requests = %d, want only the initial verification", fake.patchRequestCount())
	}
}

// TestRunSubprocessExitStatus proves the child's process status is the remote
// command's status and that cleanup runs first.
func TestRunSubprocessExitStatus(t *testing.T) {
	fake := runTestNewAPI(t)
	fake.commandFunc = func(w http.ResponseWriter, _ *http.Request, _ api.CommandRequest) bool {
		runTestWriteStream(t, w, runTestFrames("", "", new(7), false))
		return true
	}
	server := httptest.NewServer(http.HandlerFunc(fake.serveHTTP))
	t.Cleanup(server.Close)

	_, waitForExit := runTestStartSubprocess(t, server.URL)
	if code := waitForExit(); code != 7 {
		t.Fatalf("child exit status = %d, want 7", code)
	}
	assertRunTestSubprocessCleanup(t, fake)
}

// runTestStartSubprocess starts the helper and returns the command plus a
// function that waits for it and reports the exit status. Every fatal path is
// covered by a deferred kill-and-wait.
func runTestStartSubprocess(t *testing.T, serverURL string, extraEnv ...string) (*exec.Cmd, func() int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	t.Cleanup(cancel)
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestRunSubprocessHelper$")
	cmd.Env = append(os.Environ(),
		"BBRAFT_RUN_HELPER_MODE=signal",
		"BBRAFT_API_URL="+serverURL,
		"BBRAFT_API_KEY=test-key",
		"BBRAFT_RUN_HELPER_CONFIG="+t.TempDir(),
	)
	cmd.Env = append(cmd.Env, extraEnv...)
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	if err := cmd.Start(); err != nil {
		t.Fatalf("start helper: %v", err)
	}
	waited := false
	t.Cleanup(func() {
		if !waited {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	})
	waitForExit := func() int {
		waited = true
		err := cmd.Wait()
		if err == nil {
			return 0
		}
		if exit, ok := errors.AsType[*exec.ExitError](err); ok {
			return exit.ExitCode()
		}
		t.Fatalf("wait helper: %v", err)
		return -1
	}
	return cmd, waitForExit
}

// assertRunTestSubprocessCleanup proves the child deleted exactly the created
// sandbox with the pinned confirmation header.
func assertRunTestSubprocessCleanup(t *testing.T, fake *runTestAPI) {
	t.Helper()
	deletes := fake.deleteRequests()
	if len(deletes) != 1 || deletes[0] != cliTestSandboxID {
		t.Fatalf("child deletes = %v, want the created id", deletes)
	}
	confirms := fake.confirmations()
	if len(confirms) != 1 || confirms[0] != cliTestSandboxID {
		t.Fatalf("child confirmation headers = %v, want the created id", confirms)
	}
}
