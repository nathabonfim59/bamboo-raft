package node

import (
	"errors"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/node/jailer"
)

// TestMain dispatches the jailer's re-exec helpers FIRST (a selected helper
// never returns: it execs or exits jailer.HelperExitCode), then the
// re-executed argv helper, before the testing package parses flags, so the
// helper's /proc cmdline is exactly the argv its parent exec'd (a
// production QEMU argv included).
func TestMain(m *testing.M) {
	jailer.RunHelperIfSelected()
	if os.Getenv(jailArgvHelperEnv) == "1" {
		maintQMPHelper()
		jailArgvHelper()
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// TestTestMainDispatchesJailerHelperFirst: a re-exec selecting the jailer
// helper with the argv-helper environment also set reaches the real helper,
// whose handled pre-exec return is the reserved exit (an argv helper or a
// TestMain without the dispatch exits 0). The empty-environment dispatch is
// exercised by the root gate: without the dispatch it would run this whole
// suite recursively, so it is not a unit case.
func TestTestMainDispatchesJailerHelperFirst(t *testing.T) {
	cmd := exec.Command(os.Args[0], jailer.HelperArg, "x", "y", "z")
	cmd.Env = []string{jailArgvHelperEnv + "=1"}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	var err error
	select {
	case err = <-done:
	case <-time.After(10 * time.Second):
		_ = cmd.Process.Kill()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("the killed helper was never reaped")
		}
		t.Fatal("the re-executed helper did not exit")
	}
	exitErr, ok := errors.AsType[*exec.ExitError](err)
	if !ok || exitErr.ExitCode() != jailer.HelperExitCode {
		t.Fatalf("re-exec with the helper selector = %v, want exit %d", err, jailer.HelperExitCode)
	}
}
