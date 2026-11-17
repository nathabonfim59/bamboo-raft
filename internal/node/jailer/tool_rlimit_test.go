package jailer

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// A10 D1 VM finding 1: a tool helper's own resource limits must leave the Go
// helper able to allocate until its exec. The tool helper runs in Go with an
// address space (VmSize) already above a tool's memory bound, so an
// RLIMIT_AS at that bound turns any later heap growth into a fatal runtime
// out-of-memory before exec (exit 2, the helper's own comm).

// jailerRlimitProbeEnv selects jailerRlimitProbe in a re-executed test
// binary.
const jailerRlimitProbeEnv = "BBRAFT_JAILER_RLIMIT_PROBE"

// jailerRlimitProbe applies exactly the tool helper's limits, reports them,
// grows the Go heap the way helper work may, then execs /usr/bin/true.
// Exit codes: 0 from true; 7 RLIMIT_CORE not 0; 8 exec failed; 9 setup.
func jailerRlimitProbe() int {
	runtime.LockOSThread()
	if err := applyToolRlimits(helperConfig{Tool: true}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 9
	}
	var core, as unix.Rlimit
	if unix.Getrlimit(unix.RLIMIT_CORE, &core) != nil || unix.Getrlimit(unix.RLIMIT_AS, &as) != nil {
		return 9
	}
	fmt.Printf("core=%d/%d as=%d/%d\n", core.Cur, core.Max, as.Cur, as.Max)
	if core.Cur != 0 || core.Max != 0 {
		return 7
	}
	var keep [][]byte
	for range 64 {
		keep = append(keep, make([]byte, 1<<20))
	}
	runtime.KeepAlive(keep)
	err := unix.Exec("/usr/bin/true", []string{"true"}, nil)
	fmt.Fprintln(os.Stderr, "exec:", err)
	return 8
}

// TestToolHelperRlimitsKeepGoRuntimeAlive runs the tool helper's limits in a
// real subprocess: RLIMIT_CORE is 0, RLIMIT_AS is left as inherited, and
// the helper can still allocate and exec.
func TestToolHelperRlimitsKeepGoRuntimeAlive(t *testing.T) {
	if _, err := os.Stat("/usr/bin/true"); err != nil {
		t.Skipf("no /usr/bin/true: %v", err)
	}
	var parentAS unix.Rlimit
	if err := unix.Getrlimit(unix.RLIMIT_AS, &parentAS); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	cmd := exec.Command(os.Args[0])
	cmd.Env = []string{jailerRlimitProbeEnv + "=1"}
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	var err error
	select {
	case err = <-done:
	case <-time.After(30 * time.Second):
		_ = cmd.Process.Kill()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("the killed probe was never reaped")
		}
		t.Fatal("the rlimit probe did not finish")
	}
	tail := stderr.String()
	if len(tail) > 512 {
		tail = tail[:512]
	}
	if exitErr, ok := errors.AsType[*exec.ExitError](err); ok {
		t.Fatalf("helper under the tool limits exited %d before exec (stdout %q, stderr %q)", exitErr.ExitCode(), stdout.String(), tail)
	} else if err != nil {
		t.Fatal(err)
	}
	want := fmt.Sprintf("core=0/0 as=%d/%d", parentAS.Cur, parentAS.Max)
	if got := strings.TrimSpace(stdout.String()); got != want {
		t.Fatalf("limits %q, want %q (RLIMIT_AS unchanged from the parent)", got, want)
	}
}
