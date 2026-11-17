package jailer

import (
	"bufio"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

// TestProveToolExecRefusesObservedMismatchThatExits: once the live exec
// proof OBSERVED a running executable that is not the staged inode, the
// attempt is refused even if the child then exits cleanly with the staged
// tool's comm: a later waitable exit never turns an observed contradiction
// into a fast-exit proof. A real /bin/sh child (comm "sh", the staged
// /bin/sh's comm; helper comm distinct) blocks on an inherited pipe until the
// live refusal witness; the test then lets it exit 0, proves the exit real
// and unreaped (pidfd readable, WNOWAIT CLD_EXITED 0) and only then releases
// the proof.
func TestProveToolExecRefusesObservedMismatchThatExits(t *testing.T) {
	_, m := jailerToolEnv(t, jailerModeExec)
	refused, resume := make(chan struct{}, 1), make(chan struct{})
	releaseProof := sync.OnceFunc(func() { close(resume) })
	m.hooks.toolLiveRefused, m.hooks.toolLiveResume = refused, resume

	stdinR, stdinW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	readyR, readyW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	pidfd := -1
	cmd := exec.Command("/bin/sh", "-c", "echo ready; read line; exit 0")
	cmd.Stdin, cmd.Stdout = stdinR, readyW
	cmd.SysProcAttr = &syscall.SysProcAttr{PidFD: &pidfd}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	stdinR.Close()
	readyW.Close()
	var handle *processHandle
	// Registered immediately: kill through the owned handle (or the
	// process before one exists), then the sole bounded Wait.
	t.Cleanup(func() {
		stdinW.Close()
		readyR.Close()
		if handle != nil {
			_ = handle.kill()
		} else {
			_ = cmd.Process.Kill()
		}
		done := make(chan error, 1)
		go func() { done <- cmd.Wait() }()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("the /bin/sh child was not reaped")
		}
		if handle != nil {
			_ = handle.close()
		}
	})
	if pidfd < 0 {
		t.Skip("clone3 returned no pidfd")
	}
	if handle, err = newProcessHandle(cmd.Process, pidfd, nil); err != nil {
		t.Fatal(err)
	}
	// Ready witness: the shell has exec'd and runs.
	if err := readyR.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if line, err := bufio.NewReader(readyR).ReadString('\n'); err != nil || line != "ready\n" {
		t.Fatalf("ready witness %q, %v", line, err)
	}
	identity, err := readProcIdentity(cmd.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	record := Record{SandboxID: ToolID(jailerAttempt()), PID: cmd.Process.Pid, StartTicks: identity.startTicks}
	type result struct {
		proof ToolProof
		err   error
	}
	out := make(chan result, 1)
	proofDone := make(chan struct{})
	go func() {
		defer close(proofDone)
		proof, err := m.proveToolExec(t.Context(), handle, inodeIdentity{Dev: 1, Ino: 1}, record, "jailer.test", "/bin/sh")
		out <- result{proof, err}
	}()
	// Runs before the child cleanup (LIFO): release and join the proof.
	t.Cleanup(func() {
		releaseProof()
		select {
		case <-proofDone:
		case <-time.After(10 * time.Second):
			t.Error("the proof goroutine did not finish")
		}
	})
	select {
	case <-refused:
	case r := <-out:
		t.Fatalf("the proof decided (%s, %v) before observing the live exe", r.proof, r.err)
	case <-time.After(10 * time.Second):
		t.Fatal("no live refusal witness")
	}
	// The observed contradiction stands; now the child exits cleanly.
	if _, err := stdinW.Write([]byte("\n")); err != nil {
		t.Fatal(err)
	}
	own, err := unix.PidfdOpen(cmd.Process.Pid, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(own)
	poll := []unix.PollFd{{Fd: int32(own), Events: unix.POLLIN}}
	if n, err := unix.Poll(poll, 10_000); err != nil || n != 1 {
		t.Fatalf("the child did not exit: %d, %v", n, err)
	}
	var info sigchldInfo
	if _, _, errno := unix.Syscall6(unix.SYS_WAITID, unix.P_PIDFD, uintptr(own), uintptr(unsafe.Pointer(&info)),
		unix.WEXITED|unix.WNOWAIT|unix.WNOHANG, 0, 0); errno != 0 || info.Code != cldExited || info.Status != 0 {
		t.Fatalf("waitid WNOWAIT = code %d status %d, %v; want a clean exit 0", info.Code, info.Status, errno)
	}
	releaseProof()
	var r result
	select {
	case r = <-out:
	case <-time.After(10 * time.Second):
		t.Fatal("the proof did not decide")
	}
	if r.err == nil || !strings.Contains(r.err.Error(), "not the staged") {
		t.Fatalf("proof = %s, %v: an observed live exe mismatch must refuse even after a clean exit", r.proof, r.err)
	}
}
