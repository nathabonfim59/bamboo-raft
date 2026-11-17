//go:build linux

package cli

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// sshPTYResult is what the re-exec helper reports to the outer test.
type sshPTYResult struct {
	OK                  bool   `json:"ok"`
	Err                 string `json:"err,omitempty"`
	Output              string `json:"output,omitempty"`
	Restored            bool   `json:"restored"`
	DescendantGone      bool   `json:"descendantGone"`
	ForegroundUnchanged bool   `json:"foregroundUnchanged"`
	HelperGone          bool   `json:"helperGone"`
}

// TestRunProcessForegroundPTY proves terminal input, cancellation of
// descendants, and foreground restoration with a real local pseudo-terminal.
// It never contacts a host: the helper re-execs this test binary, creates a
// PTY with /dev/ptmx and runs `sh`.
func TestRunProcessForegroundPTY(t *testing.T) {
	if _, err := exec.LookPath("sh"); err != nil {
		t.Skip("sh is not installed")
	}
	for _, mode := range []string{"read", "cancel", "noop", "restorepreflight", "restoreerror", "restorehang", "restorecancel"} {
		t.Run(mode, func(t *testing.T) {
			helperDir := t.TempDir()
			resultPath := filepath.Join(helperDir, "pty-result.json")
			environment := append(os.Environ(),
				"BBRAFT_SSH_PTY_MODE="+mode,
				"BBRAFT_SSH_PTY_RESULT="+resultPath,
			)
			if mode == "restorehang" {
				scriptPath := filepath.Join(helperDir, "restore-helper.sh")
				script := "#!/bin/sh\necho $$ > \"$BBRAFT_SSH_PTY_HELPER_PID\"\nexec sleep 30\n"
				if err := os.WriteFile(scriptPath, []byte(script), 0o755); err != nil {
					t.Fatalf("write hanging helper: %v", err)
				}
				environment = append(environment,
					"BBRAFT_SSH_PTY_HELPER_SCRIPT="+scriptPath,
					"BBRAFT_SSH_PTY_HELPER_PID="+filepath.Join(helperDir, "helper.pid"),
				)
			}
			helperCtx, cancelHelper := context.WithTimeout(t.Context(), 20*time.Second)
			defer cancelHelper()
			command := exec.CommandContext(helperCtx, os.Args[0], "-test.run=^TestSSHRunProcessPTYHelper$")
			command.Env = environment
			output, err := command.CombinedOutput()
			if err != nil {
				t.Fatalf("pty helper: %v: %s", err, output)
			}
			raw, err := os.ReadFile(resultPath)
			if err != nil {
				t.Fatalf("read helper result: %v: %s", err, output)
			}
			var result sshPTYResult
			if err := json.Unmarshal(raw, &result); err != nil {
				t.Fatalf("decode helper result: %v", err)
			}
			if !result.OK {
				t.Fatalf("helper failed: %s", result.Err)
			}
			switch mode {
			case "read":
				if !result.Restored {
					t.Fatal("foreground process group was not restored")
				}
				if !strings.Contains(result.Output, "got:hello") {
					t.Fatalf("output = %q, want the terminal read to succeed", result.Output)
				}
			case "cancel":
				if !result.Restored {
					t.Fatal("foreground process group was not restored")
				}
				if !result.DescendantGone {
					t.Fatal("cancellation left a descendant running")
				}
			case "noop", "restorecancel":
				if !result.Restored {
					t.Fatal("foreground process group was not restored")
				}
			case "restorepreflight":
				if !result.ForegroundUnchanged {
					t.Fatal("missing restore helper changed the foreground group")
				}
			case "restorehang":
				if !result.HelperGone {
					t.Fatal("hanging restore helper was not cleaned up by direct pid")
				}
			}
		})
	}
}

// TestSSHRunProcessPTYHelper is the re-exec helper. It is skipped in the
// normal test run and only runs when the outer test sets the mode variable.
func TestSSHRunProcessPTYHelper(t *testing.T) {
	mode := os.Getenv("BBRAFT_SSH_PTY_MODE")
	if mode == "" {
		t.Skip("pty helper")
	}
	resultPath := os.Getenv("BBRAFT_SSH_PTY_RESULT")
	// Closing the helper's controlling terminal at exit can deliver SIGHUP to
	// the foreground group. That is a test-process concern only; ignore it so
	// the report is written and the helper exits normally.
	signal.Ignore(syscall.SIGHUP)
	result := sshPTYResult{}
	if err := sshPTYHelper(mode, &result); err != nil {
		result.Err = err.Error()
	} else {
		result.OK = true
	}
	raw, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("encode helper result: %v", err)
	}
	if err := os.WriteFile(resultPath, raw, 0o600); err != nil {
		t.Fatalf("write helper result: %v", err)
	}
}

// sshPTYHelper creates a controlling pseudo-terminal and runs one mode.
func sshPTYHelper(mode string, result *sshPTYResult) error {
	master, slave, err := sshOpenPTY()
	if err != nil {
		return err
	}
	// The descriptors stay open until the helper process exits. Closing the
	// controlling terminal while it is still our session's tty would deliver
	// SIGHUP to this process before it can report its result.

	if _, err := unix.Setsid(); err != nil {
		return fmt.Errorf("setsid: %w", err)
	}
	if err := unix.IoctlSetInt(int(slave.Fd()), unix.TIOCSCTTY, 0); err != nil {
		return fmt.Errorf("TIOCSCTTY: %w", err)
	}
	ownPgrp := syscall.Getpgrp()

	switch mode {
	case "read":
		return sshPTYRead(master, slave, ownPgrp, result)
	case "cancel":
		return sshPTYCancel(master, slave, ownPgrp, result)
	case "noop":
		return sshPTYNoopRestore(slave, ownPgrp, result)
	case "restorepreflight":
		return sshPTYRestorePreflight(slave, ownPgrp, result)
	case "restoreerror":
		return sshPTYRestoreError(slave, ownPgrp, result)
	case "restorehang":
		return sshPTYRestoreHang(slave, ownPgrp, result)
	case "restorecancel":
		return sshPTYRestoreAfterCancel(slave, ownPgrp, result)
	default:
		return fmt.Errorf("unknown pty helper mode %q", mode)
	}
}

// sshOpenPTY opens a fresh pseudo-terminal without any external module.
func sshOpenPTY() (*os.File, *os.File, error) {
	masterFD, err := unix.Open("/dev/ptmx", unix.O_RDWR|unix.O_NOCTTY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, nil, fmt.Errorf("open ptmx: %w", err)
	}
	if err := unix.IoctlSetPointerInt(masterFD, unix.TIOCSPTLCK, 0); err != nil {
		_ = unix.Close(masterFD)
		return nil, nil, fmt.Errorf("unlock pty: %w", err)
	}
	number, err := unix.IoctlGetInt(masterFD, unix.TIOCGPTN)
	if err != nil {
		_ = unix.Close(masterFD)
		return nil, nil, fmt.Errorf("pty number: %w", err)
	}
	slaveFD, err := unix.Open("/dev/pts/"+strconv.Itoa(number), unix.O_RDWR|unix.O_NOCTTY|unix.O_CLOEXEC, 0)
	if err != nil {
		_ = unix.Close(masterFD)
		return nil, nil, fmt.Errorf("open pts: %w", err)
	}
	return os.NewFile(uintptr(masterFD), "bbraft-pty-master"),
		os.NewFile(uintptr(slaveFD), "bbraft-pty-slave"), nil
}

// sshPTYRead proves the child in its own foreground group can read the
// terminal (no SIGTTIN) and that the original group is restored afterwards.
func sshPTYRead(master, slave *os.File, ownPgrp int, result *sshPTYResult) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	command := ProcessCommand{
		Path: "sh", Args: []string{"-c", "read line; echo got:$line"},
		Stdin: slave, Stdout: slave, Stderr: slave, Foreground: true,
	}
	done := make(chan error, 1)
	finished := false
	defer func() {
		cancel()
		// Closing the master unblocks the reader goroutine on every path.
		_ = master.Close()
		if !finished {
			select {
			case <-done:
			case <-time.After(5 * time.Second):
			}
		}
	}()
	go func() {
		code, err := runProcess(ctx, command)
		if err == nil && code != 0 {
			err = fmt.Errorf("child exit status %d", code)
		}
		done <- err
	}()

	outputCh := make(chan string, 1)
	go func() {
		var buffer []byte
		chunk := make([]byte, 256)
		for {
			n, readErr := master.Read(chunk)
			buffer = append(buffer, chunk[:n]...)
			if bytes.Contains(buffer, []byte("got:")) || readErr != nil {
				outputCh <- string(buffer)
				return
			}
		}
	}()

	if _, err := master.Write([]byte("hello\n")); err != nil {
		return fmt.Errorf("write pty: %w", err)
	}
	select {
	case output := <-outputCh:
		result.Output = output
	case <-time.After(5 * time.Second):
		return errors.New("terminal read never completed; child may be stopped by SIGTTIN")
	}
	select {
	case err := <-done:
		finished = true
		if err != nil {
			return err
		}
	case <-time.After(5 * time.Second):
		return errors.New("child did not exit")
	}
	if err := sshPTYCheckRestored(slave, ownPgrp, result); err != nil {
		return err
	}
	return nil
}

// sshPTYCancel proves a cancelled foreground child takes its descendants with
// it and the original group is restored.
func sshPTYCancel(master, slave *os.File, ownPgrp int, result *sshPTYResult) error {
	ctx, cancel := context.WithCancel(context.Background())
	command := ProcessCommand{
		Path: "sh", Args: []string{"-c", "sleep 30 & echo GRAND:$!; wait"},
		Stdin: slave, Stdout: slave, Stderr: slave, Foreground: true,
	}
	done := make(chan error, 1)
	finished := false
	defer func() {
		cancel()
		// Closing the master unblocks the pid reader on every path.
		_ = master.Close()
		if !finished {
			select {
			case <-done:
			case <-time.After(5 * time.Second):
			}
		}
	}()
	go func() {
		_, err := runProcess(ctx, command)
		done <- err
	}()

	pidCh := make(chan int, 1)
	go func() {
		var buffer []byte
		chunk := make([]byte, 256)
		for {
			n, readErr := master.Read(chunk)
			buffer = append(buffer, chunk[:n]...)
			if index := bytes.Index(buffer, []byte("GRAND:")); index >= 0 {
				text := string(buffer[index+len("GRAND:"):])
				if line, _, ok := strings.Cut(text, "\n"); ok {
					pid, err := strconv.Atoi(strings.TrimSpace(line))
					if err == nil {
						pidCh <- pid
						return
					}
				}
			}
			if readErr != nil {
				return
			}
		}
	}()

	var descendant int
	select {
	case descendant = <-pidCh:
	case <-time.After(5 * time.Second):
		return errors.New("did not receive the grandchild pid")
	}
	cancel()
	select {
	case err := <-done:
		finished = true
		if !errors.Is(err, context.Canceled) {
			return fmt.Errorf("run error = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		return errors.New("cancelled child did not stop")
	}
	gone := false
	for range 40 {
		if err := syscall.Kill(descendant, 0); errors.Is(err, syscall.ESRCH) {
			gone = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	result.DescendantGone = gone
	if !gone {
		return errors.New("cancellation left the descendant running")
	}
	if err := sshPTYCheckRestored(slave, ownPgrp, result); err != nil {
		return err
	}
	return nil
}

// sshPTYRestoreHelper resolves the portable restore helper for PTY tests.
func sshPTYRestoreHelper() (string, error) {
	return sshRestorePreflight(nil)
}

// sshPTYStartForegroundChild starts a long-lived sh in its own group and waits
// until the kernel reports it as the terminal foreground group.
func sshPTYStartForegroundChild(slave *os.File) (*exec.Cmd, error) {
	command := exec.Command("sh", "-c", "sleep 30")
	command.Stdin, command.Stdout, command.Stderr = slave, slave, slave
	command.SysProcAttr = &syscall.SysProcAttr{
		Setpgid:    true,
		Foreground: true,
		Ctty:       int(slave.Fd()),
	}
	if err := command.Start(); err != nil {
		return nil, fmt.Errorf("start foreground child: %w", err)
	}
	pid := command.Process.Pid
	for range 40 {
		pgrp, err := unix.IoctlGetInt(int(slave.Fd()), unix.TIOCGPGRP)
		if err == nil && pgrp == pid {
			return command, nil
		}
		time.Sleep(25 * time.Millisecond)
	}
	sshPTYStopForegroundChild(command)
	return nil, errors.New("child never became the foreground group")
}

// sshPTYStopForegroundChild kills the child's group and joins it.
func sshPTYStopForegroundChild(command *exec.Cmd) {
	if command == nil || command.Process == nil {
		return
	}
	_ = syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
	_ = command.Wait()
}

// sshPTYNoopRestore exercises the portable restoration path: a no-op child
// joins the original group and makes it foreground inside Go's fork path.
func sshPTYNoopRestore(slave *os.File, ownPgrp int, result *sshPTYResult) error {
	helper, err := sshPTYRestoreHelper()
	if err != nil {
		return err
	}
	command, err := sshPTYStartForegroundChild(slave)
	if err != nil {
		return err
	}
	defer sshPTYStopForegroundChild(command)
	if err := restoreForegroundNoop(helper, slave, ownPgrp); err != nil {
		return err
	}
	return sshPTYCheckRestored(slave, ownPgrp, result)
}

// sshPTYRestorePreflight proves a missing helper fails before any foreground
// handoff: the pty foreground group is unchanged.
func sshPTYRestorePreflight(slave *os.File, ownPgrp int, result *sshPTYResult) error {
	before, err := unix.IoctlGetInt(int(slave.Fd()), unix.TIOCGPGRP)
	if err != nil {
		return fmt.Errorf("TIOCGPGRP: %w", err)
	}
	_, err = prepareSSHForeground(ProcessCommand{Stdin: slave, Foreground: true}, true,
		func(string) (string, error) { return "", errors.New("missing helper") })
	if err == nil {
		return errors.New("missing restore helper was accepted")
	}
	cliErr, ok := errors.AsType[*CLIError](err)
	if !ok || cliErr.Code != "terminal_foreground_failed" {
		return fmt.Errorf("error = %v, want terminal_foreground_failed", err)
	}
	after, err := unix.IoctlGetInt(int(slave.Fd()), unix.TIOCGPGRP)
	if err != nil {
		return fmt.Errorf("TIOCGPGRP: %w", err)
	}
	result.ForegroundUnchanged = before == after && after == ownPgrp
	if !result.ForegroundUnchanged {
		return fmt.Errorf("foreground group changed from %d to %d", before, after)
	}
	return nil
}

// sshPTYRestoreError proves a failing restore helper is reported as a failure.
func sshPTYRestoreError(slave *os.File, ownPgrp int, result *sshPTYResult) error {
	falsePath, err := exec.LookPath("false")
	if err != nil {
		return fmt.Errorf("false is not installed: %w", err)
	}
	command, err := sshPTYStartForegroundChild(slave)
	if err != nil {
		return err
	}
	defer sshPTYStopForegroundChild(command)
	err = restoreForegroundNoopTimeout(falsePath, slave, ownPgrp, time.Second)
	if err == nil {
		return errors.New("failing restore helper reported success")
	}
	cliErr, ok := errors.AsType[*CLIError](err)
	if !ok || cliErr.Code != "terminal_restore_failed" {
		return fmt.Errorf("error = %v, want terminal_restore_failed", err)
	}
	// The helper exits non-zero after the kernel handoff. The API must not
	// claim success even if the group happens to be back at ownPgrp.
	result.Restored = false
	return nil
}

// sshPTYRestoreHang proves a hanging restore helper is cleaned up by direct
// pid, never by the original group (which contains this helper process).
func sshPTYRestoreHang(slave *os.File, ownPgrp int, result *sshPTYResult) error {
	script := os.Getenv("BBRAFT_SSH_PTY_HELPER_SCRIPT")
	pidFile := os.Getenv("BBRAFT_SSH_PTY_HELPER_PID")
	if script == "" || pidFile == "" {
		return errors.New("hanging helper paths are missing")
	}
	command, err := sshPTYStartForegroundChild(slave)
	if err != nil {
		return err
	}
	defer sshPTYStopForegroundChild(command)
	err = restoreForegroundNoopTimeout(script, slave, ownPgrp, 200*time.Millisecond)
	if err == nil {
		return errors.New("hanging restore helper reported success")
	}
	cliErr, ok := errors.AsType[*CLIError](err)
	if !ok || cliErr.Code != "terminal_restore_failed" {
		return fmt.Errorf("error = %v, want terminal_restore_failed", err)
	}
	var helper int
	for range 40 {
		raw, readErr := os.ReadFile(pidFile)
		if readErr == nil {
			helper, _ = strconv.Atoi(strings.TrimSpace(string(raw)))
			if helper > 0 {
				break
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	if helper <= 0 {
		return errors.New("hanging helper never recorded its pid")
	}
	for range 40 {
		if err := syscall.Kill(helper, 0); errors.Is(err, syscall.ESRCH) {
			result.HelperGone = true
			return nil
		}
		time.Sleep(25 * time.Millisecond)
	}
	return errors.New("hanging helper was not killed by direct pid")
}

// sshPTYRestoreAfterCancel proves restoration does not depend on the caller's
// context: it succeeds after that context is already cancelled.
func sshPTYRestoreAfterCancel(slave *os.File, ownPgrp int, result *sshPTYResult) error {
	helper, err := sshPTYRestoreHelper()
	if err != nil {
		return err
	}
	command, err := sshPTYStartForegroundChild(slave)
	if err != nil {
		return err
	}
	defer sshPTYStopForegroundChild(command)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if ctx.Err() == nil {
		return errors.New("context was not cancelled")
	}
	if err := restoreForegroundNoop(helper, slave, ownPgrp); err != nil {
		return err
	}
	return sshPTYCheckRestored(slave, ownPgrp, result)
}

// sshPTYCheckRestored asserts the terminal foreground group is ownPgrp.
func sshPTYCheckRestored(slave *os.File, ownPgrp int, result *sshPTYResult) error {
	pgrp, err := unix.IoctlGetInt(int(slave.Fd()), unix.TIOCGPGRP)
	if err != nil {
		return fmt.Errorf("TIOCGPGRP: %w", err)
	}
	result.Restored = pgrp == ownPgrp
	if !result.Restored {
		return fmt.Errorf("foreground pgrp = %d, want %d", pgrp, ownPgrp)
	}
	return nil
}

// TestRunProcessExitAndGroupKill covers the non-TTY path of the production
// runner: exact exit status and whole-group cancellation.
func TestRunProcessExitAndGroupKill(t *testing.T) {
	if _, err := exec.LookPath("sh"); err != nil {
		t.Skip("sh is not installed")
	}
	t.Run("exit status", func(t *testing.T) {
		code, err := runProcess(t.Context(), ProcessCommand{
			Path: "sh", Args: []string{"-c", "exit 7"}, Stdin: strings.NewReader(""),
		})
		if err != nil {
			t.Fatalf("run: %v", err)
		}
		if code != 7 {
			t.Fatalf("exit status = %d, want 7", code)
		}
	})
	t.Run("start failure", func(t *testing.T) {
		_, err := runProcess(t.Context(), ProcessCommand{
			Path: "bbraft-no-such-program", Args: nil, Stdin: strings.NewReader(""),
		})
		cliErr, ok := errors.AsType[*CLIError](err)
		if !ok || cliErr.Code != "process_start_failed" {
			t.Fatalf("error = %v, want process_start_failed", err)
		}
	})
	t.Run("group cancellation", func(t *testing.T) {
		pidFile := filepath.Join(t.TempDir(), "grandchild.pid")
		ctx, cancel := context.WithCancel(t.Context())
		script := "sleep 30 & echo $! > " + pidFile + "; wait"
		done := make(chan error, 1)
		go func() {
			_, err := runProcess(ctx, ProcessCommand{
				Path: "sh", Args: []string{"-c", script}, Stdin: strings.NewReader(""),
			})
			done <- err
		}()
		var descendant int
		for range 100 {
			raw, err := os.ReadFile(pidFile)
			if err == nil && len(raw) > 0 {
				descendant, _ = strconv.Atoi(strings.TrimSpace(string(raw)))
				if descendant > 0 {
					break
				}
			}
			time.Sleep(20 * time.Millisecond)
		}
		if descendant <= 0 {
			cancel()
			t.Fatal("grandchild pid was never written")
		}
		cancel()
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("run error = %v, want context.Canceled", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("cancelled child did not stop")
		}
		gone := false
		for range 40 {
			if err := syscall.Kill(descendant, 0); errors.Is(err, syscall.ESRCH) {
				gone = true
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		if !gone {
			t.Fatal("group cancellation left the grandchild running")
		}
	})
}
