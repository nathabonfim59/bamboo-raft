package cli

import (
	"os"
	"os/exec"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// sshRestoreHelperWait bounds how long the no-op restore helper may take. The
// helper is cleaned up by direct pid, never by process group.
const sshRestoreHelperWait = 2 * time.Second

// sshForegroundPlan is a prepared interactive-terminal handoff. The terminal
// is never modified until the child starts.
type sshForegroundPlan struct {
	terminal     *os.File
	originalPgrp int
	// helperPath is the pre-resolved no-op restore helper on platforms that
	// cannot restore the group from the parent process directly.
	helperPath string
}

// prepareSSHForeground reads the current terminal foreground group and, when
// the portable no-op helper is used, resolves it before any child starts. A
// missing helper therefore fails before the foreground group can change.
func prepareSSHForeground(command ProcessCommand, noopHelper bool, lookPath func(string) (string, error)) (sshForegroundPlan, error) {
	terminal, ok := command.Stdin.(*os.File)
	if !ok {
		return sshForegroundPlan{}, &CLIError{Code: "terminal_foreground_failed", Message: "stdin is not a terminal"}
	}
	originalPgrp, err := unix.IoctlGetInt(int(terminal.Fd()), unix.TIOCGPGRP)
	if err != nil || originalPgrp <= 0 {
		return sshForegroundPlan{}, &CLIError{Code: "terminal_foreground_failed", Message: "stdin is not a controlling terminal"}
	}
	plan := sshForegroundPlan{terminal: terminal, originalPgrp: originalPgrp}
	if noopHelper {
		plan.helperPath, err = sshRestorePreflight(lookPath)
		if err != nil {
			return sshForegroundPlan{}, err
		}
	}
	return plan, nil
}

// sshRestorePreflight resolves the no-op restore helper. lookPath is the test
// seam; nil uses exec.LookPath.
func sshRestorePreflight(lookPath func(string) (string, error)) (string, error) {
	if lookPath == nil {
		lookPath = exec.LookPath
	}
	path, err := lookPath("true")
	if err != nil || path == "" {
		return "", &CLIError{
			Code:    "terminal_foreground_failed",
			Message: "the terminal restore helper is not installed",
		}
	}
	return path, nil
}

// restoreForegroundNoop hands the terminal foreground group back to pgrp by
// starting a no-op child that joins pgrp and makes it foreground inside Go's
// fork path. Go masks all signals around TIOCSPGRP, so this needs no
// process-wide SIGTTOU change.
//
// The helper is in the original CLI process group. Its cleanup therefore kills
// only its own pid, never -pgrp, and it does not take the caller's context:
// restoring the terminal must work even after that context was cancelled.
func restoreForegroundNoop(helperPath string, terminal *os.File, pgrp int) error {
	return restoreForegroundNoopTimeout(helperPath, terminal, pgrp, sshRestoreHelperWait)
}

// restoreForegroundNoopTimeout is restoreForegroundNoop with a bounded wait.
func restoreForegroundNoopTimeout(helperPath string, terminal *os.File, pgrp int, timeout time.Duration) error {
	if pgrp <= 0 {
		return &CLIError{Code: "terminal_restore_failed", Message: "original terminal process group is invalid"}
	}
	if helperPath == "" {
		return &CLIError{Code: "terminal_restore_failed", Message: "terminal restore helper is not resolved"}
	}
	command := exec.Command(helperPath)
	// Nil streams become /dev/null. io.Discard would make os/exec own pipes and
	// Wait would then wait for every process that inherited them, defeating the
	// bounded cleanup.
	command.Stdin = nil
	command.Stdout = nil
	command.Stderr = nil
	command.SysProcAttr = &syscall.SysProcAttr{
		Setpgid:    true,
		Pgid:       pgrp,
		Foreground: true,
		Ctty:       int(terminal.Fd()),
	}
	if err := command.Start(); err != nil {
		return &CLIError{Code: "terminal_restore_failed", Message: "start the terminal restore helper failed"}
	}
	done := make(chan error, 1)
	go func() { done <- command.Wait() }()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case err := <-done:
		if err != nil {
			return &CLIError{Code: "terminal_restore_failed", Message: "the terminal restore helper failed"}
		}
		return nil
	case <-timer.C:
		// Direct pid only: the helper joined the original CLI group, so a group
		// kill here would kill the parent CLI too.
		if command.Process != nil {
			_ = command.Process.Kill()
		}
		<-done
		return &CLIError{Code: "terminal_restore_failed", Message: "the terminal restore helper did not exit"}
	}
}
