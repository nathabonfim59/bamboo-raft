//go:build linux

package cli

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"runtime"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// processWaitDelay bounds how long Wait keeps waiting after the context is
// done (or the child exited) for the child to exit and its I/O pipes to close.
// When it expires os/exec closes the pipes and kills the child.
const processWaitDelay = 2 * time.Second

// runProcess starts one child in its own process group, hands it the terminal
// when it owns an interactive one, and kills the whole group on cancellation.
func runProcess(ctx context.Context, command ProcessCommand) (int, error) {
	childCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	cmd := exec.CommandContext(childCtx, command.Path, command.Args...)
	cmd.Stdin = command.Stdin
	cmd.Stdout = command.Stdout
	cmd.Stderr = command.Stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		if err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
			return err
		}
		return nil
	}
	cmd.WaitDelay = processWaitDelay

	var foreground bool
	var plan sshForegroundPlan
	if command.Foreground {
		var err error
		plan, err = prepareSSHForeground(command, false, nil)
		if err != nil {
			return 0, err
		}
		foreground = true
		cmd.SysProcAttr.Foreground = true
		// Foreground takes a descriptor number in the parent; the child's stdin
		// is the same open terminal after fork.
		cmd.SysProcAttr.Ctty = int(plan.terminal.Fd())
	}

	if err := cmd.Start(); err != nil {
		// The child may have reached the foreground handoff before exec failed.
		if foreground {
			if restoreErr := tcSetForeground(plan.terminal, plan.originalPgrp); restoreErr != nil {
				return 0, restoreErr
			}
		}
		return 0, processStartFailure(ctx, err)
	}
	waitErr := cmd.Wait()
	if foreground {
		// Restoring runs from the background process group; SIGTTOU is blocked
		// locally so the shell's terminal state is recovered without touching
		// process-global signal dispositions.
		if err := tcSetForeground(plan.terminal, plan.originalPgrp); err != nil {
			return 0, err
		}
	}
	return processStatus(ctx, waitErr)
}

// tcSetForeground makes pgrp the terminal foreground process group. SIGTTOU
// is blocked on the calling OS thread only for the ioctl and the old mask is
// restored before the thread is unlocked.
func tcSetForeground(terminal *os.File, pgrp int) error {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	var set unix.Sigset_t
	bit := uint(unix.SIGTTOU) - 1
	set.Val[bit/64] |= 1 << (bit % 64)
	var previous unix.Sigset_t
	if err := unix.PthreadSigmask(unix.SIG_BLOCK, &set, &previous); err != nil {
		return &CLIError{Code: "terminal_restore_failed", Message: "block SIGTTOU failed"}
	}
	defer func() {
		_ = unix.PthreadSigmask(unix.SIG_SETMASK, &previous, nil)
	}()
	if err := unix.IoctlSetPointerInt(int(terminal.Fd()), unix.TIOCSPGRP, pgrp); err != nil {
		return &CLIError{Code: "terminal_restore_failed", Message: "restore the terminal foreground group failed"}
	}
	return nil
}
