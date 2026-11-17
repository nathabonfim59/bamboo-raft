//go:build !linux

package cli

import (
	"context"
	"errors"
	"os/exec"
	"syscall"
	"time"
)

// processWaitDelay bounds how long Wait keeps waiting after the context is
// done (or the child exited) for the child to exit and its I/O pipes to close.
// When it expires os/exec closes the pipes and kills the child.
const processWaitDelay = 2 * time.Second

// runProcess starts one child in its own process group, hands it the terminal
// when it owns an interactive one, and kills the whole group on cancellation.
// The restore helper is preflighted before Start, so a missing helper fails
// before the foreground group can change.
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
		plan, err = prepareSSHForeground(command, true, nil)
		if err != nil {
			return 0, err
		}
		foreground = true
		cmd.SysProcAttr.Foreground = true
		cmd.SysProcAttr.Ctty = int(plan.terminal.Fd())
	}

	if err := cmd.Start(); err != nil {
		if foreground {
			if restoreErr := restoreForegroundNoop(plan.helperPath, plan.terminal, plan.originalPgrp); restoreErr != nil {
				return 0, restoreErr
			}
		}
		return 0, processStartFailure(ctx, err)
	}
	waitErr := cmd.Wait()
	if foreground {
		if err := restoreForegroundNoop(plan.helperPath, plan.terminal, plan.originalPgrp); err != nil {
			return 0, err
		}
	}
	return processStatus(ctx, waitErr)
}
