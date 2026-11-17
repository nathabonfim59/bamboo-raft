package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"

	"github.com/nathabonfim59/bamboo-raft/internal/ids"
)

// newSSHCommand builds `bbraft ssh [-t] ID [command...]`.
func newSSHCommand(a *App) *cobra.Command {
	var tty bool
	cmd := &cobra.Command{
		Use:   "ssh [-t] ID [command...]",
		Short: "Open an SSH session in a sandbox, or run one command",
		Long: "Open an SSH session in a sandbox, or run one command.\n\n" +
			"The CLI manages ~/.ssh/bbraft_ed25519, installs its public key through\n" +
			"the sshKey API, pins the returned host key for this invocation and runs\n" +
			"the local OpenSSH client without a shell.\n\n" +
			"One command argument is a remote shell command line and is sent\n" +
			"unchanged. Several arguments are argv: each is POSIX-shell quoted and\n" +
			"joined into one remote command string. The remote shell interprets that\n" +
			"string. Put -- before a command that starts with -.",
		Args: cobra.MinimumNArgs(1),
		RunE: a.runE(func(ctx context.Context, args []string) error {
			return a.runSSH(ctx, args[0], args[1:], tty)
		}),
	}
	cmd.Flags().BoolVarP(&tty, "tty", "t", false, "allocate a terminal for the remote command")
	return cmd
}

// runSSH installs the managed key, pins the host key and runs ssh.
func (a *App) runSSH(ctx context.Context, sandboxID string, commandArgs []string, tty bool) error {
	if err := ctx.Err(); err != nil {
		return &ExitError{Code: 130}
	}
	if !ids.ValidSandbox(sandboxID) {
		return &UsageError{Err: fmt.Errorf("ssh: invalid sandbox id %q", sandboxID)}
	}
	if a.out.ExplicitJSON {
		return &UsageError{Err: errors.New("ssh: --json is not supported; output is the remote program's")}
	}
	remoteCommand, err := sshCommandLine(commandArgs)
	if err != nil {
		return &UsageError{Err: err}
	}
	access, cleanup, err := a.prepareSSHAccess(ctx, sandboxID)
	if err != nil {
		return sshContextExit(ctx, err)
	}
	defer cleanup()
	// A cancellation that arrived during setup must not spawn a child, even
	// when the API already answered.
	if err := ctx.Err(); err != nil {
		return &ExitError{Code: 130}
	}
	command := sshInvocation(access, tty || remoteCommand == "", remoteCommand,
		a.opts.Stdin, a.opts.Stdout, a.opts.Stderr)
	return sshContextExit(ctx, a.runSSHProcess(ctx, command))
}

// sshCommandLine builds the single remote command string. OpenSSH joins its
// remote argv with spaces, so the CLI quotes multiple words itself and always
// passes at most one command argument.
func sshCommandLine(args []string) (string, error) {
	if len(args) == 0 {
		return "", nil
	}
	for _, arg := range args {
		if strings.ContainsRune(arg, '\x00') {
			return "", errors.New("ssh: command argument contains a NUL byte")
		}
	}
	if len(args) == 1 {
		if strings.TrimSpace(args[0]) == "" {
			return "", errors.New("ssh: command is empty")
		}
		return args[0], nil
	}
	return execCommandLine(args), nil
}

// prepareSSHAccess resolves the managed key, installs its public half and
// writes the per-invocation known_hosts pin. cleanup must run after the child
// has joined.
func (a *App) prepareSSHAccess(ctx context.Context, sandboxID string) (sshAccess, func(), error) {
	noCleanup := func() {}
	if err := ctx.Err(); err != nil {
		return sshAccess{}, noCleanup, err
	}
	client, err := a.apiClient(ctx)
	if err != nil {
		return sshAccess{}, noCleanup, err
	}
	dir, err := a.sshKeyDir()
	if err != nil {
		return sshAccess{}, noCleanup, err
	}
	if err := ctx.Err(); err != nil {
		return sshAccess{}, noCleanup, err
	}
	material, err := ensureSSHKey(dir)
	if err != nil {
		return sshAccess{}, noCleanup, err
	}
	if err := ctx.Err(); err != nil {
		return sshAccess{}, noCleanup, err
	}
	target, err := installSSHKey(ctx, client, sandboxID, material.PublicKey)
	if err != nil {
		return sshAccess{}, noCleanup, err
	}
	if err := ctx.Err(); err != nil {
		return sshAccess{}, noCleanup, err
	}
	knownHostsPath, cleanup, err := writeTempKnownHosts(target.HostKey, sandboxID)
	if err != nil {
		return sshAccess{}, noCleanup, err
	}
	return sshAccess{
		sandboxID:      sandboxID,
		target:         target,
		privateKeyPath: material.PrivatePath,
		knownHostsPath: knownHostsPath,
	}, cleanup, nil
}

// sshKeyDir returns $HOME/.ssh. HOME is read through the injected environment
// so tests never touch the real user directory.
func (a *App) sshKeyDir() (string, error) {
	home := a.opts.Getenv("HOME")
	if home == "" {
		resolved, err := os.UserHomeDir()
		if err != nil {
			return "", sshKeyError("cannot find the home directory")
		}
		home = resolved
	}
	if !filepath.IsAbs(home) {
		return "", sshKeyError("the home directory is not absolute")
	}
	return filepath.Join(home, ".ssh"), nil
}

// runSSHProcess runs the local client and maps its status. A cancelled command
// exits 130 silently; a child's own exit code passes through exactly.
func (a *App) runSSHProcess(ctx context.Context, command ProcessCommand) error {
	if ctx.Err() != nil {
		return &ExitError{Code: 130}
	}
	code, err := a.opts.RunProcess(ctx, command)
	if err != nil {
		if ctx.Err() != nil || errors.Is(err, context.Canceled) {
			return &ExitError{Code: 130}
		}
		return err
	}
	if ctx.Err() != nil {
		return &ExitError{Code: 130}
	}
	if code < 0 {
		code = 1
	}
	if code != 0 {
		return &ExitError{Code: code}
	}
	return nil
}

// sshContextExit turns a cancellation anywhere in the setup or child phase into
// the conventional 130 status.
func sshContextExit(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if ctx.Err() != nil || errors.Is(err, context.Canceled) {
		return &ExitError{Code: 130}
	}
	return err
}
