package cli

import (
	"context"
	"errors"

	"github.com/spf13/cobra"
)

// newSCPCommand builds `bbraft scp [-r] SRC... DST`.
func newSCPCommand(a *App) *cobra.Command {
	var recursive bool
	cmd := &cobra.Command{
		Use:   "scp [-r] SRC... DST",
		Short: "Copy files to or from a sandbox",
		Long: "Copy files to or from a sandbox with the local OpenSSH scp client.\n\n" +
			"One operand must be bx_ID:/path. The CLI manages ~/.ssh/bbraft_ed25519,\n" +
			"installs its public key through the sshKey API and pins the returned\n" +
			"host key for this invocation. Add -r for a directory.",
		Args: cobra.MinimumNArgs(2),
		RunE: a.runE(func(ctx context.Context, args []string) error {
			return a.runSCP(ctx, args, recursive)
		}),
	}
	cmd.Flags().BoolVarP(&recursive, "recursive", "r", false, "copy directories recursively")
	return cmd
}

// runSCP installs the managed key for the remote side, pins the host key and
// runs scp with its destination last.
func (a *App) runSCP(ctx context.Context, args []string, recursive bool) error {
	if err := ctx.Err(); err != nil {
		return &ExitError{Code: 130}
	}
	if a.out.ExplicitJSON {
		return &UsageError{Err: errors.New("scp: --json is not supported; output belongs to scp")}
	}
	plan, err := parseSCPPlan(args, recursive)
	if err != nil {
		return err
	}
	access, cleanup, err := a.prepareSSHAccess(ctx, plan.remoteID)
	if err != nil {
		return sshContextExit(ctx, err)
	}
	defer cleanup()
	if err := ctx.Err(); err != nil {
		return &ExitError{Code: 130}
	}
	command := scpInvocation(access, plan, a.opts.Stdin, a.opts.Stdout, a.opts.Stderr)
	return sshContextExit(ctx, a.runSSHProcess(ctx, command))
}
