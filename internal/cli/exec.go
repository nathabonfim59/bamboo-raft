package cli

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
)

// newExecCommand builds `bbraft exec ID [command...]`.
func newExecCommand(a *App) *cobra.Command {
	var (
		cwd      string
		timeout  = execDefaultTimeoutSeconds
		stream   bool
		detach   bool
		statusID string
		killID   string
		attachID string
	)
	var cmd *cobra.Command
	cmd = &cobra.Command{
		Use:   "exec ID [command...]",
		Short: "Run a command in a sandbox",
		Long: "Run a command in a sandbox through the v1 API.\n\n" +
			"One command argument is a shell command line and is sent unchanged. " +
			"Several arguments are argv: each is POSIX-shell quoted and joined, so " +
			"the remote shell reproduces exactly those words.\n\n" +
			"The command's exit code is returned. A timeout exits 124. Above 600 " +
			"seconds, or with --timeout 0, output streams; zero discovers the " +
			"server's streamed limit instead of sending zero.\n\n" +
			"--detach starts the command in the background and prints its process " +
			"id. --status PID reads its state and log tails, --attach PID follows " +
			"its output from the start, and --kill PID stops it and every process " +
			"it started. The three process modes take no command and no --timeout, " +
			"--cwd or --stream.\n\n" +
			"A long command on a server that supports detached commands runs in " +
			"the background and is followed with reconnects, so a network cut does " +
			"not lose output. The command is started exactly once.",
		Args: cobra.ArbitraryArgs,
		RunE: a.runE(func(ctx context.Context, args []string) error {
			mode, processID, err := execResolveMode(cmd)
			if err != nil {
				return &UsageError{Err: err}
			}
			if execModeNeedsCommand(mode) {
				if len(args) < 2 {
					return &UsageError{Err: errors.New("exec needs a sandbox ID and a command")}
				}
				if timeout < 0 || timeout > execMaxTimeoutSeconds {
					return &UsageError{Err: fmt.Errorf("--timeout must be between 0 and %d seconds", execMaxTimeoutSeconds)}
				}
			} else if len(args) != 1 {
				return &UsageError{Err: fmt.Errorf("exec --%s needs a sandbox ID and no command", execModeName(mode))}
			}

			var commandLine string
			if execModeNeedsCommand(mode) {
				commandLine = execCommandLine(args[1:])
				if strings.TrimSpace(commandLine) == "" {
					return &UsageError{Err: errors.New("exec needs a non-empty command")}
				}
			}
			client, err := a.apiClient(ctx)
			if err != nil {
				return err
			}
			switch mode {
			case execModeDetach:
				return a.execDetach(ctx, client, args[0], commandLine, cwd)
			case execModeStatus:
				return a.execStatus(ctx, client, args[0], processID)
			case execModeKill:
				return a.execKill(ctx, client, args[0], processID)
			case execModeAttach:
				return a.execAttach(ctx, client, args[0], processID)
			}

			out := ExecutionOutput{Stdout: a.out.Stdout, Stderr: a.out.Stderr}
			if a.out.JSON {
				out.JSON = a.out.Stdout
			}
			request := ExecutionRequest{
				SandboxID:      args[0],
				Command:        commandLine,
				Cwd:            cwd,
				TimeoutSeconds: timeout,
				Stream:         stream,
			}
			if timeout > execMaxSyncSeconds {
				capable, probeErr := execDetachedCapability(ctx, client)
				if probeErr != nil {
					return probeErr
				}
				request.DetachedAllowed = capable
			}
			_, err = ExecuteCommand(ctx, client, request, out)
			return err
		}),
	}
	cmd.Flags().StringVar(&cwd, "cwd", "", "working directory relative to the sandbox work directory")
	cmd.Flags().Int64Var(&timeout, "timeout", execDefaultTimeoutSeconds, "stop after this many seconds (0 discovers the streamed limit; above 600 streams)")
	cmd.Flags().BoolVar(&stream, "stream", false, "print stdout and stderr as the command writes them")
	cmd.Flags().BoolVar(&detach, "detach", false, "run in the background and print the process id")
	cmd.Flags().StringVar(&statusID, "status", "", "show the status and logs of a process started with --detach")
	cmd.Flags().StringVar(&killID, "kill", "", "stop a process started with --detach and every process it started")
	cmd.Flags().StringVar(&attachID, "attach", "", "print the output of a process started with --detach until it ends")
	return cmd
}

// execCommandLine builds the `bash -lc` line for the exec arguments. One
// argument is a shell command line and stays byte-for-byte unchanged; more
// than one is argv and every word is quoted independently.
func execCommandLine(args []string) string {
	if len(args) == 1 {
		return args[0]
	}
	return execShellJoin(args)
}
