package cli

import (
	"context"
	"errors"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
)

// stopCommandOptions holds the flags of `bbraft stop`.
type stopCommandOptions struct {
	yes    bool
	force  bool
	status string
}

// newStopCommand builds `bbraft stop ID [--yes] [--force] [--status STOP_ID]`.
//
// A stop is asynchronous: the command accepts the pinned 202 and returns. An
// already stopped sandbox answers 200 with its last operation. --yes is
// accepted so scripts can pass the same flag as for delete, but stop never
// asks for confirmation. --status reads one stop by id through the fixed
// owner-scoped sandbox path.
func newStopCommand(a *App) *cobra.Command {
	var opts stopCommandOptions
	var cmd *cobra.Command
	cmd = &cobra.Command{
		Use:   "stop ID",
		Short: "Stop a sandbox and keep its disk for a later resume",
		Args:  cobra.ExactArgs(1),
		RunE: a.runE(func(ctx context.Context, args []string) error {
			statusSet := cmd.Flags().Changed("status")
			forceSet := cmd.Flags().Changed("force")
			if statusSet && opts.status == "" {
				return &UsageError{Err: errors.New("--status requires a stop id")}
			}
			if statusSet && forceSet {
				return &UsageError{Err: errors.New("--force cannot be used with --status")}
			}
			client, err := a.apiClient(ctx)
			if err != nil {
				return err
			}
			if statusSet {
				return a.readStopStatus(ctx, client, args[0], opts.status)
			}
			return a.stopSandbox(ctx, client, args[0], opts.force)
		}),
	}
	cmd.Flags().BoolVarP(&opts.yes, "yes", "y", false, "accepted for scripts; stop never asks for confirmation")
	cmd.Flags().BoolVar(&opts.force, "force", false, "force an asynchronous power-off after a one-second grace period, retaining the local disk")
	cmd.Flags().StringVar(&opts.status, "status", "", "read this stop id instead of starting a stop")
	return cmd
}

// stopSandbox accepts one stop and reports it. The returned id is checked and
// the envelope must carry one of the two pinned stop types. A server-supplied
// statusUrl is never followed: the CLI builds every path from the configured
// base URL.
func (a *App) stopSandbox(ctx context.Context, client APIClient, id string, force bool) error {
	response, err := client.StopSandbox(ctx, id, force)
	if err != nil {
		return err
	}
	if response.ID != id {
		return &CLIError{
			Code:    "invalid_response",
			Message: fmt.Sprintf("stop response returned id %q while stopping %q", response.ID, id),
		}
	}
	if response.Sandbox != nil && response.Sandbox.ID != id {
		// Do not quote the foreign id: this response must be rejected before
		// any of its sandbox data reaches the user.
		return &CLIError{Code: "invalid_response", Message: "stop response returned a different sandbox"}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if a.out.JSON {
		return a.out.JSONLine(response)
	}
	verb := "stopping"
	if response.Type == api.TypeSandboxStopped || response.Status == string(api.StateArchived) {
		verb = "stopped"
	}
	if response.Operation != nil {
		return a.out.Humanf("%s %s (stop %s, %s)", verb, id, response.Operation.ID, response.Operation.Status)
	}
	return a.out.Humanf("%s %s", verb, id)
}

// readStopStatus reads one stop by id and prints its pinned StopOperation.
func (a *App) readStopStatus(ctx context.Context, client APIClient, id, stopID string) error {
	response, err := client.GetSandboxStop(ctx, id, stopID)
	if err != nil {
		return err
	}
	if response.ID != id || response.Stop.ID != stopID {
		return &CLIError{
			Code:    "invalid_response",
			Message: fmt.Sprintf("stop %s does not belong to sandbox %s", stopID, id),
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if a.out.JSON {
		return a.out.JSONLine(response)
	}
	return a.printStopOperation(id, response.Stop)
}

// printStopOperation writes the human stop status block. It never claims that
// a snapshot was saved; the operation is the only lifecycle fact the API
// exposes.
func (a *App) printStopOperation(sandboxID string, stop api.StopOperation) error {
	lines := []string{
		fmt.Sprintf("id:             %s", sandboxID),
		fmt.Sprintf("stop:           %s", stop.ID),
		fmt.Sprintf("status:         %s", stop.Status),
	}
	if stop.RequestedAt != nil {
		lines = append(lines, fmt.Sprintf("requestedAt:    %s", formatTime(*stop.RequestedAt)))
	}
	if stop.LastAttemptAt != nil {
		lines = append(lines, fmt.Sprintf("lastAttemptAt:  %s", formatTime(*stop.LastAttemptAt)))
	}
	if stop.EndedAt != nil {
		lines = append(lines, fmt.Sprintf("endedAt:        %s", formatTime(*stop.EndedAt)))
	}
	if stop.Error != nil && *stop.Error != "" {
		lines = append(lines, fmt.Sprintf("error:          %s", *stop.Error))
	}
	if stop.Forced {
		lines = append(lines, "forced:         true")
	}
	for _, line := range lines {
		if err := a.out.Humanf("%s", line); err != nil {
			return err
		}
	}
	return nil
}
