package cli

import (
	"context"
	"errors"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
)

// extendMaxHours is the pinned 30-day maximum expressed in whole hours.
const extendMaxHours = maxTTLSeconds / 3600

// newExtendCommand builds `bbraft extend ID (--hours H | --ttl N | --no-auto-stop)`.
//
// The flags set the remaining auto-stop time from now; they never add to the
// current timer. Exactly one selector is required. --hours is a whole number
// of hours; a fractional value is a usage error from Cobra. An explicit
// --no-auto-stop=false is not a selector, so it can never silently disable a
// timer.
func newExtendCommand(a *App) *cobra.Command {
	var (
		hours      int
		ttl        int
		noAutoStop bool
	)
	var cmd *cobra.Command
	cmd = &cobra.Command{
		Use:   "extend ID",
		Short: "Set how long a sandbox keeps running before auto-stop",
		Args:  cobra.ExactArgs(1),
		RunE: a.runE(func(ctx context.Context, args []string) error {
			hoursSet := cmd.Flags().Changed("hours")
			ttlSet := cmd.Flags().Changed("ttl")
			selected := 0
			if hoursSet {
				selected++
			}
			if ttlSet {
				selected++
			}
			if noAutoStop {
				selected++
			}
			if selected != 1 {
				return &UsageError{Err: errors.New("exactly one of --hours, --ttl or --no-auto-stop is required")}
			}
			var request api.UpdateSandboxRequest
			switch {
			case hoursSet:
				if hours < 1 || hours > extendMaxHours {
					return &UsageError{Err: fmt.Errorf("--hours must be between 1 and %d", extendMaxHours)}
				}
				request.TTLSeconds = lifecycleTTLJSON(hours * 3600)
			case ttlSet:
				if ttl < minTTLSeconds || ttl > maxTTLSeconds {
					return &UsageError{Err: fmt.Errorf("--ttl must be between %d and %d seconds", minTTLSeconds, maxTTLSeconds)}
				}
				request.TTLSeconds = lifecycleTTLJSON(ttl)
			default:
				request.TTLSeconds = lifecycleTTLDisabled()
			}
			client, err := a.apiClient(ctx)
			if err != nil {
				return err
			}
			return a.extendSandbox(ctx, client, args[0], request)
		}),
	}
	cmd.Flags().IntVar(&hours, "hours", 0, "whole hours until auto-stop (1 to 720)")
	cmd.Flags().IntVar(&ttl, "ttl", 0, "seconds until auto-stop (1 to 2592000)")
	cmd.Flags().BoolVar(&noAutoStop, "no-auto-stop", false, "keep the sandbox running until it is stopped")
	return cmd
}

// extendSandbox applies the PATCH and reports the updated timer.
func (a *App) extendSandbox(ctx context.Context, client APIClient, id string, request api.UpdateSandboxRequest) error {
	response, err := client.UpdateSandbox(ctx, id, request)
	if err != nil {
		return err
	}
	if response.Sandbox.ID != id {
		return &CLIError{
			Code:    "invalid_response",
			Message: fmt.Sprintf("update response returned id %q while extending %q", response.Sandbox.ID, id),
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if a.out.JSON {
		return a.out.JSONLine(response)
	}
	sandbox := response.Sandbox
	// Only a null TTL disables auto-stop. A configured TTL with no active
	// deadline is still a timer, just not one the server has scheduled yet.
	if sandbox.TTLSeconds == nil {
		return a.out.Humanf("extended %s: auto-stop disabled", id)
	}
	if sandbox.ArchiveAfter == nil {
		return a.out.Humanf("extended %s: auto-stop configured for %d seconds", id, *sandbox.TTLSeconds)
	}
	return a.out.Humanf("extended %s: auto-stop at %s (%d seconds)", id, formatTime(*sandbox.ArchiveAfter), *sandbox.TTLSeconds)
}
