package cli

import (
	"context"
	"fmt"
	"unicode/utf8"

	"github.com/spf13/cobra"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
)

// newRenameCommand builds `bbraft rename ID NAME`.
//
// The new name follows the same 1..120 Unicode character limit as `bbraft new`
// and the pinned UpdateSandboxRequest, so the CLI never silently truncates a
// name.
func newRenameCommand(a *App) *cobra.Command {
	return &cobra.Command{
		Use:   "rename ID NAME",
		Short: "Change the name of a sandbox",
		Args:  cobra.ExactArgs(2),
		RunE: a.runE(func(ctx context.Context, args []string) error {
			id, name := args[0], args[1]
			if count := utf8.RuneCountInString(name); count < 1 || count > maxNameChars {
				return &UsageError{Err: fmt.Errorf("NAME must be between 1 and %d characters", maxNameChars)}
			}
			client, err := a.apiClient(ctx)
			if err != nil {
				return err
			}
			response, err := client.UpdateSandbox(ctx, id, api.UpdateSandboxRequest{Name: new(name)})
			if err != nil {
				return err
			}
			if response.Sandbox.ID != id {
				return &CLIError{
					Code:    "invalid_response",
					Message: fmt.Sprintf("update response returned id %q while renaming %q", response.Sandbox.ID, id),
				}
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			if a.out.JSON {
				return a.out.JSONLine(response)
			}
			return a.out.Humanf("renamed %s to %q", id, name)
		}),
	}
}
