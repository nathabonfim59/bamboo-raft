package cli

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
)

// newInfoCommand builds `bbraft info ID`.
func newInfoCommand(a *App) *cobra.Command {
	return &cobra.Command{
		Use:   "info ID",
		Short: "Show one sandbox",
		Args:  cobra.ExactArgs(1),
		RunE: a.runE(func(ctx context.Context, args []string) error {
			client, err := a.apiClient(ctx)
			if err != nil {
				return err
			}
			response, err := client.GetSandbox(ctx, args[0])
			if err != nil {
				return err
			}
			if a.out.JSON {
				return a.out.JSONLine(response)
			}
			return a.printSandboxInfo(response.Sandbox)
		}),
	}
}

// printSandboxInfo writes the human detail block.
func (a *App) printSandboxInfo(sandbox api.Sandbox) error {
	lines := []string{
		fmt.Sprintf("id:           %s", sandbox.ID),
		fmt.Sprintf("name:         %s", sandbox.Name),
		fmt.Sprintf("state:        %s", sandbox.State),
	}
	if sandbox.Type != "" {
		lines = append(lines, fmt.Sprintf("type:         %s", sandbox.Type))
	}
	if sandbox.IP != nil {
		lines = append(lines, fmt.Sprintf("ip:           %s", *sandbox.IP))
	}
	if sandbox.URL != nil {
		lines = append(lines, fmt.Sprintf("url:          %s", *sandbox.URL))
	}
	if sandbox.CreatedAt != nil {
		lines = append(lines, fmt.Sprintf("createdAt:    %s", formatTime(*sandbox.CreatedAt)))
	}
	if sandbox.ArchiveAfter != nil {
		lines = append(lines, fmt.Sprintf("archiveAfter: %s", formatTime(*sandbox.ArchiveAfter)))
	}
	if sandbox.TTLSeconds != nil {
		lines = append(lines, fmt.Sprintf("ttlSeconds:   %d", *sandbox.TTLSeconds))
	}
	if sandbox.Error != nil && *sandbox.Error != "" {
		lines = append(lines, fmt.Sprintf("error:        %s", *sandbox.Error))
	}
	for _, line := range lines {
		if err := a.out.Humanf("%s", line); err != nil {
			return err
		}
	}
	return nil
}

// formatTime prints an API timestamp in the human layout.
func formatTime(t time.Time) string { return t.Format(time.RFC3339) }
