package cli

import (
	"cmp"
	"context"
	"fmt"
	"strconv"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
)

// newSnapshotShareCommand builds `bbraft snapshot share NAME`.
func newSnapshotShareCommand(a *App) *cobra.Command {
	return &cobra.Command{
		Use:   "share NAME",
		Short: "Create a share code for a saved snapshot",
		Args:  cobra.ExactArgs(1),
		RunE: a.runE(func(ctx context.Context, args []string) error {
			client, err := a.apiClient(ctx)
			if err != nil {
				return err
			}
			response, err := client.CreateSnapshotShare(ctx, args[0])
			if err != nil {
				return err
			}
			if response.ShareCode == "" || response.Share.ID == "" {
				return &CLIError{
					Code:    "invalid_response",
					Message: "share response is missing code or id",
				}
			}
			if a.out.JSON {
				return a.out.JSONLine(response)
			}
			if err := a.out.Humanf("share: %s", response.Share.ID); err != nil {
				return err
			}
			if err := a.out.Humanf("code:  %s", response.ShareCode); err != nil {
				return err
			}
			return a.out.Humanf("Save this code now; it is shown only once.")
		}),
	}
}

// newSnapshotSharesCommand builds `bbraft snapshot shares NAME`.
func newSnapshotSharesCommand(a *App) *cobra.Command {
	return &cobra.Command{
		Use:   "shares NAME",
		Short: "List share codes for a saved snapshot",
		Args:  cobra.ExactArgs(1),
		RunE: a.runE(func(ctx context.Context, args []string) error {
			client, err := a.apiClient(ctx)
			if err != nil {
				return err
			}
			response, err := client.ListSnapshotShares(ctx, args[0])
			if err != nil {
				return err
			}
			if response.Shares == nil {
				response.Shares = []api.SnapshotShare{}
			}
			if a.out.JSON {
				return a.out.JSONLine(response)
			}
			if len(response.Shares) == 0 {
				return a.out.Humanf("no shares for %s", args[0])
			}
			return a.printSnapshotShareRows(response.Shares)
		}),
	}
}

// newSnapshotUnshareCommand builds `bbraft snapshot unshare NAME SHARE_ID`.
func newSnapshotUnshareCommand(a *App) *cobra.Command {
	return &cobra.Command{
		Use:   "unshare NAME SHARE_ID",
		Short: "Revoke a saved snapshot share code",
		Args:  cobra.ExactArgs(2),
		RunE: a.runE(func(ctx context.Context, args []string) error {
			client, err := a.apiClient(ctx)
			if err != nil {
				return err
			}
			response, err := client.RevokeSnapshotShare(ctx, args[0], args[1])
			if err != nil {
				return err
			}
			if response.Share.ID != "" && response.Share.ID != args[1] {
				return &CLIError{
					Code:    "invalid_response",
					Message: fmt.Sprintf("unshare response returned id %q while revoking %q", response.Share.ID, args[1]),
				}
			}
			if a.out.JSON {
				return a.out.JSONLine(response)
			}
			id := cmp.Or(response.Share.ID, args[1])
			name := cmp.Or(response.Name, args[0])
			return a.out.Humanf("revoked share %s for %s", id, name)
		}),
	}
}

// printSnapshotShareRows writes the human share table.
func (a *App) printSnapshotShareRows(shares []api.SnapshotShare) error {
	table := tabwriter.NewWriter(a.out.Stdout, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(table, "ID\tPREFIX\tSTATUS\tUSES\tLAST USED\tCREATED"); err != nil {
		return err
	}
	for _, share := range shares {
		if _, err := fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%s\t%s\n",
			share.ID, share.CodePrefix, share.Status,
			strconv.FormatInt(share.UseCount, 10),
			shareOptionalTime(share.LastUsedAt),
			share.CreatedAt.UTC().Format(time.RFC3339)); err != nil {
			return err
		}
	}
	return table.Flush()
}

// shareOptionalTime renders a nullable timestamp for the shares table.
func shareOptionalTime(t *time.Time) string {
	if t == nil {
		return "-"
	}
	return t.UTC().Format(time.RFC3339)
}
