package cli

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
)

// newSnapshotsCommand builds `bbraft snapshots [SANDBOX_ID]`. Without an
// argument it lists every snapshot of the account; with one it lists that
// sandbox's snapshots. One page is read unless --all follows the cursors.
func newSnapshotsCommand(a *App) *cobra.Command {
	var (
		all   bool
		limit int
	)
	cmd := &cobra.Command{
		Use:   "snapshots [SANDBOX_ID]",
		Short: "List the snapshots your sandboxes captured",
		Args:  cobra.MaximumNArgs(1),
		RunE: a.runE(func(ctx context.Context, args []string) error {
			if _, err := parseLimit(limit); err != nil {
				return &UsageError{Err: err}
			}
			client, err := a.apiClient(ctx)
			if err != nil {
				return err
			}
			sandboxID := ""
			if len(args) == 1 {
				sandboxID = args[0]
			}
			response, err := a.listSnapshots(ctx, client, sandboxID, limit, all)
			if err != nil {
				return err
			}
			return a.printSnapshotList(response)
		}),
	}
	cmd.Flags().BoolVar(&all, "all", false, "follow pagination to the last page")
	cmd.Flags().IntVar(&limit, "limit", 100, "page size (1 to 200)")
	return cmd
}

// listSnapshots reads one page, or every page with all, and returns the merged
// envelope. A page that claims another page without a usable cursor, or that
// repeats a cursor, is an error rather than a silent partial list.
func (a *App) listSnapshots(ctx context.Context, client APIClient, sandboxID string, limit int, all bool) (api.SnapshotListResponse, error) {
	query := url.Values{}
	query.Set("limit", strconv.Itoa(limit))
	var merged api.SnapshotListResponse
	seen := map[string]bool{}
	for {
		page, err := client.ListSnapshots(ctx, sandboxID, query)
		if err != nil {
			return api.SnapshotListResponse{}, err
		}
		if merged.Type == "" {
			merged.SuccessBase = page.SuccessBase
		}
		merged.Snapshots = append(merged.Snapshots, page.Snapshots...)
		if !all || !page.PageInfo.HasMore {
			if all {
				merged.PageInfo = api.NewPageInfo(nil, limit)
			} else {
				merged.PageInfo = page.PageInfo
			}
			break
		}
		if page.PageInfo.NextCursor == nil || *page.PageInfo.NextCursor == "" {
			return api.SnapshotListResponse{}, &CLIError{
				Code:    "invalid_response",
				Message: "snapshot pagination reported another page without a cursor",
			}
		}
		next := *page.PageInfo.NextCursor
		if seen[next] {
			return api.SnapshotListResponse{}, &CLIError{
				Code:    "invalid_response",
				Message: "snapshot pagination returned a repeated cursor",
			}
		}
		seen[next] = true
		query.Set("cursor", next)
	}
	if merged.Snapshots == nil {
		merged.Snapshots = []api.SnapshotSummary{}
	}
	return merged, nil
}

// printSnapshotList writes the snapshot list envelope or a human table.
func (a *App) printSnapshotList(response api.SnapshotListResponse) error {
	if a.out.JSON {
		return a.out.JSONLine(response)
	}
	if len(response.Snapshots) == 0 {
		return a.out.Humanf("no snapshots")
	}
	return a.printSnapshotRows(response.Snapshots)
}

// printSnapshotLatest writes the latest-snapshot envelope or a human table.
func (a *App) printSnapshotLatest(response api.SnapshotLatestResponse, sandboxID string) error {
	if a.out.JSON {
		return a.out.JSONLine(response)
	}
	if response.Snapshot == nil {
		return a.out.Humanf("no snapshots for %s", sandboxID)
	}
	return a.printSnapshotRows([]api.SnapshotSummary{*response.Snapshot})
}

// printSnapshotRows writes the human snapshot table.
func (a *App) printSnapshotRows(snapshots []api.SnapshotSummary) error {
	table := tabwriter.NewWriter(a.out.Stdout, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(table, "ID\tSANDBOX\tGEN\tKIND\tSIZE\tCREATED"); err != nil {
		return err
	}
	for _, snapshot := range snapshots {
		if _, err := fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%s\t%s\n",
			snapshot.ID, snapshot.SandboxID, snapshotGeneration(snapshot.Generation),
			snapshotKind(snapshot.Kind), snapshotSize(snapshot.SizeBytes),
			snapshot.CreatedAt.UTC().Format(time.RFC3339)); err != nil {
			return err
		}
	}
	return table.Flush()
}

// snapshotGeneration renders the nullable chain position.
func snapshotGeneration(generation *int64) string {
	if generation == nil {
		return "-"
	}
	return strconv.FormatInt(*generation, 10)
}

// snapshotKind renders the nullable generation kind.
func snapshotKind(kind *api.SnapshotKind) string {
	if kind == nil || *kind == "" {
		return "-"
	}
	return string(*kind)
}

// snapshotSize renders the nullable added-bytes count.
func snapshotSize(size *int64) string {
	if size == nil {
		return "-"
	}
	return strconv.FormatInt(*size, 10)
}
