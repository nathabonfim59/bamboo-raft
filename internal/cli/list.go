package cli

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"text/tabwriter"

	"github.com/spf13/cobra"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
)

// sandboxStateGroup maps one CLI filter letter to pinned API states. The CLI
// calls the archived state "stopped"; the wire uses the pinned names.
func sandboxStateGroup(letter rune) ([]api.SandboxState, bool) {
	switch letter {
	case 'r':
		return []api.SandboxState{api.StateReady, api.StateCloning, api.StateIdle, api.StateRunning}, true
	case 's':
		return []api.SandboxState{api.StateArchived}, true
	case 'p':
		return []api.SandboxState{api.StateInit, api.StateProvisioning}, true
	case 't':
		return []api.SandboxState{api.StateArchiving}, true
	case 'e':
		return []api.SandboxState{api.StateError}, true
	default:
		return nil, false
	}
}

// newListCommand builds `bbraft list`.
func newListCommand(a *App) *cobra.Command {
	var (
		filter string
		all    bool
		limit  int
	)
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List your sandboxes",
		Args:  cobra.NoArgs,
		RunE: a.runE(func(ctx context.Context, _ []string) error {
			var states []api.SandboxState
			if !all {
				var err error
				states, err = parseStateFilter(filter)
				if err != nil {
					return &UsageError{Err: err}
				}
			}
			if _, err := parseLimit(limit); err != nil {
				return &UsageError{Err: err}
			}
			client, err := a.apiClient(ctx)
			if err != nil {
				return err
			}
			response, err := a.listAll(ctx, client, states, limit)
			if err != nil {
				return err
			}
			return a.printSandboxList(response)
		}),
	}
	cmd.Flags().StringVar(&filter, "filter", "r", "state groups: r (running), s (stopped), p (pending), t (stopping), e (error)")
	cmd.Flags().BoolVar(&all, "all", false, "list every state")
	cmd.Flags().IntVar(&limit, "limit", 100, "page size (1 to 200)")
	return cmd
}

// listAll follows cursors until the last page and returns one merged list. A
// non-empty state filter is sent on every page; --all passes none. A final
// empty list is an empty array, never null, and a page that claims another page
// without a usable cursor is an error rather than a silent partial list.
func (a *App) listAll(ctx context.Context, client APIClient, states []api.SandboxState, limit int) (api.SandboxListResponse, error) {
	var merged api.SandboxListResponse
	seen := map[string]bool{}
	cursor := ""
	first := true
	for {
		query := url.Values{}
		if len(states) > 0 {
			query.Set("state", joinStates(states))
		}
		query.Set("limit", strconv.Itoa(limit))
		if cursor != "" {
			query.Set("cursor", cursor)
		}
		page, err := client.ListSandboxes(ctx, query)
		if err != nil {
			return api.SandboxListResponse{}, err
		}
		if first {
			merged.SuccessBase = page.SuccessBase
			merged.DeletedCost = page.DeletedCost
			first = false
		}
		merged.Sandboxes = append(merged.Sandboxes, page.Sandboxes...)
		if !page.PageInfo.HasMore {
			break
		}
		if page.PageInfo.NextCursor == nil || *page.PageInfo.NextCursor == "" {
			return api.SandboxListResponse{}, &CLIError{
				Code:    "invalid_response",
				Message: "list pagination reported another page without a cursor",
			}
		}
		next := *page.PageInfo.NextCursor
		if seen[next] {
			return api.SandboxListResponse{}, &CLIError{Code: "invalid_response", Message: "list pagination returned a repeated cursor"}
		}
		seen[next] = true
		cursor = next
	}
	if merged.Sandboxes == nil {
		merged.Sandboxes = []api.Sandbox{}
	}
	merged.PageInfo = api.NewPageInfo(nil, limit)
	return merged, nil
}

// printSandboxList writes the pinned list envelope or a human table.
func (a *App) printSandboxList(response api.SandboxListResponse) error {
	if a.out.JSON {
		return a.out.JSONLine(response)
	}
	if len(response.Sandboxes) == 0 {
		return a.out.Humanf("no sandboxes")
	}
	table := tabwriter.NewWriter(a.out.Stdout, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(table, "ID\tSTATE\tTYPE\tNAME"); err != nil {
		return err
	}
	for _, sandbox := range response.Sandboxes {
		if _, err := fmt.Fprintf(table, "%s\t%s\t%s\t%s\n", sandbox.ID, sandbox.State, sandbox.Type, sandbox.Name); err != nil {
			return err
		}
	}
	return table.Flush()
}

// parseStateFilter expands the --filter letters, preserving first-seen order.
func parseStateFilter(filter string) ([]api.SandboxState, error) {
	if filter == "" {
		return nil, errors.New("--filter is empty: use r, s, p, t or e")
	}
	var states []api.SandboxState
	for _, letter := range filter {
		group, ok := sandboxStateGroup(letter)
		if !ok {
			return nil, fmt.Errorf("unknown --filter letter %q: use r, s, p, t or e", string(letter))
		}
		for _, state := range group {
			if !slices.Contains(states, state) {
				states = append(states, state)
			}
		}
	}
	return states, nil
}

// joinStates encodes a state filter for the list query.
func joinStates(states []api.SandboxState) string {
	parts := make([]string, len(states))
	for i, state := range states {
		parts[i] = string(state)
	}
	return strings.Join(parts, ",")
}

// parseLimit validates the list page size.
func parseLimit(limit int) (string, error) {
	if limit < 1 || limit > 200 {
		return "", errors.New("--limit must be between 1 and 200")
	}
	return strconv.Itoa(limit), nil
}
