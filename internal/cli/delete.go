package cli

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
)

// newDeleteCommand builds `bbraft delete ID [--yes] [--wait]`.
func newDeleteCommand(a *App) *cobra.Command {
	var (
		yes  bool
		wait bool
	)
	cmd := &cobra.Command{
		Use:   "delete ID",
		Short: "Delete a sandbox",
		Args:  cobra.ExactArgs(1),
		RunE: a.runE(func(ctx context.Context, args []string) error {
			id := args[0]
			if !yes {
				if !a.out.IsTTY || a.out.ExplicitJSON {
					return &UsageError{Err: fmt.Errorf("refusing to delete %s without --yes when not attached to a terminal", id)}
				}
				confirmed, err := a.confirmDelete(ctx, id)
				if err != nil {
					return err
				}
				if !confirmed {
					return &CLIError{Code: "aborted", Message: "delete cancelled"}
				}
			}
			client, err := a.apiClient(ctx)
			if err != nil {
				return err
			}
			response, err := client.DeleteSandbox(ctx, id)
			if err != nil {
				return err
			}
			if !a.out.JSON {
				if err := a.out.Humanf("deleting %s (operation %s, %s)", id, response.Operation.ID, response.Operation.Status); err != nil {
					return err
				}
			}
			if !wait {
				if a.out.JSON {
					return a.out.JSONLine(response)
				}
				return nil
			}
			final, err := a.waitDeletion(ctx, client, response.Operation, id)
			if err != nil {
				return err
			}
			if a.out.JSON {
				return a.out.JSONLine(final)
			}
			return a.out.Humanf("deleted %s", id)
		}),
	}
	cmd.Flags().BoolVarP(&yes, "yes", "y", false, "do not ask for confirmation")
	cmd.Flags().BoolVar(&wait, "wait", false, "wait until the deletion operation completes")
	return cmd
}

// waitDeletion polls the owner-scoped operation until it completes. A blocked
// operation is reported as a failure; it is not a deleted sandbox. Every poll
// must return the same operation for the same target, so a foreign record can
// never be reported as this deletion.
func (a *App) waitDeletion(ctx context.Context, client APIClient, initial api.DeletionOperation, targetID string) (api.DeletionOperationResponse, error) {
	waitCtx, cancel := a.waitContext(ctx)
	defer cancel()
	operation := initial
	for {
		if operation.ID != initial.ID || operation.Kind != api.DeletionKindSandbox || operation.TargetID != targetID {
			return api.DeletionOperationResponse{}, &CLIError{
				Code:    "invalid_response",
				Message: "deletion poll returned a different operation",
			}
		}
		switch operation.Status {
		case api.DeletionCompleted:
			return api.NewDeletionOperationResponse(operation), nil
		case api.DeletionBlocked:
			return api.DeletionOperationResponse{}, &CLIError{
				Code:    "deletion_blocked",
				Message: fmt.Sprintf("deletion operation %s is blocked; the server retries automatically", operation.ID),
			}
		case api.DeletionPending, api.DeletionProcessing:
		default:
			return api.DeletionOperationResponse{}, &CLIError{
				Code:    "invalid_response",
				Message: fmt.Sprintf("deletion operation %s has unknown status %q", operation.ID, operation.Status),
			}
		}
		if err := sleepCtx(waitCtx, a.opts.PollInterval); err != nil {
			return api.DeletionOperationResponse{}, a.waitFailure(waitCtx, "deletion "+operation.ID, err)
		}
		response, err := client.GetDeletionOperation(waitCtx, operation.ID)
		if err != nil {
			if waitCtx.Err() != nil {
				return api.DeletionOperationResponse{}, a.waitFailure(waitCtx, "deletion "+operation.ID, waitCtx.Err())
			}
			return api.DeletionOperationResponse{}, err
		}
		operation = response.Operation
	}
}

// confirmDelete asks on stderr and reads the answer from stdin.
func (a *App) confirmDelete(ctx context.Context, id string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if _, err := fmt.Fprintf(a.opts.Stderr, "Delete sandbox %s? This cannot be undone. Saved snapshots are kept. [y/N] ", id); err != nil {
		return false, errors.New("write confirmation prompt failed")
	}
	line := ""
	err := readWithContext(ctx, a.opts.Stdin, func(r io.Reader) error {
		var readErr error
		line, readErr = bufio.NewReader(r).ReadString('\n')
		return readErr
	})
	if err != nil && !errors.Is(err, io.EOF) {
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		return false, errors.New("read confirmation failed")
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	answer := strings.ToLower(strings.TrimSpace(line))
	return answer == "y" || answer == "yes", nil
}
