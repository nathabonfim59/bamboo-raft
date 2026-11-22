package cli

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
)

// snapshotEvent is one JSON Lines event of `snapshot ID NAME`.
type snapshotEvent struct {
	Event      string                  `json:"event"`
	Name       string                  `json:"name"`
	Status     api.SavedSnapshotStatus `json:"status"`
	SnapshotID string                  `json:"snapshotId,omitempty"`
}

// newSnapshotCommand builds `bbraft snapshot ID NAME`, `bbraft snapshot rm
// NAME`, `bbraft snapshot latest SANDBOX_ID`, and `bbraft snapshot
// share|shares|unshare`. The child subcommands cannot collide with the save
// form because its first argument is always a sandbox id (`bx_...`).
func newSnapshotCommand(a *App) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "snapshot ID NAME",
		Short: "Save a sandbox disk under a name",
		Args:  cobra.ExactArgs(2),
		RunE: a.runE(func(ctx context.Context, args []string) error {
			return a.saveSnapshot(ctx, args[0], args[1])
		}),
	}
	cmd.AddCommand(&cobra.Command{
		Use:   "latest SANDBOX_ID",
		Short: "Show the newest snapshot of a sandbox",
		Args:  cobra.ExactArgs(1),
		RunE: a.runE(func(ctx context.Context, args []string) error {
			client, err := a.apiClient(ctx)
			if err != nil {
				return err
			}
			response, err := client.GetLatestSandboxSnapshot(ctx, args[0])
			if err != nil {
				return err
			}
			return a.printSnapshotLatest(response, args[0])
		}),
	})
	cmd.AddCommand(&cobra.Command{
		Use:   "rm NAME",
		Short: "Remove a saved snapshot",
		Args:  cobra.ExactArgs(1),
		RunE: a.runE(func(ctx context.Context, args []string) error {
			client, err := a.apiClient(ctx)
			if err != nil {
				return err
			}
			response, err := client.DeleteSavedSnapshot(ctx, args[0])
			if err != nil {
				return err
			}
			if a.out.JSON {
				return a.out.JSONLine(response)
			}
			return a.out.Humanf("deleted snapshot %s", response.Name)
		}),
	})
	cmd.AddCommand(
		newSnapshotShareCommand(a),
		newSnapshotSharesCommand(a),
		newSnapshotUnshareCommand(a),
	)
	return cmd
}

// saveSnapshot starts a save and polls until the snapshot is ready or failed.
// It pins the accepted generation (source, createdAt and, when present, the
// artifact id) so a concurrent replacement under the same name is never
// reported as this save.
func (a *App) saveSnapshot(ctx context.Context, sandboxID, name string) error {
	client, err := a.apiClient(ctx)
	if err != nil {
		return err
	}
	accepted, err := client.SaveSnapshot(ctx, api.SavedSnapshotSaveRequest{SandboxID: sandboxID, Name: name})
	if err != nil {
		return err
	}
	generation := accepted.Snapshot
	if generation.Name != name || generation.SourceSandboxID != sandboxID || generation.CreatedAt.IsZero() {
		return &CLIError{Code: "invalid_response", Message: "save response has no usable snapshot generation"}
	}
	if err := a.out.Event(
		snapshotEvent{Event: "saving", Name: name, Status: api.SavedSnapshotSaving},
		"saving snapshot %s", name,
	); err != nil {
		return err
	}
	waitCtx, cancel := a.waitContext(ctx)
	defer cancel()
	for {
		info, err := client.GetSavedSnapshot(waitCtx, name)
		if err != nil {
			if waitCtx.Err() != nil {
				return a.waitFailure(waitCtx, "snapshot "+name, waitCtx.Err())
			}
			return err
		}
		snapshot := info.Snapshot
		if err := checkSnapshotGeneration(generation, snapshot); err != nil {
			return err
		}
		switch snapshot.Status {
		case api.SavedSnapshotReady:
			return a.out.Event(
				snapshotEvent{Event: "ready", Name: name, Status: snapshot.Status, SnapshotID: snapshot.SnapshotID},
				"snapshot %s ready (%s)", name, snapshot.SnapshotID,
			)
		case api.SavedSnapshotFailed:
			message := fmt.Sprintf("snapshot %s failed", name)
			if snapshot.Error != "" {
				message += ": " + snapshot.Error
			}
			return &CLIError{Code: "snapshot_failed", Message: message}
		case api.SavedSnapshotSaving:
			// Keep polling.
		default:
			return &CLIError{
				Code:    "invalid_response",
				Message: fmt.Sprintf("snapshot %s has unknown status %q", name, snapshot.Status),
			}
		}
		if err := sleepCtx(waitCtx, a.opts.PollInterval); err != nil {
			return a.waitFailure(waitCtx, "snapshot "+name, err)
		}
	}
}

// checkSnapshotGeneration rejects a poll result that belongs to a different
// save under the same name.
func checkSnapshotGeneration(accepted, current api.SavedSnapshot) error {
	if current.Name != accepted.Name ||
		current.SourceSandboxID != accepted.SourceSandboxID ||
		!current.CreatedAt.Equal(accepted.CreatedAt) ||
		(accepted.SnapshotID != "" && current.SnapshotID != accepted.SnapshotID) {
		return &CLIError{
			Code:    "invalid_response",
			Message: fmt.Sprintf("snapshot poll for %s returned a different save generation", accepted.Name),
		}
	}
	return nil
}
