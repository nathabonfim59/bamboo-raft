package cli

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/spf13/cobra"
)

// newRecoverCommand builds `bbraft recover ID`.
func newRecoverCommand(a *App) *cobra.Command {
	return &cobra.Command{
		Use:   "recover ID",
		Short: "Power-cycle an unresponsive running sandbox",
		Args:  cobra.ExactArgs(1),
		RunE: a.runE(func(ctx context.Context, args []string) error {
			client, err := a.apiClient(ctx)
			if err != nil {
				return err
			}
			response, err := client.RecoverSandbox(ctx, args[0])
			if err != nil {
				return recoverConflictError(err)
			}
			if a.out.JSON {
				return a.out.JSONLine(response)
			}
			if response.Message != "" {
				return a.out.Humanf("%s", response.Message)
			}
			return a.out.Humanf("recovering %s", args[0])
		}),
	}
}

// recoverConflictError ensures every documented 409 recovery refusal renders
// as a clear single-line error while keeping its API status and code intact.
func recoverConflictError(err error) error {
	apiErr, ok := errors.AsType[*APIError](err)
	if !ok || apiErr.Status != http.StatusConflict {
		return err
	}
	var fallback string
	switch apiErr.Code {
	case "no_machine":
		fallback = "sandbox has no running machine; resume it instead"
	case "too_soon":
		fallback = "a recovery was already started within the last 10 minutes"
	case "recover_unsupported":
		fallback = "the sandbox machine cannot be power-cycled right now"
	case "sandbox_is_reachable":
		fallback = "sandbox is reachable and responding normally"
	default:
		return err
	}
	message := strings.Join(strings.Fields(apiErr.Message), " ")
	if message == "" {
		message = fallback
	}
	out := *apiErr
	out.Message = message
	return &out
}
