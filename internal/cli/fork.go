package cli

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
	"github.com/nathabonfim59/bamboo-raft/internal/machinetype"
)

// forkCommandOptions holds the flags of `bbraft fork`.
type forkCommandOptions struct {
	machineType string
	typeSet     bool
	env         []string
	labels      []string
	ttl         int
	ttlSet      bool
	noAutoStop  bool
	failFast    bool
	wait        bool
}

// newForkCommand builds `bbraft fork ID [flags]`.
func newForkCommand(a *App) *cobra.Command {
	var opts forkCommandOptions
	var cmd *cobra.Command
	cmd = &cobra.Command{
		Use:   "fork ID",
		Short: "Fork a sandbox into a new sandbox on a new snapshot chain",
		Args:  cobra.ExactArgs(1),
		RunE: a.runE(func(ctx context.Context, args []string) error {
			opts.typeSet = cmd.Flags().Changed("type")
			opts.ttlSet = cmd.Flags().Changed("ttl")
			req, err := opts.request()
			if err != nil {
				return &UsageError{Err: err}
			}
			client, err := a.apiClient(ctx)
			if err != nil {
				return err
			}
			return a.forkSandbox(ctx, client, args[0], opts.wait, req)
		}),
	}
	cmd.Flags().StringVar(&opts.machineType, "type", "", "machine size: small, default, large or xlarge")
	cmd.Flags().StringArrayVarP(&opts.env, "env", "e", nil, "environment variable KEY=VALUE (repeatable; replaces the source env)")
	cmd.Flags().IntVar(&opts.ttl, "ttl", 0, "seconds until the fork auto-stops (1 to 2592000)")
	cmd.Flags().BoolVar(&opts.noAutoStop, "no-auto-stop", false, "turn auto-stop off")
	cmd.Flags().BoolVar(&opts.failFast, "fail-fast", false, "fork only if a machine is ready now")
	cmd.Flags().StringArrayVar(&opts.labels, "label", nil, "label KEY=VALUE (repeatable)")
	cmd.Flags().BoolVar(&opts.wait, "wait", false, "wait until the forked sandbox is ready")
	return cmd
}

// request validates the fork flags and builds the v1 API request body.
func (o forkCommandOptions) request() (api.ForkSandboxRequest, error) {
	var req api.ForkSandboxRequest
	if o.ttlSet && o.noAutoStop {
		return req, errors.New("--ttl and --no-auto-stop cannot be used together")
	}
	if o.typeSet {
		machine, ok := machinetype.Lookup(o.machineType)
		if !ok || o.machineType == "" {
			names := make([]string, 0, len(machinetype.All))
			for _, candidate := range machinetype.All {
				names = append(names, candidate.Name)
			}
			return req, fmt.Errorf("unknown --type %q: use %s", o.machineType, strings.Join(names, ", "))
		}
		req.Type = new(machine.Name)
	}
	if o.ttlSet {
		if o.ttl < minTTLSeconds || o.ttl > maxTTLSeconds {
			return req, fmt.Errorf("--ttl must be between %d and %d seconds", minTTLSeconds, maxTTLSeconds)
		}
		req.TTLSeconds = api.TTLSeconds{Present: true, Value: new(o.ttl)}
	}
	if o.noAutoStop {
		req.TTLSeconds = api.TTLSeconds{Present: true}
	}
	if o.failFast {
		req.FailFast = new(true)
	}
	if len(o.env) > 0 {
		env, err := parseKeyValueFlag("--env", o.env)
		if err != nil {
			return req, err
		}
		req.Env = env
	}
	if len(o.labels) > 0 {
		labels, err := parseKeyValueFlag("--label", o.labels)
		if err != nil {
			return req, err
		}
		req.Labels = labels
	}
	return req, nil
}

// parseKeyValueFlag splits repeatable KEY=VALUE flag entries without echoing
// their values in error messages.
func parseKeyValueFlag(flag string, pairs []string) (map[string]string, error) {
	values := make(map[string]string, len(pairs))
	for _, pair := range pairs {
		key, value, ok := strings.Cut(pair, "=")
		if !ok || key == "" {
			return nil, fmt.Errorf("%s value must be KEY=VALUE", flag)
		}
		if strings.ContainsRune(key, '\x00') || strings.ContainsRune(value, '\x00') {
			return nil, fmt.Errorf("%s value contains a NUL byte", flag)
		}
		values[key] = value
	}
	return values, nil
}

// forkSandbox sends the fork request and either reports acceptance or polls
// until the forked sandbox is ready.
func (a *App) forkSandbox(ctx context.Context, client APIClient, sourceID string, wait bool, req api.ForkSandboxRequest) error {
	response, err := client.ForkSandbox(ctx, sourceID, req)
	if err != nil {
		return err
	}
	forkID := response.ID
	if forkID == "" || forkID == sourceID {
		return &CLIError{
			Code:    "invalid_response",
			Message: "fork response did not return a new sandbox id",
		}
	}
	if response.Sandbox != nil && response.Sandbox.ID != "" && response.Sandbox.ID != forkID {
		return &CLIError{
			Code:    "invalid_response",
			Message: "fork response returned mismatched sandbox ids",
		}
	}
	if !wait {
		if err := ctx.Err(); err != nil {
			return err
		}
		if a.out.JSON {
			return a.out.JSONLine(response)
		}
		return a.out.Humanf("%s", forkID)
	}

	sandbox := api.Sandbox{ID: forkID, State: api.StateProvisioning}
	if response.Sandbox != nil {
		sandbox = *response.Sandbox
		if sandbox.ID == "" {
			sandbox.ID = forkID
		}
		if sandbox.State == "" {
			sandbox.State = api.StateProvisioning
		}
	}
	if err := a.out.Event(
		newCreatedEvent{Event: "created", ID: forkID, State: sandbox.State},
		"created %s (%s)", forkID, sandbox.State,
	); err != nil {
		return err
	}
	if sandbox.State == api.StateReady {
		return a.reportReady(sandbox)
	}
	if err := terminalSandboxError(sandbox); err != nil {
		return err
	}
	return a.waitSandboxReady(ctx, client, sandbox)
}
