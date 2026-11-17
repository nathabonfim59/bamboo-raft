package cli

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/spf13/cobra"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
	"github.com/nathabonfim59/bamboo-raft/internal/machinetype"
)

// TTL bounds from the pinned CLI reference.
const (
	minTTLSeconds = 1
	maxTTLSeconds = 2592000
	maxNameChars  = 120
)

// newCreatedEvent is the first JSON Lines object of `new`.
type newCreatedEvent struct {
	Event string           `json:"event"`
	ID    string           `json:"id"`
	State api.SandboxState `json:"state"`
}

// newStateEvent reports a sandbox state change while waiting.
type newStateEvent struct {
	Event string           `json:"event"`
	ID    string           `json:"id"`
	State api.SandboxState `json:"state"`
}

// newReadyEvent is the single terminal JSON Lines object agent harnesses parse.
type newReadyEvent struct {
	Event string `json:"event"`
	ID    string `json:"id"`
}

// newCommandOptions holds the flags of `bbraft new`.
type newCommandOptions struct {
	machineType string
	name        string
	ttl         int
	ttlSet      bool
	from        string
	env         []string
}

// newNewCommand builds `bbraft new`.
func newNewCommand(a *App) *cobra.Command {
	var opts newCommandOptions
	cmd := &cobra.Command{
		Use:   "new",
		Short: "Create a sandbox and wait until it is ready",
		Args:  cobra.NoArgs,
		RunE: a.runE(func(ctx context.Context, _ []string) error {
			req, err := opts.request()
			if err != nil {
				return &UsageError{Err: err}
			}
			var ttl *int
			if opts.ttlSet {
				ttl = &opts.ttl
			}
			client, err := a.apiClient(ctx)
			if err != nil {
				return err
			}
			created, err := client.CreateSandbox(ctx, req)
			if err != nil {
				return createFailure(err, ttl)
			}
			id := created.Sandbox.ID
			if id == "" {
				return ambiguousCreate(ttl)
			}
			if err := a.out.Event(
				newCreatedEvent{Event: "created", ID: id, State: created.Sandbox.State},
				"created %s (%s)", id, created.Sandbox.State,
			); err != nil {
				return err
			}
			if created.Sandbox.State == api.StateReady {
				return a.reportReady(created.Sandbox)
			}
			if err := terminalSandboxError(created.Sandbox); err != nil {
				return err
			}
			return a.waitSandboxReady(ctx, client, created.Sandbox)
		}),
	}
	cmd.Flags().StringVar(&opts.machineType, "type", "", "machine size: small, default, large or xlarge")
	cmd.Flags().StringVar(&opts.name, "name", "", "display name")
	cmd.Flags().IntVar(&opts.ttl, "ttl", 0, "seconds until the sandbox auto-stops (1 to 2592000)")
	cmd.Flags().StringVar(&opts.from, "from", "", "saved snapshot name to create from")
	cmd.Flags().StringArrayVarP(&opts.env, "env", "e", nil, "environment variable KEY=VALUE (repeatable)")
	cmd.PreRun = func(*cobra.Command, []string) {
		opts.ttlSet = cmd.Flags().Changed("ttl")
	}
	return cmd
}

// request validates the flags and builds the pinned create body.
func (o newCommandOptions) request() (api.CreateSandboxRequest, error) {
	var req api.CreateSandboxRequest
	if o.machineType != "" {
		if _, ok := machinetype.Lookup(o.machineType); !ok {
			names := make([]string, 0, len(machinetype.All))
			for _, t := range machinetype.All {
				names = append(names, t.Name)
			}
			return req, fmt.Errorf("unknown --type %q: use %s", o.machineType, strings.Join(names, ", "))
		}
		req.Type = &o.machineType
	}
	if o.name != "" {
		if utf8.RuneCountInString(o.name) > maxNameChars {
			return req, fmt.Errorf("--name is longer than %d characters", maxNameChars)
		}
		req.Name = &o.name
	}
	if o.ttlSet {
		if o.ttl < minTTLSeconds || o.ttl > maxTTLSeconds {
			return req, fmt.Errorf("--ttl must be between %d and %d seconds", minTTLSeconds, maxTTLSeconds)
		}
		ttl := o.ttl
		req.TTLSeconds = api.TTLSeconds{Present: true, Value: &ttl}
	}
	if o.from != "" {
		req.From = &o.from
	}
	if len(o.env) > 0 {
		env := make(map[string]string, len(o.env))
		for _, pair := range o.env {
			key, value, ok := strings.Cut(pair, "=")
			if !ok || key == "" {
				return req, errors.New("--env value must be KEY=VALUE")
			}
			if strings.ContainsRune(key, '\x00') || strings.ContainsRune(value, '\x00') {
				return req, errors.New("--env value contains a NUL byte")
			}
			env[key] = value
		}
		req.Env = env
	}
	return req, nil
}

// reportReady emits the state and ready events for a sandbox that is already
// ready in the create response.
func (a *App) reportReady(sandbox api.Sandbox) error {
	if err := a.out.Event(
		newStateEvent{Event: "state", ID: sandbox.ID, State: sandbox.State},
		"state %s", sandbox.State,
	); err != nil {
		return err
	}
	return a.out.Event(
		newReadyEvent{Event: "ready", ID: sandbox.ID},
		"ready %s", sandbox.ID,
	)
}

// waitSandboxReady polls until the sandbox is ready, fails or the wait ends.
// It emits at most one ready event.
func (a *App) waitSandboxReady(ctx context.Context, client APIClient, first api.Sandbox) error {
	waitCtx, cancel := a.waitContext(ctx)
	defer cancel()
	last := first.State
	for {
		info, err := client.GetSandbox(waitCtx, first.ID)
		if err != nil {
			if waitCtx.Err() != nil {
				return a.waitFailure(waitCtx, "sandbox "+first.ID, waitCtx.Err())
			}
			return err
		}
		sandbox := info.Sandbox
		if sandbox.ID != first.ID {
			return &CLIError{
				Code:    "invalid_response",
				Message: fmt.Sprintf("sandbox poll returned id %q while waiting for %q", sandbox.ID, first.ID),
			}
		}
		if sandbox.State != last {
			if err := a.out.Event(
				newStateEvent{Event: "state", ID: sandbox.ID, State: sandbox.State},
				"state %s", sandbox.State,
			); err != nil {
				return err
			}
			last = sandbox.State
		}
		if sandbox.State == api.StateReady {
			return a.out.Event(
				newReadyEvent{Event: "ready", ID: sandbox.ID},
				"ready %s", sandbox.ID,
			)
		}
		if err := terminalSandboxError(sandbox); err != nil {
			return err
		}
		switch sandbox.State {
		case api.StateInit, api.StateProvisioning, api.StateProvisioned, api.StateCloning, api.StateIdle, api.StateRunning, api.StateArchiving, api.StateArchived:
		default:
			return &CLIError{
				Code:    "invalid_response",
				Message: fmt.Sprintf("sandbox %s has unknown state %q", sandbox.ID, sandbox.State),
			}
		}
		if err := sleepCtx(waitCtx, a.opts.PollInterval); err != nil {
			return a.waitFailure(waitCtx, "sandbox "+first.ID, err)
		}
	}
}

// terminalSandboxError converts a final failure state into a CLI error.
func terminalSandboxError(sandbox api.Sandbox) error {
	switch sandbox.State {
	case api.StateError, api.StateCancelled:
		message := fmt.Sprintf("sandbox %s is in state %s", sandbox.ID, sandbox.State)
		if sandbox.Error != nil && *sandbox.Error != "" {
			message += ": " + *sandbox.Error
		}
		code := "sandbox_error"
		if sandbox.State == api.StateCancelled {
			code = "sandbox_cancelled"
		}
		return &CLIError{Code: code, Message: message}
	default:
		return nil
	}
}
