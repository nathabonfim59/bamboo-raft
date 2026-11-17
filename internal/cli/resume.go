package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/spf13/cobra"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
	"github.com/nathabonfim59/bamboo-raft/internal/machinetype"
)

// resumeProbeTimeoutSeconds bounds the no-op command that proves a resumed
// sandbox can run commands. It stays inside the pinned synchronous window.
const resumeProbeTimeoutSeconds int64 = 30

// The probe retries only these documented transient command refusals when the
// envelope also says details.retryable=true. Any other API error, transport
// failure, malformed answer or non-zero exit is terminal: retrying it could
// hide an auth, validation or sandbox failure.
const (
	codeSandboxStarting     = "sandbox_starting"
	codeSandboxNotReady     = "sandbox_not_ready"
	codeSandboxDirectFailed = "sandbox_direct_failed"
)

// resumeStateEvent reports a sandbox state change while waiting.
type resumeStateEvent struct {
	Event string           `json:"event"`
	ID    string           `json:"id"`
	State api.SandboxState `json:"state"`
}

// resumeReadyEvent is the terminal event, emitted only after the no-op probe
// succeeded.
type resumeReadyEvent struct {
	Event string `json:"event"`
	ID    string `json:"id"`
}

// resumeCommandOptions holds the flags of `bbraft resume`.
type resumeCommandOptions struct {
	noWait         bool
	failFast       bool
	machineType    string
	typeSet        bool
	environment    string
	environmentSet bool
	noEnv          bool
	env            []string
	ttl            int
	ttlSet         bool
	noAutoStop     bool
}

// newResumeCommand builds `bbraft resume ID [flags]`.
//
// Unless --no-wait is set, the command waits for a ready sandbox and then runs
// one no-op command. The public ready state alone is not enough: the cached
// contract also requires a command to succeed before the CLI reports ready.
// State polling and the probe share one bounded wait context, so the whole
// resume can never take longer than Options.WaitTimeout.
func newResumeCommand(a *App) *cobra.Command {
	var opts resumeCommandOptions
	var cmd *cobra.Command
	cmd = &cobra.Command{
		Use:   "resume ID",
		Short: "Resume a stopped sandbox",
		Args:  cobra.ExactArgs(1),
		RunE: a.runE(func(ctx context.Context, args []string) error {
			opts.typeSet = cmd.Flags().Changed("type")
			opts.environmentSet = cmd.Flags().Changed("environment")
			opts.ttlSet = cmd.Flags().Changed("ttl")
			request, err := opts.request()
			if err != nil {
				return &UsageError{Err: err}
			}
			client, err := a.apiClient(ctx)
			if err != nil {
				return err
			}
			return a.resumeSandbox(ctx, client, args[0], opts.noWait, request)
		}),
	}
	cmd.Flags().BoolVar(&opts.noWait, "no-wait", false, "return when the resume is accepted, without waiting for ready")
	cmd.Flags().BoolVar(&opts.failFast, "fail-fast", false, "resume only if a machine is ready now")
	cmd.Flags().StringVar(&opts.machineType, "type", "", "machine size: small, default, large or xlarge")
	cmd.Flags().StringVar(&opts.environment, "environment", "", "sandbox environment name")
	cmd.Flags().BoolVar(&opts.noEnv, "no-env", false, "resume without account secrets")
	cmd.Flags().StringArrayVarP(&opts.env, "env", "e", nil, "per-sandbox environment variable KEY=VALUE (repeatable; replaces the current set)")
	cmd.Flags().IntVar(&opts.ttl, "ttl", 0, "seconds until auto-stop after the resume (1 to 2592000)")
	cmd.Flags().BoolVar(&opts.noAutoStop, "no-auto-stop", false, "turn auto-stop off")
	return cmd
}

// request validates the flags and builds the resume body. Unset optional
// members stay omitted; an explicit --no-auto-stop sends ttlSeconds null.
func (o resumeCommandOptions) request() (api.ResumeSandboxRequest, error) {
	var request api.ResumeSandboxRequest
	if o.ttlSet && o.noAutoStop {
		return request, errors.New("--ttl and --no-auto-stop cannot be used together")
	}
	if o.failFast {
		request.FailFast = new(true)
	}
	if o.typeSet {
		machine, ok := machinetype.Lookup(o.machineType)
		if !ok {
			names := make([]string, 0, len(machinetype.All))
			for _, candidate := range machinetype.All {
				names = append(names, candidate.Name)
			}
			return request, fmt.Errorf("unknown --type %q: use %s", o.machineType, strings.Join(names, ", "))
		}
		request.Type = new(machine.Name)
	}
	if len(o.env) > 0 {
		values := make(map[string]string, len(o.env))
		for _, pair := range o.env {
			key, value, ok := strings.Cut(pair, "=")
			if !ok || key == "" {
				return request, errors.New("--env value must be KEY=VALUE")
			}
			if strings.ContainsRune(key, '\x00') || strings.ContainsRune(value, '\x00') {
				return request, errors.New("--env value contains a NUL byte")
			}
			values[key] = value
		}
		request.Env = values
	}
	if o.environmentSet {
		request.Environment = new(o.environment)
	}
	if o.noEnv {
		request.NoEnv = new(true)
	}
	if o.ttlSet {
		if o.ttl < minTTLSeconds || o.ttl > maxTTLSeconds {
			return request, fmt.Errorf("--ttl must be between %d and %d seconds", minTTLSeconds, maxTTLSeconds)
		}
		request.TTLSeconds = lifecycleTTLJSON(o.ttl)
	}
	if o.noAutoStop {
		request.TTLSeconds = lifecycleTTLDisabled()
	}
	return request, nil
}

// resumeSandbox accepts one resume and, unless noWait is set, waits for ready
// and proves the sandbox can run a command.
func (a *App) resumeSandbox(ctx context.Context, client APIClient, id string, noWait bool, request api.ResumeSandboxRequest) error {
	response, err := client.ResumeSandbox(ctx, id, request)
	if err != nil {
		return err
	}
	if response.ID != id {
		return &CLIError{
			Code:    "invalid_response",
			Message: fmt.Sprintf("resume response returned id %q while resuming %q", response.ID, id),
		}
	}
	if response.Sandbox != nil && response.Sandbox.ID != id {
		return &CLIError{
			Code:    "invalid_response",
			Message: fmt.Sprintf("resume response returned sandbox id %q while resuming %q", response.Sandbox.ID, id),
		}
	}
	if noWait {
		if err := ctx.Err(); err != nil {
			return err
		}
		if a.out.JSON {
			return a.out.JSONLine(response)
		}
		return a.out.Humanf("resuming %s", id)
	}

	waitCtx, cancel := a.waitContext(ctx)
	defer cancel()

	var first *api.Sandbox
	if response.Sandbox != nil {
		sandbox := *response.Sandbox
		first = &sandbox
	}
	sandbox, err := a.waitResumeReady(waitCtx, client, id, first)
	if err != nil {
		if waitCtx.Err() != nil {
			return a.waitFailure(waitCtx, "sandbox "+id, waitCtx.Err())
		}
		return err
	}
	if err := a.probeResumeReady(waitCtx, client, sandbox.ID); err != nil {
		if waitCtx.Err() != nil {
			return a.waitFailure(waitCtx, "sandbox "+id, waitCtx.Err())
		}
		return err
	}
	if err := waitCtx.Err(); err != nil {
		return a.waitFailure(waitCtx, "sandbox "+id, err)
	}
	return a.out.Event(
		resumeReadyEvent{Event: "ready", ID: sandbox.ID},
		"ready %s", sandbox.ID,
	)
}

// waitResumeReady polls until the public state means ready (ready, idle or
// running), fails terminally, or the wait context ends. It emits one state
// event per change and never emits ready.
func (a *App) waitResumeReady(ctx context.Context, client APIClient, id string, first *api.Sandbox) (api.Sandbox, error) {
	sandbox := api.Sandbox{ID: id}
	if first != nil {
		sandbox = *first
	}
	last := api.SandboxState("")
	for {
		if err := ctx.Err(); err != nil {
			return sandbox, err
		}
		if sandbox.ID != id {
			return sandbox, &CLIError{
				Code:    "invalid_response",
				Message: fmt.Sprintf("sandbox poll returned id %q while waiting for %q", sandbox.ID, id),
			}
		}
		if sandbox.State != last && sandbox.State != "" {
			if err := a.out.Event(
				resumeStateEvent{Event: "state", ID: sandbox.ID, State: sandbox.State},
				"state %s", sandbox.State,
			); err != nil {
				return sandbox, err
			}
			last = sandbox.State
		}
		switch sandbox.State {
		case api.StateReady, api.StateIdle, api.StateRunning:
			return sandbox, nil
		case api.StateError, api.StateCancelled:
			return sandbox, terminalSandboxError(sandbox)
		case api.StateArchived:
			// An accepted resume leaves the archived state synchronously.
			// Seeing it again means the resume did not start; waiting for the
			// full timeout would hide that failure.
			return sandbox, &CLIError{
				Code:    codeSandboxNotReady,
				Message: fmt.Sprintf("sandbox %s is archived after the resume; it did not start", id),
			}
		case "", api.StateInit, api.StateProvisioning, api.StateProvisioned, api.StateCloning, api.StateArchiving:
		default:
			return sandbox, &CLIError{
				Code:    "invalid_response",
				Message: fmt.Sprintf("sandbox %s has unknown state %q", sandbox.ID, sandbox.State),
			}
		}
		if err := sleepCtx(ctx, a.opts.PollInterval); err != nil {
			return sandbox, err
		}
		info, err := client.GetSandbox(ctx, id)
		if err != nil {
			return sandbox, err
		}
		sandbox = info.Sandbox
	}
}

// probeResumeReady runs the deliberately idempotent no-op command. It retries
// only documented transient refusals with details.retryable=true; a non-zero
// exit, malformed answer or any other API error ends the resume without a
// ready event. The resume POST is never repeated here.
func (a *App) probeResumeReady(ctx context.Context, client APIClient, id string) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		_, err := ExecuteCommand(ctx, client, ExecutionRequest{
			SandboxID:      id,
			Command:        "true",
			TimeoutSeconds: resumeProbeTimeoutSeconds,
		}, ExecutionOutput{Stdout: io.Discard, Stderr: io.Discard})
		if err == nil {
			// The read may complete successfully just as cancellation lands.
			// A cancelled command never proved readiness.
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if exit, ok := errors.AsType[*ExitError](err); ok {
			if exit.Code == 124 {
				return &CLIError{Code: "timeout", Message: fmt.Sprintf("sandbox %s readiness check timed out", id)}
			}
			return &CLIError{
				Code:    "sandbox_probe_failed",
				Message: fmt.Sprintf("sandbox %s failed its readiness check with exit status %d", id, exit.Code),
			}
		}
		if !resumeProbeRetryable(err) {
			return err
		}
		if err := sleepCtx(ctx, a.opts.PollInterval); err != nil {
			return err
		}
	}
}

// resumeProbeRetryable reports the exact transient command refusals worth
// retrying. The envelope must both belong to the narrow documented set and
// explicitly say details.retryable=true; an absent, malformed or false hint is
// terminal, as is every other error.
func resumeProbeRetryable(err error) bool {
	apiErr, ok := errors.AsType[*APIError](err)
	if !ok || apiErr.Retryable == nil || !*apiErr.Retryable {
		return false
	}
	switch apiErr.Status {
	case http.StatusConflict:
		return apiErr.Code == codeSandboxStarting || apiErr.Code == codeSandboxNotReady
	case http.StatusBadGateway:
		return apiErr.Code == codeSandboxDirectFailed
	default:
		return false
	}
}
