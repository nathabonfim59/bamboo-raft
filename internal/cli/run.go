package cli

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cobra"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
	"github.com/nathabonfim59/bamboo-raft/internal/machinetype"
)

// Run command bounds. A server that advertises detached commands gets a
// rolling 900-second auto-stop TTL renewed while the sandbox is in use; an old
// server keeps the fixed TTL derived from the command timeout, which has to
// cover the setup phase, the command itself and a cleanup grace.
const (
	runMinTTLSeconds           int64 = 900
	runRollingTTLSeconds       int64 = 900
	runKeepTTLSeconds          int64 = 900
	runTTLCommandGraceSeconds  int64 = 3720
	runSetupTimeout                  = time.Hour
	runCleanupTimeout                = 2 * time.Minute
	runHeartbeatInterval             = 5 * time.Minute
	runHeartbeatRequestTimeout       = 30 * time.Second
	runHeartbeatMaxFailures          = 2
	runKeepPatchAttempts             = 2
	runDefaultCwd                    = "/home/user"
	runExitCancelled                 = 130
)

// errRunSetupTimeout and errRunCleanupTimeout let the command tell its own
// deadlines apart from a caller cancellation. errRunLeaseLost is the cause of a
// work cancellation triggered by a failed TTL renewal.
var (
	errRunSetupTimeout   = errors.New("run: setup timed out")
	errRunCleanupTimeout = errors.New("run: cleanup timed out")
	errRunLeaseLost      = errors.New("run: sandbox ttl renewal failed")
)

// runRuntime carries the per-command test seams. Production builds it with
// defaultRunRuntime; tests inject it through newRunCommandWithRuntime so no
// mutable package state is shared.
type runRuntime struct {
	heartbeatTicker func(time.Duration) (ticks <-chan time.Time, stop func())
	leaseObserver   func(*runLease)
}

// defaultRunRuntime is the production runtime.
func defaultRunRuntime() runRuntime {
	return runRuntime{
		heartbeatTicker: func(interval time.Duration) (<-chan time.Time, func()) {
			ticker := time.NewTicker(interval)
			return ticker.C, ticker.Stop
		},
	}
}

// runMode selects the auto-stop strategy before a sandbox is created.
type runMode int

const (
	runModeLegacy runMode = iota
	runModeRolling
)

// runDisposition is how the sandbox was left when the command finished.
type runDisposition int

const (
	runNotDeleted runDisposition = iota
	runDeleted
	runKept
)

// runCommandOptions holds the flags of `bbraft run`.
type runCommandOptions struct {
	from        string
	machineType string
	push        string
	cwd         string
	timeout     int64
	env         []string

	// Unsupported reference flags. They are accepted by the parser so the
	// command can reject them before any request, with a clear message.
	keep        bool
	personal    bool
	environment string
	noEnv       bool
}

// runSummaryEvent is the single terminal JSON Lines object of `run --json`.
type runSummaryEvent struct {
	Event          string  `json:"event"`
	ExitCode       int     `json:"exitCode"`
	ElapsedSeconds int64   `json:"elapsedSeconds"`
	Dollars        float64 `json:"dollars"`
	SandboxType    string  `json:"sandboxType"`
	Deleted        bool    `json:"deleted"`
}

// runErrorEvent is the safe JSON error line of `run --json`.
type runErrorEvent struct {
	Event             string `json:"event"`
	Error             string `json:"error"`
	Code              string `json:"code,omitempty"`
	Status            int    `json:"status,omitempty"`
	RetryAfterSeconds int    `json:"retryAfterSeconds,omitzero"`
}

// newRunCommand builds `bbraft run [flags] [command...]`.
func newRunCommand(a *App) *cobra.Command {
	return newRunCommandWithRuntime(a, defaultRunRuntime())
}

// newRunCommandWithRuntime builds the run command with private test seams. It
// replaces only this command, so the rest of the tree stays production-wired.
func newRunCommandWithRuntime(a *App, runtime runRuntime) *cobra.Command {
	var opts runCommandOptions
	var cmd *cobra.Command
	cmd = &cobra.Command{
		Use:   "run [flags] [command...]",
		Short: "Run one command in a new sandbox, then delete the sandbox",
		Long: "Create a sandbox, copy a directory into it, run one command and delete\n" +
			"the sandbox. Command output passes through unchanged; the last line on\n" +
			"stderr reports the exit code, elapsed time, sandbox type and sandbox id.\n\n" +
			"One command argument is a shell command line and is sent unchanged.\n" +
			"Several arguments are argv: each is POSIX-shell quoted and joined.\n\n" +
			"On a server with detached commands the sandbox auto-stops 15 minutes\n" +
			"ahead and the timer is renewed while it is in use. --keep leaves the\n" +
			"sandbox running after the command ends; it stops 15 minutes later.\n" +
			"Ctrl-C always deletes the sandbox and exits 130.",
		RunE: a.runE(func(ctx context.Context, args []string) error {
			return a.runCommand(ctx, cmd, opts, args, runtime)
		}),
	}
	cmd.Flags().StringVar(&opts.from, "from", "", "start from a saved snapshot name or share code")
	cmd.Flags().StringVar(&opts.machineType, "type", "", "machine size: small, default, large or xlarge")
	cmd.Flags().StringVar(&opts.push, "push", "", "copy this local directory to /home/user/<dir name> first")
	cmd.Flags().StringVar(&opts.cwd, "cwd", "", "working directory (default the pushed directory, else /home/user)")
	cmd.Flags().Int64Var(&opts.timeout, "timeout", 0, "stop the command after this many seconds and exit 124 (0 is unlimited on a capable server, legacy streamed limit otherwise)")
	cmd.Flags().StringArrayVarP(&opts.env, "env", "e", nil, "environment variable KEY=VALUE (repeatable)")
	cmd.Flags().BoolVar(&opts.keep, "keep", false, "keep the sandbox after the command ends (it stops 15 minutes later)")
	cmd.Flags().BoolVar(&opts.personal, "personal", false, "unsupported in bbraft")
	cmd.Flags().StringVar(&opts.environment, "environment", "", "unsupported in bbraft")
	cmd.Flags().BoolVar(&opts.noEnv, "no-env", false, "unsupported in bbraft")
	for _, name := range []string{"personal", "environment", "no-env"} {
		if err := cmd.Flags().MarkHidden(name); err != nil {
			// The flags were just registered; a failure means the command
			// tree is malformed, which is a programmer error.
			panic(err)
		}
	}
	return cmd
}

// runCommand implements the command body. Every local check runs before the
// first request; a usage failure never creates a sandbox.
func (a *App) runCommand(ctx context.Context, cmd *cobra.Command, opts runCommandOptions, args []string, runtime runRuntime) error {
	if err := opts.validateUnsupported(cmd); err != nil {
		return &UsageError{Err: err}
	}
	commandLine, err := opts.commandLine(args)
	if err != nil {
		return &UsageError{Err: err}
	}
	base := newCommandOptions{machineType: opts.machineType, from: opts.from, env: opts.env}
	if _, err := base.request(); err != nil {
		return &UsageError{Err: err}
	}
	// The legacy TTL arithmetic is also the pinned 30-day guard for an
	// explicit timeout. Run it before any request so an impossible timeout
	// still creates nothing.
	if opts.timeout > 0 {
		if _, err := runTTLSeconds(opts.timeout); err != nil {
			return &UsageError{Err: err}
		}
	}
	pushSource := ""
	if opts.push != "" {
		source, _, err := pushResolveSource(opts.push)
		if err != nil {
			// Ordinary enumeration and upload failures happen after create and
			// are handled by PushDirectory's own cleanup; only the source
			// location is a local usage check.
			return &UsageError{Err: err}
		}
		pushSource = source
	}
	if ctx.Err() != nil {
		return &ExitError{Code: runExitCancelled}
	}
	client, err := a.apiClient(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return a.runReportFailure(runExitCancelled, nil)
		}
		return a.runReportFailure(1, err)
	}

	// One metadata read decides the auto-stop strategy before create. Only an
	// absent extension or detached=false is old-server fallback; malformed,
	// unauthenticated or unreachable metadata fails before a sandbox exists.
	limits, limitsErr := execDiscoverLimits(ctx, client)
	legacy := errors.Is(limitsErr, errExecLegacyServer)
	if limitsErr != nil && !legacy {
		if ctx.Err() != nil {
			return a.runReportFailure(runExitCancelled, nil)
		}
		return a.runReportFailure(1, limitsErr)
	}
	mode := runModeLegacy
	if !legacy && limits.Detached {
		mode = runModeRolling
	}
	if opts.keep && mode == runModeLegacy {
		if ctx.Err() != nil {
			return a.runReportFailure(runExitCancelled, nil)
		}
		return a.runReportFailure(1, &CLIError{
			Code:    "keep_unsupported",
			Message: "--keep needs a server that supports sandbox TTL renewal",
		})
	}
	effectiveTimeout := opts.timeout
	if effectiveTimeout == 0 && mode == runModeLegacy {
		// Only an old server needs a concrete stream timeout. Rolling timeout
		// 0 stays unlimited and is passed through as the preconfirmed detached
		// zero seam.
		if legacy {
			effectiveTimeout, err = execFallbackStreamCap(a.opts.Stderr)
			if err != nil {
				if ctx.Err() != nil {
					return a.runReportFailure(runExitCancelled, nil)
				}
				return a.runReportFailure(1, err)
			}
		} else {
			effectiveTimeout = limits.StreamMaxSeconds
		}
	}
	ttl := int(runRollingTTLSeconds)
	if mode == runModeLegacy {
		ttl, err = runTTLSeconds(effectiveTimeout)
		if err != nil {
			return &UsageError{Err: err}
		}
	}
	requestOptions := base
	requestOptions.ttl = ttl
	requestOptions.ttlSet = true
	request, err := requestOptions.request()
	if err != nil {
		return &UsageError{Err: err}
	}

	workCtx, cancelWork := context.WithCancelCause(ctx)
	defer cancelWork(nil)
	setupCtx, cancelSetup := context.WithTimeoutCause(workCtx, runSetupTimeout, errRunSetupTimeout)
	defer cancelSetup()

	created, err := client.CreateSandbox(setupCtx, request)
	if err != nil {
		createErr := createFailure(err, &ttl)
		if ctx.Err() != nil {
			// The caller cancelled, but the create may still have happened.
			// Keep the Ctrl-C status and explain the unconfirmed resource.
			a.runReportError(createErr)
			return &ExitError{Code: runExitCancelled}
		}
		return a.runReportFailure(1, createErr)
	}
	id := created.Sandbox.ID
	if id == "" {
		return a.runReportFailure(1, ambiguousCreate(&ttl))
	}
	sandboxType := created.Sandbox.Type
	if err := runValidateSandboxType(created.Sandbox, ""); err != nil {
		// A sandbox exists now, so it must still be deleted. The summary is
		// deliberately not printed because it would have to invent the type.
		return a.runCleanupAndFail(ctx, client, id, err)
	}

	var lease *runLease
	if mode == runModeRolling {
		lease, err = startRunLease(workCtx, cancelWork, client, id, int(runRollingTTLSeconds), runtime)
		if err != nil {
			return a.runPostIDFailure(ctx, workCtx, setupCtx, client, id, sandboxType, 0, err)
		}
		defer lease.stop()
	}

	if _, err := a.runWaitReady(setupCtx, client, created.Sandbox, sandboxType); err != nil {
		return a.runPostIDFailure(ctx, workCtx, setupCtx, client, id, sandboxType, 0, err)
	}

	cwd := runDefaultCwd
	if pushSource != "" {
		pushed, err := PushDirectory(setupCtx, client, NewUploadClient(a.opts.HTTPClient), PushRequest{
			SandboxID: id,
			Source:    pushSource,
		}, PushOutput{})
		if err != nil {
			return a.runPostIDFailure(ctx, workCtx, setupCtx, client, id, sandboxType, 0, err)
		}
		cwd = pushed.Directory
	}
	if opts.cwd != "" {
		cwd = opts.cwd
	}

	out := ExecutionOutput{Stdout: a.out.Stdout, Stderr: a.opts.Stderr}
	if a.out.ExplicitJSON {
		out = ExecutionOutput{JSON: a.out.Stdout}
	}
	started := time.Now()
	result, execErr := ExecuteCommand(workCtx, client, ExecutionRequest{
		SandboxID:      id,
		Command:        commandLine,
		Cwd:            cwd,
		TimeoutSeconds: effectiveTimeout,
		// The reference promises live output, so run always streams, even
		// below the synchronous cap or with an explicit positive timeout.
		// A rolling server may transparently use detached start plus attach.
		Stream:          true,
		DetachedAllowed: mode == runModeRolling,
	}, out)
	elapsed := time.Since(started)
	if lease != nil {
		lease.stop()
	}
	code, primary := runExecutionOutcome(ctx, workCtx, execErr)
	keep := opts.keep && mode == runModeRolling && result.Completed &&
		ctx.Err() == nil && !errors.Is(context.Cause(workCtx), errRunLeaseLost)
	return a.runFinish(ctx, client, id, sandboxType, elapsed, code, primary, keep)
}

// validateUnsupported rejects reference flags this phase does not implement.
// Presence is enough: the parser accepted the flag, but no request may follow.
func (o runCommandOptions) validateUnsupported(cmd *cobra.Command) error {
	for _, name := range []string{"personal", "environment", "no-env"} {
		if cmd.Flags().Changed(name) {
			return fmt.Errorf("--%s is not supported by bbraft run", name)
		}
	}
	return nil
}

// commandLine validates the command words and builds the remote shell line.
func (o runCommandOptions) commandLine(args []string) (string, error) {
	if o.timeout < 0 || o.timeout > execMaxTimeoutSeconds {
		return "", fmt.Errorf("--timeout must be between 0 and %d seconds", execMaxTimeoutSeconds)
	}
	for _, arg := range args {
		if strings.ContainsRune(arg, '\x00') {
			return "", errors.New("command argument contains a NUL byte")
		}
	}
	if strings.ContainsRune(o.cwd, '\x00') {
		return "", errors.New("--cwd contains a NUL byte")
	}
	line := execCommandLine(args)
	if strings.TrimSpace(line) == "" {
		return "", errors.New("run needs a non-empty command")
	}
	return line, nil
}

// runTTLSeconds derives the fixed Phase 0.5 TTL. The arithmetic is int64 so a
// hostile --timeout cannot wrap around; anything past the pinned 30-day limit
// is rejected before create.
func runTTLSeconds(effectiveTimeout int64) (int, error) {
	if effectiveTimeout < 0 || effectiveTimeout > execMaxTimeoutSeconds {
		return 0, fmt.Errorf("--timeout must be between 0 and %d seconds", execMaxTimeoutSeconds)
	}
	ttl := effectiveTimeout + runTTLCommandGraceSeconds
	if ttl < runMinTTLSeconds {
		ttl = runMinTTLSeconds
	}
	if ttl > int64(maxTTLSeconds) {
		return 0, fmt.Errorf("--timeout %d is too large: the sandbox ttlSeconds would exceed the %d-second maximum", effectiveTimeout, maxTTLSeconds)
	}
	return int(ttl), nil
}

// createFailure classifies a create failure. A 4xx answer or a validated API
// error envelope is a definite refusal, even when the status is a 5xx.
// Everything else may have created a sandbox whose response was lost, so no
// id is invented and no completion is claimed. ttl is the explicitly requested
// lifetime, nil when the command did not ask for one.
func createFailure(err error, ttl *int) error {
	if apiErr, ok := errors.AsType[*APIError](err); ok &&
		(apiErr.Envelope || (apiErr.Status >= 400 && apiErr.Status < 500)) {
		return err
	}
	return ambiguousCreate(ttl)
}

// ambiguousCreate explains an unconfirmed create without guessing an id or
// claiming an expiry the command did not request.
func ambiguousCreate(ttl *int) error {
	message := "sandbox creation could not be confirmed; check bbraft list before retrying"
	if ttl != nil {
		message = fmt.Sprintf(
			"sandbox creation could not be confirmed; if one was created it auto-stops after %d seconds; check bbraft list before retrying",
			*ttl,
		)
	}
	return &CLIError{Code: "create_ambiguous", Message: message}
}

// runValidateSandboxType requires a supported type from the server response.
// When want is non-empty the type must also stay unchanged; no requested or
// default type is ever substituted.
func runValidateSandboxType(sandbox api.Sandbox, want string) error {
	if sandbox.Type == "" {
		return &CLIError{Code: "invalid_response", Message: "sandbox response has no sandbox type"}
	}
	if _, ok := machinetype.Lookup(sandbox.Type); !ok {
		return &CLIError{
			Code:    "invalid_response",
			Message: fmt.Sprintf("sandbox response has unsupported type %q", sandbox.Type),
		}
	}
	if want != "" && sandbox.Type != want {
		return &CLIError{
			Code:    "invalid_response",
			Message: fmt.Sprintf("sandbox type changed from %q to %q while waiting for ready", want, sandbox.Type),
		}
	}
	return nil
}

// runWaitReady polls without emitting the `new` command's created/state/ready
// events, so raw run output stays clean. It validates identity and the actual
// sandbox type at every poll.
func (a *App) runWaitReady(ctx context.Context, client APIClient, first api.Sandbox, wantType string) (api.Sandbox, error) {
	sandbox := first
	for {
		if sandbox.ID != first.ID {
			return sandbox, &CLIError{
				Code:    "invalid_response",
				Message: fmt.Sprintf("sandbox poll returned id %q while waiting for %q", sandbox.ID, first.ID),
			}
		}
		if err := runValidateSandboxType(sandbox, wantType); err != nil {
			return sandbox, err
		}
		if sandbox.State == api.StateReady {
			return sandbox, nil
		}
		if err := terminalSandboxError(sandbox); err != nil {
			return sandbox, err
		}
		switch sandbox.State {
		case api.StateInit, api.StateProvisioning, api.StateProvisioned, api.StateCloning, api.StateIdle, api.StateRunning, api.StateArchiving, api.StateArchived:
		default:
			return sandbox, &CLIError{
				Code:    "invalid_response",
				Message: fmt.Sprintf("sandbox %s has unknown state %q", sandbox.ID, sandbox.State),
			}
		}
		if err := sleepCtx(ctx, a.opts.PollInterval); err != nil {
			return sandbox, err
		}
		info, err := client.GetSandbox(ctx, first.ID)
		if err != nil {
			return sandbox, err
		}
		sandbox = info.Sandbox
	}
}

// runPostIDFailure maps a setup failure once the sandbox id is known.
func (a *App) runPostIDFailure(ctx, workCtx, setupCtx context.Context, client APIClient, id, sandboxType string, elapsed time.Duration, err error) error {
	code, primary := runSetupFailureOutcome(ctx, workCtx, setupCtx, err)
	return a.runFinish(ctx, client, id, sandboxType, elapsed, code, primary, false)
}

// runSetupFailureOutcome separates caller cancellation, a lost TTL lease and a
// setup deadline from ordinary setup failures. setupCtx keeps its own cause so
// a readiness or push timeout stays a timeout even though the work context is
// used for renewal cancellation.
func runSetupFailureOutcome(ctx, workCtx, setupCtx context.Context, err error) (int, error) {
	switch {
	case ctx.Err() != nil:
		return runExitCancelled, nil
	case errors.Is(context.Cause(workCtx), errRunLeaseLost):
		return 1, runLeaseLostError()
	case errors.Is(context.Cause(setupCtx), errRunSetupTimeout) || errors.Is(err, context.DeadlineExceeded):
		return 1, &CLIError{Code: "timeout", Message: "sandbox setup timed out"}
	}
	return 1, err
}

// runExecutionOutcome maps the command result to a process status. A failed
// TTL renewal wins over an ambiguous command result; otherwise a remote
// nonzero status and the timeout status win over local failures, and a caller
// cancellation becomes 130.
func runExecutionOutcome(ctx, workCtx context.Context, err error) (int, error) {
	if ctx.Err() == nil && workCtx != nil && errors.Is(context.Cause(workCtx), errRunLeaseLost) {
		return 1, runLeaseLostError()
	}
	if err == nil {
		if ctx.Err() != nil {
			return runExitCancelled, nil
		}
		return 0, nil
	}
	if exit, ok := errors.AsType[*ExitError](err); ok {
		code := exit.Code
		if code == 0 {
			code = 1
		}
		if ctx.Err() != nil && errors.Is(exit.Err, context.Canceled) {
			return runExitCancelled, nil
		}
		return code, exit.Err
	}
	if ctx.Err() != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
		return runExitCancelled, nil
	}
	return 1, err
}

// runLeaseLostError is the safe diagnostic of a failed rolling-TTL renewal.
func runLeaseLostError() error {
	return &CLIError{
		Code:    "ttl_renewal_failed",
		Message: "sandbox auto-stop could not be renewed; cleanup was attempted",
	}
}

// runFinish applies the keep decision or deletes the sandbox, reports every
// failure before the single terminal summary, and returns the process status
// without an error, because the failure was already rendered.
func (a *App) runFinish(ctx context.Context, client APIClient, id, sandboxType string, elapsed time.Duration, code int, primary error, keep bool) error {
	deleted := false
	kept := false
	var cleanupErr error

	if keep {
		keepCtx, cancel := context.WithTimeoutCause(context.WithoutCancel(ctx), runCleanupTimeout, errRunCleanupTimeout)
		keepErr := a.runKeepSandbox(keepCtx, client, id)
		cancel()
		if ctx.Err() != nil {
			// Ctrl-C during the keep decision, including while the final
			// PATCH was in flight, always deletes and exits 130.
			cleanupCtx, cancelCleanup := context.WithTimeoutCause(context.WithoutCancel(ctx), runCleanupTimeout, errRunCleanupTimeout)
			deleted, cleanupErr = a.runDeleteSandbox(cleanupCtx, client, id)
			cancelCleanup()
			kept = false
			code = runExitCancelled
			primary = nil
		} else if keepErr != nil {
			// The sandbox was kept as requested, but renewal is unconfirmed;
			// never claim the full 900 seconds remain.
			cleanupErr = keepErr
		} else {
			kept = true
		}
	} else {
		cleanupCtx, cancel := context.WithTimeoutCause(context.WithoutCancel(ctx), runCleanupTimeout, errRunCleanupTimeout)
		deleted, cleanupErr = a.runDeleteSandbox(cleanupCtx, client, id)
		cancel()

		// A cancellation that arrives while cleanup is finishing still wins
		// over a clean remote status, but never over an earlier remote nonzero
		// or 124.
		if ctx.Err() != nil && code == 0 {
			code = runExitCancelled
		}
	}

	writeFailed := false
	if primary != nil {
		writeFailed = a.runReportError(primary) || writeFailed
	}
	if cleanupErr != nil {
		writeFailed = a.runReportError(cleanupErr) || writeFailed
	}
	if code == 0 && (primary != nil || cleanupErr != nil || writeFailed) {
		code = 1
	}
	disposition := runNotDeleted
	switch {
	case deleted:
		disposition = runDeleted
	case kept:
		disposition = runKept
	}
	if a.runWriteSummary(code, elapsed, sandboxType, id, disposition) {
		writeFailed = true
	}
	if code == 0 && writeFailed {
		code = 1
	}
	if code == 0 {
		return nil
	}
	return &ExitError{Code: code}
}

// runCleanupAndFail handles the rare state where create returned an id but no
// truthful sandbox type. Cleanup still runs; no summary is printed.
func (a *App) runCleanupAndFail(ctx context.Context, client APIClient, id string, primary error) error {
	cleanupCtx, cancel := context.WithTimeoutCause(context.WithoutCancel(ctx), runCleanupTimeout, errRunCleanupTimeout)
	_, cleanupErr := a.runDeleteSandbox(cleanupCtx, client, id)
	cancel()
	a.runReportError(primary)
	if cleanupErr != nil {
		a.runReportError(cleanupErr)
	}
	return &ExitError{Code: 1}
}

// runReportFailure renders a pre-summary failure in the run command's own
// output mode. No summary follows because no truthful sandbox outcome exists.
func (a *App) runReportFailure(code int, err error) error {
	a.runReportError(err)
	if code == 0 {
		code = 1
	}
	return &ExitError{Code: code}
}

// runDeleteSandbox asks for deletion with the pinned confirmation header and
// polls the owner-scoped operation until it completes. Only a completed
// operation for this exact sandbox counts as deleted.
func (a *App) runDeleteSandbox(ctx context.Context, client APIClient, id string) (bool, error) {
	response, err := client.DeleteSandbox(ctx, id)
	if err != nil {
		return false, runCleanupError(ctx, err)
	}
	final, err := a.waitDeletion(ctx, client, response.Operation, id)
	if err != nil {
		return false, runCleanupError(ctx, err)
	}
	operation := final.Operation
	if operation.Status != api.DeletionCompleted ||
		operation.Kind != api.DeletionKindSandbox ||
		operation.TargetID != id {
		return false, &CLIError{
			Code:    "invalid_response",
			Message: "deletion operation did not confirm the sandbox deletion",
		}
	}
	return true, nil
}

// runCleanupError classifies a cleanup failure without echoing request or
// transport text.
func runCleanupError(ctx context.Context, err error) error {
	if errors.Is(context.Cause(ctx), errRunCleanupTimeout) || errors.Is(err, context.DeadlineExceeded) {
		return &CLIError{Code: "timeout", Message: "sandbox cleanup timed out"}
	}
	return err
}

// runReportError renders one failure exactly once, using raw stdout mode's
// stderr rather than the automatic non-TTY JSON mode.
func (a *App) runReportError(err error) bool {
	if err == nil {
		return false
	}
	message := err.Error()
	if a.out.ExplicitJSON {
		event := runErrorEvent{Event: "error", Error: message}
		if apiErr, ok := errors.AsType[*APIError](err); ok {
			event.Error = apiErr.Message
			event.Code = apiErr.Code
			event.Status = apiErr.Status
			event.RetryAfterSeconds = apiErr.RetryAfterSeconds
		} else if cliErr, ok := errors.AsType[*CLIError](err); ok {
			event.Error = cliErr.Message
			event.Code = cliErr.Code
		}
		raw, marshalErr := json.Marshal(event)
		if marshalErr != nil {
			return true
		}
		raw = append(raw, '\n')
		n, writeErr := a.out.Stdout.Write(raw)
		return writeErr != nil || n != len(raw)
	}
	line := fmt.Sprintf("bbraft: %s\n", message)
	n, writeErr := io.WriteString(a.opts.Stderr, line)
	return writeErr != nil || n != len(line)
}

// runWriteSummary writes the one terminal reference line: JSON on stdout in
// explicit JSON mode, otherwise the human line on stderr.
func (a *App) runWriteSummary(code int, elapsed time.Duration, sandboxType, id string, disposition runDisposition) (failed bool) {
	seconds := max(int64(elapsed/time.Second), 0)
	deleted := disposition == runDeleted
	if a.out.ExplicitJSON {
		raw, err := json.Marshal(runSummaryEvent{
			Event:          "summary",
			ExitCode:       code,
			ElapsedSeconds: seconds,
			Dollars:        0,
			SandboxType:    sandboxType,
			Deleted:        deleted,
		})
		if err != nil {
			return true
		}
		raw = append(raw, '\n')
		n, writeErr := a.out.Stdout.Write(raw)
		return writeErr != nil || n != len(raw)
	}
	status := id + " not deleted"
	switch disposition {
	case runDeleted:
		status = id + " deleted"
	case runKept:
		status = id + " kept"
	}
	line := fmt.Sprintf("exit %d · %dm %ds · %s · $0 · %s\n", code, seconds/60, seconds%60, sandboxType, status)
	n, writeErr := io.WriteString(a.opts.Stderr, line)
	return writeErr != nil || n != len(line)
}

// runLease renews one sandbox's rolling auto-stop TTL while setup, push and
// the command run. stop is separate from the caller's work context: stopping a
// healthy lease never cancels the work or reports a lost lease.
type runLease struct {
	client    APIClient
	sandboxID string
	ttl       int

	ticker      func(time.Duration) (<-chan time.Time, func())
	ticks       <-chan time.Time
	stopTicks   func()
	cancelWork  context.CancelCauseFunc
	stopCh      chan struct{}
	leaseCtx    context.Context
	leaseCancel context.CancelFunc
	done        chan struct{}

	mu       sync.Mutex
	stopped  bool
	failures int
	err      error
}

// startRunLease verifies the rolling TTL once and starts the renewal
// goroutine. A permanent initial rejection is returned before readiness; a
// transient one is counted and retried on the first tick.
func startRunLease(ctx context.Context, cancelWork context.CancelCauseFunc, client APIClient, id string, ttl int, runtime runRuntime) (*runLease, error) {
	lease := &runLease{
		client:     client,
		sandboxID:  id,
		ttl:        ttl,
		ticker:     runtime.heartbeatTicker,
		cancelWork: cancelWork,
		stopCh:     make(chan struct{}),
		done:       make(chan struct{}),
	}
	patchCtx, cancel := context.WithTimeout(ctx, runHeartbeatRequestTimeout)
	_, err := runPatchSandboxTTL(patchCtx, client, id, ttl)
	cancel()
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if runPatchPermanent(err) {
			return nil, &CLIError{
				Code:    "ttl_renewal_failed",
				Message: "sandbox auto-stop renewal is unavailable on this server; cleanup was attempted",
			}
		}
		lease.failures = 1
		lease.err = err
	}
	lease.leaseCtx, lease.leaseCancel = context.WithCancel(ctx)
	lease.ticks, lease.stopTicks = lease.ticker(runHeartbeatInterval)
	if runtime.leaseObserver != nil {
		runtime.leaseObserver(lease)
	}
	go lease.run(lease.leaseCtx)
	return lease, nil
}

// run renews on every tick until it is stopped, the work is cancelled, or a
// permanent or second consecutive retryable failure aborts the work.
func (l *runLease) run(ctx context.Context) {
	defer close(l.done)
	for {
		select {
		case <-ctx.Done():
			return
		case <-l.stopCh:
			return
		case <-l.ticks:
		}
		err := l.renew(ctx)
		if l.isStopped() || ctx.Err() != nil {
			return
		}
		if err == nil {
			l.mu.Lock()
			l.failures = 0
			l.mu.Unlock()
			continue
		}
		permanent := runPatchPermanent(err)
		l.mu.Lock()
		l.failures++
		failures := l.failures
		l.err = err
		l.mu.Unlock()
		if permanent || failures >= runHeartbeatMaxFailures {
			l.cancelWork(errRunLeaseLost)
			return
		}
	}
}

// renew performs one bounded PATCH.
func (l *runLease) renew(ctx context.Context) error {
	patchCtx, cancel := context.WithTimeout(ctx, runHeartbeatRequestTimeout)
	defer cancel()
	_, err := runPatchSandboxTTL(patchCtx, l.client, l.sandboxID, l.ttl)
	return err
}

// isStopped reports whether stop began.
func (l *runLease) isStopped() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.stopped
}

// stop cancels the ticker and any in-flight renewal request, then waits for
// the goroutine. It is idempotent.
func (l *runLease) stop() {
	l.mu.Lock()
	if l.stopped {
		l.mu.Unlock()
		return
	}
	l.stopped = true
	l.mu.Unlock()
	close(l.stopCh)
	if l.stopTicks != nil {
		l.stopTicks()
	}
	if l.leaseCancel != nil {
		l.leaseCancel()
	}
	<-l.done
}

// runPatchSandboxTTL sets the exact ttl and validates that the answer confirms
// this sandbox, this ttl, a finite auto-stop deadline and a live/starting
// state. The server's clock is authoritative; no local-time comparison is made.
func runPatchSandboxTTL(ctx context.Context, client APIClient, id string, ttl int) (api.SandboxInfoResponse, error) {
	response, err := client.UpdateSandbox(ctx, id, api.UpdateSandboxRequest{
		TTLSeconds: lifecycleTTLJSON(ttl),
	})
	if err != nil {
		return response, err
	}
	switch {
	case response.Sandbox.ID != id:
		return response, &CLIError{
			Code:    "invalid_response",
			Message: "sandbox renewal returned a different sandbox id",
		}
	case response.Sandbox.TTLSeconds == nil || *response.Sandbox.TTLSeconds != ttl:
		return response, &CLIError{
			Code:    "invalid_response",
			Message: "sandbox renewal did not confirm the requested ttl",
		}
	case response.Sandbox.ArchiveAfter == nil:
		return response, &CLIError{
			Code:    "invalid_response",
			Message: "sandbox renewal returned no auto-stop deadline",
		}
	case !runSandboxLive(response.Sandbox.State):
		return response, &CLIError{
			Code:    "invalid_response",
			Message: fmt.Sprintf("sandbox renewal returned non-live state %q", response.Sandbox.State),
		}
	}
	return response, nil
}

// runSandboxLive reports the states in which a renewal may be accepted.
func runSandboxLive(state api.SandboxState) bool {
	switch state {
	case api.StateInit, api.StateProvisioning, api.StateProvisioned,
		api.StateCloning, api.StateReady, api.StateIdle, api.StateRunning:
		return true
	default:
		return false
	}
}

// runPatchPermanent classifies a failed renewal. Any 4xx, an explicit
// retryable=false, an invalid response and any non-listed status are permanent.
// Transport, timeout, 500, 502, 503 and 504 are retryable.
func runPatchPermanent(err error) bool {
	if err == nil {
		return false
	}
	if apiErr, ok := errors.AsType[*APIError](err); ok {
		if apiErr.Retryable != nil && !*apiErr.Retryable {
			return true
		}
		switch apiErr.Status {
		case http.StatusInternalServerError, http.StatusBadGateway,
			http.StatusServiceUnavailable, http.StatusGatewayTimeout:
			return false
		}
		return true
	}
	if _, ok := errors.AsType[*requestError](err); ok {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return false
	}
	return true
}

// runKeepSandbox makes the post-command keep intention explicit with a bounded
// idempotent PATCH. A permanent rejection is not retried; transport/5xx get
// one more attempt. Failure means renewal is unconfirmed, never deleted.
func (a *App) runKeepSandbox(ctx context.Context, client APIClient, id string) error {
	for attempt := 0; attempt < runKeepPatchAttempts; attempt++ {
		if ctx.Err() != nil {
			return runKeepUnconfirmedError()
		}
		patchCtx, cancel := context.WithTimeout(ctx, runHeartbeatRequestTimeout)
		_, err := runPatchSandboxTTL(patchCtx, client, id, int(runKeepTTLSeconds))
		cancel()
		if err == nil {
			return nil
		}
		if ctx.Err() != nil || runPatchPermanent(err) {
			break
		}
	}
	return runKeepUnconfirmedError()
}

// runKeepUnconfirmedError never promises the full remaining TTL.
func runKeepUnconfirmedError() error {
	return &CLIError{
		Code:    "keep_unconfirmed",
		Message: "sandbox kept as requested, but the auto-stop renewal could not be confirmed; it will stop at the last confirmed time",
	}
}
