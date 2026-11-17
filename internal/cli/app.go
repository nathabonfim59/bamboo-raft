// Package cli implements the bbraft command tree, its configuration and the
// typed v1 API client used by the commands.
//
// The package is deliberately injectable: Options carries the streams, the
// HTTP client, the environment, the configuration directory and the TTY
// decision, so command tests run without a terminal or a real server. Later
// command packages (exec, run, push) add themselves through Options.Commands
// or App.AddCommand and share the same client contract.
package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/spf13/cobra"

	"github.com/nathabonfim59/bamboo-raft/internal/version"
)

// DefaultPollInterval is how often commands that wait for a sandbox or a
// snapshot poll the API. Tests shorten it through Options.
const DefaultPollInterval = 2 * time.Second

// DefaultWaitTimeout bounds every wait a command performs (create, delete,
// snapshot save). It keeps a broken server from hanging the CLI forever.
const DefaultWaitTimeout = 30 * time.Minute

// CommandFactory builds one command bound to an App. It is the injection
// point for command packages that live outside this file set (exec, run,
// push).
type CommandFactory func(*App) *cobra.Command

// Options configures an App. Zero values are replaced with production
// defaults; tests override the fields they need.
type Options struct {
	// Stdin, Stdout and Stderr are the command streams.
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer

	// HTTPClient is the client used for API requests. A clone is made, so the
	// caller's redirect policy and timeout are preserved and extended.
	HTTPClient *http.Client

	// RunProcess starts external programs (ssh, scp). It defaults to the
	// process-group implementation; command tests inject a recorder so no real
	// client ever runs.
	RunProcess ProcessRunner

	// Getenv reads configuration variables. It defaults to os.Getenv.
	Getenv func(string) string

	// ConfigDir holds config.json. It defaults to <user config dir>/bbraft.
	ConfigDir string

	// IsTTY reports whether w is an interactive terminal. It controls
	// automatic JSON output and the delete confirmation prompt.
	IsTTY func(io.Writer) bool

	// PollInterval and WaitTimeout bound the commands that wait on the API.
	PollInterval time.Duration
	WaitTimeout  time.Duration

	// Commands are extra command factories registered after the core tree.
	Commands []CommandFactory
}

// ExitError carries the process exit status. Commands return it when the
// failure already has a specific status, such as a remote exec exit code.
type ExitError struct {
	Code int
	Err  error
}

// Error implements error. A nil Err is a status already reported elsewhere,
// such as a remote exec exit frame; it must not panic or print again.
func (e *ExitError) Error() string {
	if e.Err == nil {
		return fmt.Sprintf("exit status %d", e.Code)
	}
	return e.Err.Error()
}

// Unwrap exposes the cause.
func (e *ExitError) Unwrap() error { return e.Err }

// UsageError marks a parse or argument failure. It exits with status 2 and is
// reported on stderr, never as an API JSON event, because no request ran.
type UsageError struct {
	Err error
}

// Error implements error.
func (e *UsageError) Error() string {
	if e.Err == nil {
		return "invalid usage"
	}
	return e.Err.Error()
}

// Unwrap exposes the cause.
func (e *UsageError) Unwrap() error { return e.Err }

// CLIError is a local failure with a stable code for JSON output, such as a
// blocked deletion or a missing login. It never carries request or secret
// material.
type CLIError struct {
	Code    string
	Message string
}

// Error implements error.
func (e *CLIError) Error() string { return e.Message }

// App is one configured bbraft invocation. It is safe to execute once.
type App struct {
	opts Options
	out  *Output
	root *cobra.Command

	flags globalFlags
}

// globalFlags are the persistent flags shared by every command.
type globalFlags struct {
	apiURL string
	json   bool
}

// NewApp builds the command tree with the core Phase 0.5 commands and the
// caller's extra command factories.
func NewApp(opts Options) *App {
	opts = opts.withDefaults()
	out := &Output{
		Stdout: opts.Stdout,
		Stderr: opts.Stderr,
		IsTTY:  opts.IsTTY(opts.Stdout),
	}
	a := &App{opts: opts, out: out}
	root := &cobra.Command{
		Use:           "bbraft",
		Short:         "Persistent Linux sandboxes for AI agents",
		Version:       version.Version,
		SilenceErrors: true,
		SilenceUsage:  true,
	}
	root.SetIn(opts.Stdin)
	root.SetOut(opts.Stdout)
	root.SetErr(opts.Stderr)
	root.SetFlagErrorFunc(func(_ *cobra.Command, err error) error {
		// Cobra's message names the flag and is already safe.
		return &UsageError{Err: err}
	})
	root.PersistentFlags().StringVar(&a.flags.apiURL, "api-url", "", "API endpoint (env: BBRAFT_API_URL)")
	root.PersistentFlags().BoolVar(&a.flags.json, "json", false, "print JSON for machines instead of text for people")
	root.PersistentPreRunE = func(*cobra.Command, []string) error {
		if a.flags.apiURL != "" {
			if err := ValidateAPIURL(a.flags.apiURL); err != nil {
				return &UsageError{Err: fmt.Errorf("--api-url: %w", err)}
			}
		}
		a.out.ExplicitJSON = a.flags.json
		a.out.JSON = a.flags.json || !a.out.IsTTY
		return nil
	}

	a.root = root
	root.AddCommand(
		newLoginCommand(a),
		newNewCommand(a),
		newListCommand(a),
		newInfoCommand(a),
		newExecCommand(a),
		newRunCommand(a),
		newPushCommand(a),
		newStopCommand(a),
		newResumeCommand(a),
		newExtendCommand(a),
		newRenameCommand(a),
		newSSHCommand(a),
		newSCPCommand(a),
		newDeleteCommand(a),
		newSnapshotCommand(a),
	)
	for _, factory := range opts.Commands {
		if factory != nil {
			root.AddCommand(factory(a))
		}
	}
	return a
}

// withDefaults fills unset options.
func (o Options) withDefaults() Options {
	if o.Stdin == nil {
		o.Stdin = os.Stdin
	}
	if o.Stdout == nil {
		o.Stdout = os.Stdout
	}
	if o.Stderr == nil {
		o.Stderr = os.Stderr
	}
	if o.Getenv == nil {
		o.Getenv = os.Getenv
	}
	if o.IsTTY == nil {
		o.IsTTY = isTerminal
	}
	if o.RunProcess == nil {
		o.RunProcess = runProcess
	}
	if o.PollInterval <= 0 {
		o.PollInterval = DefaultPollInterval
	}
	if o.WaitTimeout <= 0 {
		o.WaitTimeout = DefaultWaitTimeout
	}
	return o
}

// isTerminal reports whether w is an interactive terminal. A plain character
// device is not enough (/dev/null is one), so Linux asks the terminal driver.
func isTerminal(w io.Writer) bool {
	f, ok := w.(*os.File)
	if !ok {
		return false
	}
	return isTerminalFile(f)
}

// SetArgs replaces the command-line arguments. It is the test seam; production
// code lets Cobra read os.Args.
func (a *App) SetArgs(args []string) { a.root.SetArgs(args) }

// AddCommand registers one more command. It must be called before Execute.
func (a *App) AddCommand(cmd *cobra.Command) { a.root.AddCommand(cmd) }

// Execute parses the arguments and runs the selected command. It returns a
// UsageError, an ExitError or a CLIError for the caller to render.
func (a *App) Execute(ctx context.Context) error {
	if err := a.root.ExecuteContext(ctx); err != nil {
		if _, ok := errors.AsType[*UsageError](err); ok {
			return err
		}
		if _, ok := errors.AsType[*ExitError](err); ok {
			return err
		}
		if _, ok := errors.AsType[*CLIError](err); ok {
			return err
		}
		// Cobra's own failures (unknown command, bad positional arguments)
		// are usage errors and must not be reported as API events.
		return &UsageError{Err: err}
	}
	return nil
}

// Run executes the command and renders any failure exactly once. The returned
// value is the process exit status.
func (a *App) Run(ctx context.Context) int {
	err := a.Execute(ctx)
	if err == nil {
		return 0
	}
	if usage, ok := errors.AsType[*UsageError](err); ok {
		fmt.Fprintf(a.opts.Stderr, "bbraft: %v\n", usage.Err)
		return 2
	}
	if exit, ok := errors.AsType[*ExitError](err); ok {
		a.out.Error(exit.Err)
		return exit.Code
	}
	a.out.Error(err)
	return 1
}

// runE adapts a command body to Cobra, tagging ordinary failures with the
// default exit status.
func (a *App) runE(fn func(context.Context, []string) error) func(*cobra.Command, []string) error {
	return func(cmd *cobra.Command, args []string) error {
		if err := fn(cmd.Context(), args); err != nil {
			if _, ok := errors.AsType[*ExitError](err); ok {
				return err
			}
			if _, ok := errors.AsType[*UsageError](err); ok {
				return err
			}
			return &ExitError{Code: 1, Err: err}
		}
		return nil
	}
}

// configDir returns the effective configuration directory.
func (a *App) configDir() (string, error) {
	if a.opts.ConfigDir != "" {
		return a.opts.ConfigDir, nil
	}
	return DefaultConfigDir()
}

// resolveConfig loads the saved configuration and applies the environment and
// the --api-url flag in precedence order.
func (a *App) resolveConfig(ctx context.Context) (Config, error) {
	dir, err := a.configDir()
	if err != nil {
		return Config{}, err
	}
	cfg, err := LoadConfig(ctx, dir, a.opts.Getenv)
	if err != nil {
		return Config{}, err
	}
	if a.flags.apiURL != "" {
		cfg.APIURL = a.flags.apiURL
	}
	if err := ValidateAPIURL(cfg.APIURL); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// apiClient builds the authenticated client for a command.
func (a *App) apiClient(ctx context.Context) (APIClient, error) {
	cfg, err := a.resolveConfig(ctx)
	if err != nil {
		return nil, err
	}
	if cfg.APIKey == "" {
		return nil, &CLIError{
			Code:    "not_logged_in",
			Message: "not signed in: run bbraft login --key-stdin",
		}
	}
	return NewClient(cfg.APIURL, cfg.APIKey, a.opts.HTTPClient)
}

// readWithContext runs read(r). When r is closable and ctx ends first, it
// closes the reader (or a pollable replacement for an os.File that Go cannot
// interrupt) to unblock the read, waits for the reader goroutine to finish, and
// returns ctx.Err; no goroutine is left behind. An injected reader that is not
// closable cannot be interrupted, so its read runs to completion.
func readWithContext(ctx context.Context, r io.Reader, read func(io.Reader) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	readFrom := r
	replaced := false
	if file, ok := r.(*os.File); ok {
		if reopened, ok := reopenPollable(file); ok {
			readFrom = reopened
			replaced = true
		}
	}
	closer, ok := readFrom.(io.Closer)
	if !ok {
		return read(readFrom)
	}
	done := make(chan error, 1)
	go func() { done <- read(readFrom) }()
	select {
	case err := <-done:
		if replaced {
			_ = closer.Close()
		}
		return err
	case <-ctx.Done():
		_ = closer.Close()
		<-done
		if replaced {
			// The read is no longer blocked, so closing the original cannot
			// wait on it; this releases the caller's pipe or terminal.
			if original, ok := r.(io.Closer); ok {
				_ = original.Close()
			}
		}
		return ctx.Err()
	}
}

// sleepCtx waits for d or until ctx is done.
func sleepCtx(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// errWaitTimeout is the cancellation cause of a command wait timeout.
var errWaitTimeout = errors.New("wait timeout")

// waitContext derives a bounded wait context and reports whether the bound
// was the timeout rather than a caller cancellation.
func (a *App) waitContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeoutCause(ctx, a.opts.WaitTimeout, errWaitTimeout)
}

// waitFailure converts a poll-loop error into a stable CLI error.
func (a *App) waitFailure(ctx context.Context, what string, err error) error {
	if errors.Is(context.Cause(ctx), errWaitTimeout) {
		return &CLIError{Code: "timeout", Message: fmt.Sprintf("timed out waiting for %s", what)}
	}
	if errors.Is(err, context.Canceled) {
		return &CLIError{Code: "cancelled", Message: fmt.Sprintf("wait for %s cancelled", what)}
	}
	return err
}
