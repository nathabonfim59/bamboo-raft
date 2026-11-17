package cli

import (
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
)

// Output renders command results. JSON is used when --json was passed or when
// stdout is not a terminal; ExplicitJSON records which of the two happened so
// future commands that pass through a child's output can tell them apart.
type Output struct {
	Stdout       io.Writer
	Stderr       io.Writer
	JSON         bool
	ExplicitJSON bool
	IsTTY        bool
}

// JSONLine writes one JSON object followed by a newline. It is the JSON Lines
// format of new, prompt, events and conversations.
func (o *Output) JSONLine(v any) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("encode json: %w", err)
	}
	raw = append(raw, '\n')
	if _, err := o.Stdout.Write(raw); err != nil {
		return fmt.Errorf("write json: %w", err)
	}
	return nil
}

// Humanf writes one human-readable line to stdout and reports a broken writer.
// Commands must not report success after output failed.
func (o *Output) Humanf(format string, args ...any) error {
	if _, err := fmt.Fprintf(o.Stdout, format+"\n", args...); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	return nil
}

// Event writes one JSON event in JSON mode, or the human line otherwise.
func (o *Output) Event(v any, humanFormat string, args ...any) error {
	if o.JSON {
		return o.JSONLine(v)
	}
	return o.Humanf(humanFormat, args...)
}

// Error renders a failure exactly once. In JSON mode it is the final JSON
// Lines event {"event":"error","error":...,"code":...,"status":...}; otherwise
// it is a line on stderr. API messages are already safe; request bodies and
// credentials are never added here.
func (o *Output) Error(err error) {
	if err == nil {
		return
	}
	message := err.Error()
	event := map[string]any{"event": "error", "error": message}
	if apiErr, ok := errors.AsType[*APIError](err); ok {
		event["error"] = apiErr.Message
		event["code"] = apiErr.Code
		event["status"] = apiErr.Status
		if apiErr.RetryAfterSeconds > 0 {
			event["retryAfterSeconds"] = apiErr.RetryAfterSeconds
		}
	} else if cliErr, ok := errors.AsType[*CLIError](err); ok {
		event["error"] = cliErr.Message
		event["code"] = cliErr.Code
	}
	if !o.JSON {
		fmt.Fprintf(o.Stderr, "bbraft: %s\n", message)
		return
	}
	if writeErr := o.JSONLine(event); writeErr != nil {
		fmt.Fprintf(o.Stderr, "bbraft: %s\n", message)
	}
}
