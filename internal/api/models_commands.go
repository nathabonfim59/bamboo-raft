package api

import "time"

// CommandResponse is the synchronous 200 envelope of POST
// /api/v1/sandboxes/{sandboxId}/commands.
type CommandResponse struct {
	SuccessBase
	Success         bool       `json:"success"`
	CommandID       string     `json:"commandId,omitzero"`
	ExitCode        *int       `json:"exitCode"`
	Signal          *string    `json:"signal,omitzero"`
	OOMKilled       *bool      `json:"oomKilled,omitzero"`
	Stdout          string     `json:"stdout"`
	Stderr          string     `json:"stderr"`
	StdoutTruncated *bool      `json:"stdoutTruncated,omitzero"`
	StderrTruncated *bool      `json:"stderrTruncated,omitzero"`
	TimedOut        bool       `json:"timedOut"`
	Cwd             string     `json:"cwd,omitzero"`
	StartedAt       *time.Time `json:"startedAt,omitzero"`
	FinishedAt      *time.Time `json:"finishedAt,omitzero"`
}

// NewCommandResponse builds a finished-command envelope. exitCode is null when
// the command timed out.
func NewCommandResponse(ok bool, exitCode *int, stdout, stderr string, timedOut bool) CommandResponse {
	return CommandResponse{
		SuccessBase: success(TypeCommandFinished),
		Success:     ok,
		ExitCode:    exitCode,
		Stdout:      stdout,
		Stderr:      stderr,
		TimedOut:    timedOut,
	}
}

// CommandFrameType is one line kind of a streamed command.
type CommandFrameType string

// Stream frame kinds. The stream starts with started, then carries stdout and
// stderr chunks, and ends with exactly one exit or error.
const (
	CommandFrameStarted CommandFrameType = "started"
	CommandFrameStdout  CommandFrameType = "stdout"
	CommandFrameStderr  CommandFrameType = "stderr"
	CommandFrameExit    CommandFrameType = "exit"
	CommandFrameError   CommandFrameType = "error"
)

// CommandStreamFrame is one newline-delimited JSON line of a streamed command.
// Exit and error frames use Optional fields to distinguish an omitted member
// from an explicit null, so a timeout exit frame can carry exitCode: null.
type CommandStreamFrame struct {
	Type      CommandFrameType `json:"type"`
	CommandID string           `json:"commandId,omitzero"`
	Data      string           `json:"data,omitzero"`
	ExitCode  Optional[int]    `json:"exitCode,omitzero"`
	Success   Optional[bool]   `json:"success,omitzero"`
	TimedOut  Optional[bool]   `json:"timedOut,omitzero"`
	Error     string           `json:"error,omitzero"`
	Message   string           `json:"message,omitzero"`
	Retryable Optional[bool]   `json:"retryable,omitzero"`
}
