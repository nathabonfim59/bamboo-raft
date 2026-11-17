package api

// CommandAttachFrameType is one line kind of a detached command attach stream.
type CommandAttachFrameType string

// Attach frame kinds. The stream starts with exactly one attached line, then
// carries stdout and stderr chunks, heartbeats while the command is quiet, and
// ends with exactly one exit, reconnect or error line.
const (
	CommandAttachAttached  CommandAttachFrameType = "attached"
	CommandAttachStdout    CommandAttachFrameType = "stdout"
	CommandAttachStderr    CommandAttachFrameType = "stderr"
	CommandAttachHeartbeat CommandAttachFrameType = "heartbeat"
	CommandAttachExit      CommandAttachFrameType = "exit"
	CommandAttachReconnect CommandAttachFrameType = "reconnect"
	CommandAttachError     CommandAttachFrameType = "error"
)

// CommandAttachFrame is one newline-delimited JSON line of
// GET /api/v1/sandboxes/{sandboxId}/commands/{processId}/stream.
//
// Offsets are raw log byte offsets, never rune or character counts. A client
// reconnects with the stdoutOffset and stderrOffset of the last reconnect,
// exit or error frame, so output is neither lost nor repeated. Reconnect and
// error frames point before any incomplete trailing UTF-8 rune that had not
// been emitted yet.
type CommandAttachFrame struct {
	Type         CommandAttachFrameType `json:"type"`
	ProcessID    uint32                 `json:"processId,omitzero"`
	Data         string                 `json:"data,omitzero"`
	NextOffset   *int64                 `json:"nextOffset,omitzero"`
	StdoutOffset *int64                 `json:"stdoutOffset,omitzero"`
	StderrOffset *int64                 `json:"stderrOffset,omitzero"`
	Status       string                 `json:"status,omitzero"`
	ExitCode     Optional[int]          `json:"exitCode,omitzero"`
	Signal       Optional[string]       `json:"signal,omitzero"`
	Error        string                 `json:"error,omitzero"`
	Message      string                 `json:"message,omitzero"`
}
