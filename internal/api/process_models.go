package api

import "time"

// CommandStartedResponse is the pinned 200 envelope of a detached
// POST /api/v1/sandboxes/{sandboxId}/commands. Command carries the full text as
// sent, not the durable record's 64 KiB prefix.
type CommandStartedResponse struct {
	SuccessBase
	Success    bool      `json:"success"`
	CommandID  string    `json:"commandId,omitzero"`
	ProcessID  uint32    `json:"processId"`
	PID        int32     `json:"pid"`
	Command    string    `json:"command"`
	Cwd        string    `json:"cwd,omitzero"`
	StartedAt  time.Time `json:"startedAt"`
	LogPath    string    `json:"logPath,omitzero"`
	ErrLogPath string    `json:"errLogPath,omitzero"`
}

// CommandStatusResponse is the pinned 200 envelope of
// GET /api/v1/sandboxes/{sandboxId}/commands/{processId}. exitCode is required
// and nullable; the offset and more members are present only in offset mode.
type CommandStatusResponse struct {
	SuccessBase
	Success          bool       `json:"success"`
	ProcessID        uint32     `json:"processId"`
	PID              *int32     `json:"pid,omitzero"`
	Status           string     `json:"status"`
	Known            *bool      `json:"known,omitzero"`
	Running          bool       `json:"running"`
	ExitCode         *int       `json:"exitCode"`
	Signal           *string    `json:"signal,omitzero"`
	OOMKilled        *bool      `json:"oomKilled,omitzero"`
	Command          *string    `json:"command,omitzero"`
	Cwd              *string    `json:"cwd,omitzero"`
	StartedAt        *time.Time `json:"startedAt,omitzero"`
	FinishedAt       *time.Time `json:"finishedAt,omitzero"`
	Stdout           string     `json:"stdout"`
	Stderr           string     `json:"stderr"`
	StdoutTruncated  *bool      `json:"stdoutTruncated,omitzero"`
	StderrTruncated  *bool      `json:"stderrTruncated,omitzero"`
	LogPath          string     `json:"logPath,omitzero"`
	ErrLogPath       string     `json:"errLogPath,omitzero"`
	StdoutOffset     *int64     `json:"stdoutOffset,omitzero"`
	StderrOffset     *int64     `json:"stderrOffset,omitzero"`
	StdoutNextOffset *int64     `json:"stdoutNextOffset,omitzero"`
	StderrNextOffset *int64     `json:"stderrNextOffset,omitzero"`
	More             *bool      `json:"more,omitzero"`
}
