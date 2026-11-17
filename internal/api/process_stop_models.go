package api

import (
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
)

// TypeCommandStopped is the success envelope discriminator of
// DELETE /api/v1/sandboxes/{sandboxId}/commands/{processId}.
const TypeCommandStopped = "command.stopped"

// CommandRecord is the pinned command-history record carried by
// CommandStopResponse. It is one command run through the commands endpoint or,
// later, one recorded sandbox process. Nullable members stay present as JSON
// null because the pinned schema distinguishes null from an omitted value.
type CommandRecord struct {
	CommandID       string     `json:"commandId"`
	SandboxID       string     `json:"sandboxId"`
	Command         string     `json:"command"`
	Cwd             *string    `json:"cwd"`
	Mode            string     `json:"mode"`
	ProcessID       *uint32    `json:"processId"`
	Status          string     `json:"status"`
	ExitCode        *int       `json:"exitCode"`
	Signal          *string    `json:"signal"`
	StdoutBytes     int64      `json:"stdoutBytes"`
	StderrBytes     int64      `json:"stderrBytes"`
	OutputTruncated bool       `json:"outputTruncated"`
	StartedAt       time.Time  `json:"startedAt"`
	FinishedAt      *time.Time `json:"finishedAt"`
}

// CommandStopResponse is the pinned 200 envelope of a stopped command.
// Running is always false after a successful stop; Signaled is false when the
// process had already ended. Command is always the durable record.
type CommandStopResponse struct {
	SuccessBase
	ProcessID uint32         `json:"processId"`
	Running   bool           `json:"running"`
	Signaled  bool           `json:"signaled"`
	Command   *CommandRecord `json:"command"`
}

// commandRecordWire converts one durable record to the pinned wire shape. The
// caller validates the record first, so an internal "starting" status never
// reaches this function's output.
func commandRecordWire(record db.CommandRecord) CommandRecord {
	wire := CommandRecord{
		CommandID:       record.CommandID,
		SandboxID:       record.SandboxID,
		Command:         record.Command,
		Cwd:             record.Cwd,
		Mode:            string(record.Mode),
		Status:          string(record.Status),
		StdoutBytes:     record.StdoutBytes,
		StderrBytes:     record.StderrBytes,
		OutputTruncated: record.OutputTruncated,
		StartedAt:       record.StartedAt,
		FinishedAt:      record.FinishedAt,
	}
	if record.ProcessID != 0 {
		wire.ProcessID = new(record.ProcessID)
	}
	if record.ExitCode != nil {
		code := int(*record.ExitCode)
		wire.ExitCode = &code
	}
	if record.Signal != nil {
		signal := *record.Signal
		wire.Signal = &signal
	}
	return wire
}

// commandStoppedResponse builds the pinned stop envelope. signaled is the
// node's native KillProcess flag, kept separate from the durable status: a
// graceful DELETE of a process that exited 0 is signaled but recorded killed.
func commandStoppedResponse(record db.CommandRecord, signaled bool) CommandStopResponse {
	wire := commandRecordWire(record)
	return CommandStopResponse{
		SuccessBase: success(TypeCommandStopped),
		ProcessID:   record.ProcessID,
		Running:     false,
		Signaled:    signaled,
		Command:     &wire,
	}
}
