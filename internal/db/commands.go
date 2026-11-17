package db

// Durable detached command records (B2.1). Allocation commits the command
// row and its per-sandbox process id before StartProcess is called, so an
// ambiguous start keeps the id and retries with it, and a process id is never
// handed out twice. The row stores identity, node log metadata, byte counts
// and state only: the request's environment is never persisted, and output
// bytes stay on the sandbox.

import (
	"context"
	"errors"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/ids"
)

// MaxCommandProcessID is the largest process id the node contract and the
// command_records.check allow.
const MaxCommandProcessID uint32 = 1<<32 - 1

// maxStoredCommandBytes bounds the command text kept in the record. The
// pinned CommandRecord keeps the first 64 KiB; the API keeps the full text
// for StartProcess and stores only this prefix.
const maxStoredCommandBytes = 64 * 1024

// Command-mode values. Phase 1 records only detached starts; sync, stream and
// process-history modes are later work.
type CommandMode string

// CommandModeDetached marks a command started with StartProcess.
const CommandModeDetached CommandMode = "detached"

// CommandStatus is the durable command state. The pinned wire enum is
// running, exited, lost, killed, failed and timed_out. CommandStarting is
// internal: it means the id and record were committed but no StartProcess
// observation has confirmed the process yet. It must never be emitted as a
// pinned CommandRecord status.
type CommandStatus string

const (
	// CommandStarting is the internal pre-observation state.
	CommandStarting CommandStatus = "starting"
	// CommandRunning mirrors ProcessState RUNNING.
	CommandRunning CommandStatus = "running"
	// CommandExited mirrors ProcessState EXITED with an exit result.
	CommandExited CommandStatus = "exited"
	// CommandLost mirrors ProcessState LOST (the VM stopped or died).
	CommandLost CommandStatus = "lost"
	// CommandKilled marks a process ended by a stop or signal.
	CommandKilled CommandStatus = "killed"
	// CommandFailed marks a start that definitely failed.
	CommandFailed CommandStatus = "failed"
	// CommandTimedOut marks a command ended by its timeout.
	CommandTimedOut CommandStatus = "timed_out"
)

// Errors reported by the command store. They are returned unwrapped so
// callers compare with errors.Is.
var (
	// ErrInvalidCommand rejects command or cwd text that is not valid
	// UTF-8 or contains a NUL byte, and observations with a negative pid,
	// stdout or stderr byte count.
	ErrInvalidCommand = errors.New("db: invalid command text")

	// ErrInvalidCommandStatus rejects an observation with an unknown or
	// non-observable status.
	ErrInvalidCommandStatus = errors.New("db: invalid command status")

	// ErrCommandIDExhausted means the sandbox handed out process id
	// 4294967295; no further id can be allocated and the counter does not
	// wrap or change.
	ErrCommandIDExhausted = errors.New("db: command process ids exhausted")
)

// CommandRecord is one durable detached command. Command is the recorded
// prefix of the request (up to 64 KiB on a UTF-8 boundary). Byte counts are
// the last observation's sizes, never output payload. StartedAt is always
// set: allocation time until the node reports its canonical start time.
// FinishedAt is nil while open, and stays nil for a LOST process whose finish
// time the node did not record (Process.finished_at_unix_ms is 0 for LOST);
// the pinned CommandRecord.finishedAt allows null.
type CommandRecord struct {
	CommandID       string
	OwnerID         uuid.UUID
	SandboxID       string
	ProcessID       uint32
	Mode            CommandMode
	Command         string
	Cwd             *string
	Status          CommandStatus
	PID             int32
	ExitCode        *int32
	Signal          *string
	StartedAt       time.Time
	FinishedAt      *time.Time
	StdoutBytes     int64
	StderrBytes     int64
	OutputTruncated bool
	LogPath         string
	ErrLogPath      string
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

// AllocateCommandParams is one detached start. Env is deliberately absent:
// environment values are never part of the command record.
type AllocateCommandParams struct {
	SandboxID string
	Command   string
	Cwd       *string
}

// CommandObservation is what the server learned from StartProcess, GetProcess
// or KillProcess. StartedAt is the node's canonical time; nil keeps the
// record's value. Cwd, when set, is the node's canonical working directory
// and replaces the request's value while the record is open. FinishedAt nil
// on a final status defaults to now, except for LOST, where nil means the
// node did not record a finish time and is stored as NULL. Byte counts and
// OutputTruncated merge monotonically, so a stale observation cannot shrink
// or un-truncate a record.
type CommandObservation struct {
	Status          CommandStatus
	PID             int32
	ExitCode        *int32
	Signal          *string
	StartedAt       *time.Time
	FinishedAt      *time.Time
	Cwd             *string
	LogPath         string
	ErrLogPath      string
	StdoutBytes     int64
	StderrBytes     int64
	OutputTruncated bool
}

const commandRecordCols = `id, owner_id, sandbox_id, process_id, mode, command,
	cwd, status, pid, exit_code, signal, started_at, finished_at, stdout_bytes,
	stderr_bytes, output_truncated, log_path, err_log_path, created_at, updated_at`

// commandRecordColsQualified is [commandRecordCols] for reads that join the
// sandbox row, where the column names would otherwise be ambiguous.
const commandRecordColsQualified = `r.id, r.owner_id, r.sandbox_id, r.process_id,
	r.mode, r.command, r.cwd, r.status, r.pid, r.exit_code, r.signal,
	r.started_at, r.finished_at, r.stdout_bytes, r.stderr_bytes,
	r.output_truncated, r.log_path, r.err_log_path, r.created_at, r.updated_at`

func scanCommandRecord(row scanner) (CommandRecord, error) {
	var (
		rec       CommandRecord
		processID int64
		mode      string
	)
	err := row.Scan(&rec.CommandID, &rec.OwnerID, &rec.SandboxID, &processID,
		&mode, &rec.Command, &rec.Cwd, &rec.Status, &rec.PID, &rec.ExitCode,
		&rec.Signal, &rec.StartedAt, &rec.FinishedAt, &rec.StdoutBytes,
		&rec.StderrBytes, &rec.OutputTruncated, &rec.LogPath, &rec.ErrLogPath,
		&rec.CreatedAt, &rec.UpdatedAt)
	if err != nil {
		return CommandRecord{}, err
	}
	rec.ProcessID = uint32(processID)
	rec.Mode = CommandMode(mode)
	return rec, nil
}

// AllocateCommand atomically reserves the next process id for one sandbox and
// commits its command record. The sandbox row is locked for the whole
// transaction, so a concurrent deletion or stop serializes against this
// allocation. An unknown, foreign or deleted sandbox answers [ErrNotFound]; a
// sandbox that is not ready, idle or running answers [ErrInvalidState]. A
// failed allocation consumes no process id because the counter update rolls
// back with the transaction.
//
// The returned row is CommandStarting. The caller keeps the full command text
// for StartProcess and must retry an ambiguous start with the same ProcessID.
func (s *Store) AllocateCommand(ctx context.Context, owner uuid.UUID, p AllocateCommandParams) (CommandRecord, error) {
	if owner == uuid.Nil() {
		return CommandRecord{}, ErrNoOwner
	}
	command, err := commandTextForRecord(p.Command)
	if err != nil {
		return CommandRecord{}, err
	}
	var cwd *string
	if p.Cwd != nil {
		if !storableText(*p.Cwd) {
			return CommandRecord{}, ErrInvalidCommand
		}
		cwd = p.Cwd
	}
	var (
		rec   CommandRecord
		cwdDB any
	)
	if cwd != nil {
		cwdDB = *cwd
	}
	err = s.runTx(ctx, "allocate command", func(ctx context.Context, tx pgx.Tx) error {
		sbx, err := sandboxRowForUpdateTx(ctx, tx, owner, p.SandboxID)
		if err != nil {
			return err
		}
		switch sbx.State {
		case StateReady, StateIdle, StateRunning:
		default:
			return ErrInvalidState
		}
		// Insert the first id for a new sandbox; otherwise advance the
		// counter only while the last allocated id is below the uint32
		// maximum. At the maximum the conflict update matches no row, so
		// the counter stays put and the allocation refuses.
		var last int64
		err = tx.QueryRow(ctx, `
			INSERT INTO command_process_counters (sandbox_id, last_process_id)
			VALUES ($1, 1)
			ON CONFLICT (sandbox_id) DO UPDATE
				SET last_process_id = command_process_counters.last_process_id + 1
				WHERE command_process_counters.last_process_id < 4294967295
			RETURNING last_process_id`, p.SandboxID).Scan(&last)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrCommandIDExhausted
		}
		if err != nil {
			return fmtError("allocate command process id", err)
		}
		rec, err = scanCommandRecord(tx.QueryRow(ctx, `
			INSERT INTO command_records (
				id, owner_id, sandbox_id, process_id, mode, command, cwd, status)
			VALUES ($1, $2, $3, $4, 'detached', $5, $6, 'starting')
			RETURNING `+commandRecordCols,
			ids.New("cmd_", 12), owner, p.SandboxID, last, command, cwdDB))
		if err != nil {
			return fmtError("insert command record", err)
		}
		return nil
	})
	if err != nil {
		return CommandRecord{}, err
	}
	return rec, nil
}

// ObserveCommand records what the node reported for one command. The sandbox
// row is locked first (so a concurrent deletion fences the write), then the
// record row. Starting and running rows move to any observation; the first
// final status is sticky. Later observations still merge monotonic byte
// counts, OutputTruncated and non-empty log paths, but never change a final
// status, exit, signal, pid or finish time. Reverse-order observations are
// therefore safe and return the current row without an error. An unknown,
// foreign or deleted sandbox or process answers [ErrNotFound]. Archived and
// other non-deleted sandboxes may still record a late LOST observation; a
// LOST observation without a node finish time leaves FinishedAt nil.
func (s *Store) ObserveCommand(ctx context.Context, owner uuid.UUID, sandboxID string, processID uint32, obs CommandObservation) (CommandRecord, error) {
	if owner == uuid.Nil() {
		return CommandRecord{}, ErrNoOwner
	}
	if processID == 0 {
		return CommandRecord{}, ErrNotFound
	}
	if !obs.Status.observable() {
		return CommandRecord{}, ErrInvalidCommandStatus
	}
	if obs.Cwd != nil && !storableText(*obs.Cwd) {
		return CommandRecord{}, ErrInvalidCommand
	}
	if obs.PID < 0 || obs.StdoutBytes < 0 || obs.StderrBytes < 0 {
		return CommandRecord{}, ErrInvalidCommand
	}
	var rec CommandRecord
	err := s.runTx(ctx, "observe command", func(ctx context.Context, tx pgx.Tx) error {
		var state SandboxState
		err := tx.QueryRow(ctx, `
			SELECT state FROM sandboxes
			WHERE id = $1 AND owner_id = $2 AND deleted_at IS NULL
			FOR UPDATE`, sandboxID, owner).Scan(&state)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return fmtError("lock command sandbox", err)
		}
		current, err := scanCommandRecord(tx.QueryRow(ctx, `
			SELECT `+commandRecordCols+` FROM command_records
			WHERE owner_id = $1 AND sandbox_id = $2 AND process_id = $3
			FOR UPDATE`, owner, sandboxID, processID))
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return fmtError("lock command record", err)
		}
		next := mergeCommandObservation(current, obs, time.Now())
		rec, err = scanCommandRecord(tx.QueryRow(ctx, `
			UPDATE command_records SET
				status = $4, pid = $5, exit_code = $6, signal = $7,
				started_at = $8, finished_at = $9, cwd = $15,
				stdout_bytes = $10, stderr_bytes = $11,
				output_truncated = $12, log_path = $13,
				err_log_path = $14, updated_at = now()
			WHERE id = $1 AND owner_id = $2 AND sandbox_id = $3
			RETURNING `+commandRecordCols,
			current.CommandID, owner, sandboxID, next.Status, next.PID,
			next.ExitCode, next.Signal, next.StartedAt, next.FinishedAt,
			next.StdoutBytes, next.StderrBytes, next.OutputTruncated,
			next.LogPath, next.ErrLogPath, next.Cwd))
		if err != nil {
			return fmtError("update command record", err)
		}
		return nil
	})
	if err != nil {
		return CommandRecord{}, err
	}
	return rec, nil
}

// CommandRecord reads one command by owner, sandbox and process id. Unknown,
// foreign, deleted-sandbox and process-id-zero lookups answer [ErrNotFound],
// never another owner's row. It works while the sandbox is archived, matching
// the pinned status endpoint.
func (s *Store) CommandRecord(ctx context.Context, owner uuid.UUID, sandboxID string, processID uint32) (CommandRecord, error) {
	if owner == uuid.Nil() {
		return CommandRecord{}, ErrNoOwner
	}
	if processID == 0 {
		return CommandRecord{}, ErrNotFound
	}
	rec, err := scanCommandRecord(s.pool.QueryRow(ctx, `
		SELECT `+commandRecordColsQualified+` FROM command_records r
		JOIN sandboxes s ON s.id = r.sandbox_id AND s.owner_id = r.owner_id
		WHERE r.owner_id = $1 AND r.sandbox_id = $2 AND r.process_id = $3
			AND s.deleted_at IS NULL`, owner, sandboxID, processID))
	if errors.Is(err, pgx.ErrNoRows) {
		return CommandRecord{}, ErrNotFound
	}
	if err != nil {
		return CommandRecord{}, fmtError("read command record", err)
	}
	return rec, nil
}

// observable reports whether obs is a status the node can report or the
// server can record for a definite start failure. CommandStarting is never an
// observation.
func (c CommandStatus) observable() bool {
	switch c {
	case CommandRunning, CommandExited, CommandLost, CommandKilled,
		CommandFailed, CommandTimedOut:
		return true
	}
	return false
}

// mergeCommandObservation applies obs on top of cur. State fields only move
// while cur is open; terminal state (status, pid, exit, signal, start and
// finish times, cwd) is sticky. Counts, truncation and log paths merge on
// every observation so a late or out-of-order read cannot lose the node's
// final metadata.
func mergeCommandObservation(cur CommandRecord, obs CommandObservation, now time.Time) CommandRecord {
	next := cur
	next.StdoutBytes = max(cur.StdoutBytes, obs.StdoutBytes)
	next.StderrBytes = max(cur.StderrBytes, obs.StderrBytes)
	next.OutputTruncated = cur.OutputTruncated || obs.OutputTruncated
	if obs.LogPath != "" {
		next.LogPath = obs.LogPath
	}
	if obs.ErrLogPath != "" {
		next.ErrLogPath = obs.ErrLogPath
	}
	open := cur.Status == CommandStarting || cur.Status == CommandRunning
	if !open {
		return next
	}
	if obs.StartedAt != nil {
		next.StartedAt = *obs.StartedAt
	}
	if obs.Cwd != nil {
		next.Cwd = obs.Cwd
	}
	if obs.PID > 0 {
		next.PID = obs.PID
	}
	if obs.Status == CommandRunning {
		next.Status = CommandRunning
		next.ExitCode = nil
		next.Signal = nil
		next.FinishedAt = nil
		return next
	}
	next.Status = obs.Status
	next.ExitCode = obs.ExitCode
	next.Signal = obs.Signal
	if obs.Status == CommandLost {
		// Process.finished_at_unix_ms is 0 for LOST, and the pinned
		// CommandRecord.finishedAt may be null: keep an unknown finish
		// unknown instead of fabricating one.
		next.ExitCode = nil
		next.Signal = nil
		if obs.FinishedAt != nil {
			finished := *obs.FinishedAt
			next.FinishedAt = &finished
		} else {
			next.FinishedAt = nil
		}
		return next
	}
	if obs.Status == CommandFailed {
		// A definite start failure has no exit result to report.
		next.ExitCode = nil
		next.Signal = nil
	}
	finished := now
	if obs.FinishedAt != nil {
		finished = *obs.FinishedAt
	}
	next.FinishedAt = &finished
	return next
}

// commandTextForRecord validates the text and cuts the stored command to
// maxStoredCommandBytes without splitting a rune. The full text stays with
// the caller for StartProcess.
func commandTextForRecord(command string) (string, error) {
	if !storableText(command) {
		return "", ErrInvalidCommand
	}
	if len(command) <= maxStoredCommandBytes {
		return command, nil
	}
	cut := maxStoredCommandBytes
	for cut > 0 && !utf8.RuneStart(command[cut]) {
		cut--
	}
	return command[:cut], nil
}

// storableText reports whether s can be stored in a PostgreSQL text column
// and is valid UTF-8. NUL is valid UTF-8 but text cannot hold it; leaving it
// to the insert would surface as an internal error instead of a rejected
// request.
func storableText(s string) bool {
	return utf8.ValidString(s) && strings.IndexByte(s, 0) < 0
}
