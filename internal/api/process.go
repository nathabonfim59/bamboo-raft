package api

// Detached command API shared pieces (B2.1): the status path, the durable
// command store contract and the forward-only mapping between the frozen node
// process record and the API wire model. The store commits a process id before
// StartProcess runs, so an ambiguous start keeps the same id and is never
// retried as a new command.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/apierr"
	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// ProcessStatusPath is the API path pattern of detached command status. The
// server adapter registers "GET "+ProcessStatusPath; the pattern carries no
// method so the caller owns registration.
const ProcessStatusPath = "/api/v1/sandboxes/{sandboxId}/commands/{processId}"

// ParamProcessID is the path parameter of the detached process id.
const ParamProcessID = "processId"

// Pinned commandStatus windows. Without offsets each log returns its tail up
// to DefaultCommandStatusTailBytes. With any offset, both streams use offset
// mode and at most CommandStatusOffsetBytes each. tailBytes is validated when
// supplied but ignored in offset mode.
const (
	DefaultCommandStatusTailBytes = 8 << 20
	MaxCommandStatusTailBytes     = 8 << 20
	CommandStatusOffsetBytes      = 1 << 20
)

// Wire status values of the pinned CommandStatusResponse. They are derived from
// the node's RUNNING/EXITED/LOST state only; the internal starting and failed
// states are never invented on the wire.
const (
	commandWireRunning = "running"
	commandWireExited  = "exited"
	commandWireLost    = "lost"
)

// CommandStore is the durable command persistence consumed by detached starts
// and status reads. Every method takes the authenticated owner explicitly and
// must treat a foreign row exactly like a missing one. The accepted
// [*db.Store] satisfies it directly.
type CommandStore interface {
	// AllocateCommand atomically reserves the next never-reused process id
	// and commits the starting record before StartProcess runs.
	AllocateCommand(ctx context.Context, owner uuid.UUID, p db.AllocateCommandParams) (db.CommandRecord, error)
	// ObserveCommand merges one node observation, keeping terminal state
	// and byte counts monotonic.
	ObserveCommand(ctx context.Context, owner uuid.UUID, sandboxID string, processID uint32, obs db.CommandObservation) (db.CommandRecord, error)
	// CommandRecord reads one owner-scoped record, including while the
	// sandbox is stopped.
	CommandRecord(ctx context.Context, owner uuid.UUID, sandboxID string, processID uint32) (db.CommandRecord, error)
}

// CommandStoreContract keeps the database facade inside the accepted contract.
var _ CommandStore = (*db.Store)(nil)

// processNodeError maps a node RPC failure to the pinned process-call error.
// For detached StartProcess and GetProcess, RESOURCE_EXHAUSTED is a transport
// or window-size refusal, never VM capacity: it is the approved 502
// sandbox_direct_failed. Synchronous Exec keeps its legacy 503 capacity
// mapping and its command-table exception.
func processNodeError(err error) *apierr.Error {
	if errors.Is(err, io.EOF) {
		return commandDirectFailed()
	}
	switch status.Code(err) {
	case codes.NotFound:
		return commandNotFound()
	case codes.InvalidArgument:
		return invalidCommandPath()
	case codes.FailedPrecondition:
		return commandNotReady("")
	default:
		return commandDirectFailed()
	}
}

// processNodeErrorForState is [processNodeError] with the durable state used
// for the 409 sandbox_not_ready detail.
func processNodeErrorForState(err error, state SandboxState) *apierr.Error {
	if status.Code(err) == codes.FailedPrecondition {
		return commandNotReady(state)
	}
	return processNodeError(err)
}

// processStartDefinite reports whether a StartProcess failure proves the
// command never started. Only those failures become a failed record; every
// other failure is ambiguous and keeps the record starting.
func processStartDefinite(err error) bool {
	if err == nil || errors.Is(err, io.EOF) {
		return false
	}
	st, ok := status.FromError(err)
	if !ok {
		return false
	}
	switch st.Code() {
	case codes.NotFound, codes.InvalidArgument, codes.FailedPrecondition:
		// These documented refusals answer before any process side
		// effect. Anything else, including RESOURCE_EXHAUSTED and the
		// undocumented auth/conflict codes, may have started the process.
		return true
	default:
		return false
	}
}

// processStartAmbiguous reports whether retrying the exact same StartProcess
// request (same process id and command) is safe. StartProcess is idempotent on
// process_id, so only transport-level uncertainty is retried.
func processStartAmbiguous(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) {
		return true
	}
	st, ok := status.FromError(err)
	if !ok {
		return true
	}
	switch st.Code() {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Internal,
		codes.Unknown, codes.Canceled, codes.Aborted,
		codes.ResourceExhausted:
		return true
	default:
		return false
	}
}

// validateAllocatedCommand rejects a store result that does not describe a
// fresh detached record for this owner and sandbox. A malformed result is a
// safe 500 before any node call, and no id from it is written to the wire.
func validateAllocatedCommand(owner uuid.UUID, sandboxID string, record db.CommandRecord) error {
	if record.OwnerID != owner {
		return errors.New("allocation returned another owner")
	}
	if record.SandboxID != sandboxID {
		return errors.New("allocation returned another sandbox")
	}
	if record.ProcessID == 0 {
		return errors.New("allocation returned process id zero")
	}
	if record.CommandID == "" {
		return errors.New("allocation returned no command id")
	}
	if record.Mode != db.CommandModeDetached {
		return errors.New("allocation returned another mode")
	}
	if record.Status != db.CommandStarting {
		return errors.New("allocation did not return a starting record")
	}
	return nil
}

// commandInternalUnretryable is the standing 500 with an explicit retryable
// false detail for a fault after the node already started the process.
func commandInternalUnretryable() *apierr.Error {
	return apierr.New(http.StatusInternalServerError, apierr.CodeInternal,
		"internal server error").Retryable(false)
}

// invalidCommandPath is the pinned 400 for a bad command working directory.
func invalidCommandPath() *apierr.Error {
	return apierr.New(http.StatusBadRequest, apierr.CodeInvalidPath, "invalid path")
}

// commandProcessIDsExhausted is the pinned 409 for a sandbox that has handed
// out the largest uint32 process id. The counter stays exhausted; the caller
// neither resets it nor retries with a new id.
func commandProcessIDsExhausted() *apierr.Error {
	return apierr.New(http.StatusConflict, apierr.CodeSandboxNotReady,
		"sandbox cannot start more commands; create a new sandbox").Retryable(false)
}

// validateStartedProcess rejects a StartProcess result that cannot be turned
// into a started record or response. An idempotent retry may return an existing
// process in any state, including LOST, so LOST is valid here with no finish
// time. The node must echo the exact command that was sent.
func validateStartedProcess(want uint32, command string, p *nodev1.Process) error {
	if err := validateProcessRecord(want, p, true); err != nil {
		return err
	}
	if p.Command != command {
		return errors.New("start returned a different command")
	}
	return nil
}

// validateProcessOutput rejects a GetProcess result whose identity, state,
// window start or size cannot come from any snapshot of the current log. The
// reference stats the log before it reads it, and the log may grow, shrink or
// be replaced between those two operations, so the captured size proves
// nothing about a valid window. The checks therefore keep only invariants that
// hold for every snapshot: a nonnegative in-range start no later than the
// request, a window within the limit, an empty window when a requested offset
// clamped down, and a zero start for a short tail. Native More is the only
// authority for whether output continues.
func validateProcessOutput(want uint32, request *nodev1.GetProcessRequest, out *nodev1.ProcessOutput) error {
	if request == nil || out == nil {
		return errors.New("missing process window")
	}
	if err := validateProcessRecord(want, out.Process, true); err != nil {
		return err
	}
	limit := int(request.GetLimitBytes())
	if limit == 0 {
		limit = 1 << 20
	}
	if limit < 0 || len(out.Stdout) > limit || len(out.Stderr) > limit {
		return errors.New("window exceeds the requested limit")
	}
	if out.StdoutOffset < 0 || out.StderrOffset < 0 {
		return errors.New("negative window offset")
	}
	if int64(len(out.Stdout)) > maxInt64-out.StdoutOffset ||
		int64(len(out.Stderr)) > maxInt64-out.StderrOffset {
		return errors.New("window offset overflows")
	}
	offsetMode := request.StdoutOffset != nil || request.StderrOffset != nil
	if offsetMode {
		if err := validateOffsetWindow(request.GetStdoutOffset(), out.StdoutOffset, len(out.Stdout)); err != nil {
			return fmt.Errorf("stdout: %w", err)
		}
		if err := validateOffsetWindow(request.GetStderrOffset(), out.StderrOffset, len(out.Stderr)); err != nil {
			return fmt.Errorf("stderr: %w", err)
		}
		return nil
	}
	if err := validateTailWindow(out.StdoutOffset, len(out.Stdout), limit); err != nil {
		return fmt.Errorf("stdout: %w", err)
	}
	if err := validateTailWindow(out.StderrOffset, len(out.Stderr), limit); err != nil {
		return fmt.Errorf("stderr: %w", err)
	}
	return nil
}

// maxInt64 is the largest int64 offset a parsed query can carry.
const maxInt64 = int64(^uint64(0) >> 1)

// validateOffsetWindow accepts the clamped start of any log snapshot. A start
// below the requested offset means min(requested,currentSize) clamped, so the
// log is at its end and the window must be empty; a start above the request or
// a nonempty replayed window there is not a valid snapshot.
func validateOffsetWindow(requested, start int64, length int) error {
	requested = max(requested, 0)
	if start > requested {
		return errors.New("window start is past the requested offset")
	}
	if start < requested && length > 0 {
		return errors.New("clamped window replayed earlier output")
	}
	return nil
}

// validateTailWindow accepts a short tail only at zero and a full tail at any
// nonnegative start; the log snapshot itself may have moved in either
// direction since the process metadata was read.
func validateTailWindow(start int64, length, limit int) error {
	if length < limit && start != 0 {
		return errors.New("short tail did not start at zero")
	}
	return nil
}

// validateStoredCommand rejects a stored or observed record that is not this
// owner's detached process. A malformed result is a safe 500 and never becomes
// wire metadata.
func validateStoredCommand(owner uuid.UUID, sandboxID string, processID uint32, record db.CommandRecord) error {
	if record.OwnerID != owner {
		return errors.New("record belongs to another owner")
	}
	if record.SandboxID != sandboxID {
		return errors.New("record belongs to another sandbox")
	}
	if record.ProcessID != processID {
		return errors.New("record has another process id")
	}
	if record.Mode != db.CommandModeDetached {
		return errors.New("record has another mode")
	}
	return nil
}

// validateProcessRecord checks the node process fields shared by start and
// status. An idempotent StartProcess retry may return LOST, so start also
// allows it.
func validateProcessRecord(want uint32, p *nodev1.Process, allowLost bool) error {
	if p == nil {
		return errors.New("missing process")
	}
	if p.ProcessId != want {
		return fmt.Errorf("process id %d does not match %d", p.ProcessId, want)
	}
	if p.Pid < 0 {
		return errors.New("negative pid")
	}
	switch p.State {
	case nodev1.ProcessState_PROCESS_STATE_RUNNING:
		if p.Pid == 0 {
			return errors.New("running process has no pid")
		}
		if p.Exit != nil {
			return errors.New("running process has an exit")
		}
		if p.FinishedAtUnixMs != 0 {
			return errors.New("running process has a finish time")
		}
	case nodev1.ProcessState_PROCESS_STATE_EXITED:
		if p.Pid == 0 {
			return errors.New("exited process has no pid")
		}
		if err := validateProcessExit(p.Exit); err != nil {
			return err
		}
		if p.FinishedAtUnixMs <= 0 {
			return errors.New("exited process has no finish time")
		}
	case nodev1.ProcessState_PROCESS_STATE_LOST:
		if !allowLost {
			return errors.New("start returned a lost process")
		}
		if p.Exit != nil {
			return errors.New("lost process has an exit")
		}
		if p.FinishedAtUnixMs != 0 {
			return errors.New("lost process has a finish time")
		}
	default:
		return fmt.Errorf("unknown process state %d", p.State)
	}
	if p.StartedAtUnixMs <= 0 {
		return errors.New("process has no start time")
	}
	if p.StdoutSize < 0 || p.StderrSize < 0 {
		return errors.New("negative output size")
	}
	if p.Cwd != "" && !strings.HasPrefix(p.Cwd, "/") {
		return errors.New("process cwd is not absolute")
	}
	if p.LogPath != "" && !strings.HasPrefix(p.LogPath, "/") {
		return errors.New("process stdout log path is not absolute")
	}
	if p.ErrLogPath != "" && !strings.HasPrefix(p.ErrLogPath, "/") {
		return errors.New("process stderr log path is not absolute")
	}
	return nil
}

// validateProcessExit requires exactly one of an exit code or a signal and
// rejects a timeout, which a detached process cannot have.
func validateProcessExit(exit *nodev1.ExecEvent_Exit) error {
	if exit == nil {
		return errors.New("exited process has no exit")
	}
	if exit.TimedOut {
		return errors.New("detached process exit has timed_out")
	}
	hasCode := exit.ExitCode != nil
	hasSignal := exit.Signal != ""
	if hasCode == hasSignal {
		return errors.New("exit code and signal must be mutually exclusive")
	}
	return nil
}

// commandObservationFromProcess converts a validated node process into the
// durable observation. A signal exit is recorded as killed, matching the
// pinned CommandRecord status, while the status endpoint keeps the wire status
// exited.
func commandObservationFromProcess(p *nodev1.Process) db.CommandObservation {
	obs := db.CommandObservation{
		PID:         p.Pid,
		LogPath:     p.LogPath,
		ErrLogPath:  p.ErrLogPath,
		StdoutBytes: p.StdoutSize,
		StderrBytes: p.StderrSize,
	}
	if p.StartedAtUnixMs > 0 {
		started := time.UnixMilli(p.StartedAtUnixMs).UTC()
		obs.StartedAt = &started
	}
	if p.FinishedAtUnixMs > 0 {
		finished := time.UnixMilli(p.FinishedAtUnixMs).UTC()
		obs.FinishedAt = &finished
	}
	if p.Cwd != "" {
		cwd := p.Cwd
		obs.Cwd = &cwd
	}
	switch p.State {
	case nodev1.ProcessState_PROCESS_STATE_RUNNING:
		obs.Status = db.CommandRunning
	case nodev1.ProcessState_PROCESS_STATE_LOST:
		obs.Status = db.CommandLost
	case nodev1.ProcessState_PROCESS_STATE_EXITED:
		if p.Exit.GetSignal() != "" {
			obs.Status = db.CommandKilled
			signal := p.Exit.GetSignal()
			obs.Signal = &signal
		} else {
			obs.Status = db.CommandExited
		}
		if p.Exit.ExitCode != nil {
			code := *p.Exit.ExitCode
			obs.ExitCode = &code
		}
	}
	return obs
}

// processWireStatus converts the node state to the only three values the pinned
// CommandStatusResponse allows.
func processWireStatus(state nodev1.ProcessState) string {
	switch state {
	case nodev1.ProcessState_PROCESS_STATE_RUNNING:
		return commandWireRunning
	case nodev1.ProcessState_PROCESS_STATE_EXITED:
		return commandWireExited
	case nodev1.ProcessState_PROCESS_STATE_LOST:
		return commandWireLost
	default:
		return ""
	}
}
