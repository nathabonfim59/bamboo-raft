package api

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/apierr"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// fileNodeOp identifies the node RPC behind a handler. It selects the
// retryable flag of ambiguous failures and the two documented ALREADY_EXISTS
// answers.
type fileNodeOp int

const (
	fileOpRead fileNodeOp = iota
	fileOpWrite
	fileOpDelete
	fileOpList
	fileOpStat
	fileOpMakeDir
	fileOpMove
	fileOpChmod
)

// retryable reports whether repeating the operation has the same result. The
// pinned error model marks file reads and writes retryable: repeating them
// gives the same result. Listing and stat do too. Deletes, moves, mkdir and
// chmod stay conservative because they may already have taken effect. The
// flag is wire metadata only; the server never retries a call itself.
func (op fileNodeOp) retryable() bool {
	switch op {
	case fileOpRead, fileOpWrite, fileOpList, fileOpStat:
		return true
	default:
		return false
	}
}

// resolveTarget performs the owner-scoped lookup and waits for a starting
// sandbox, like the pinned command readiness rule. proceed is false only when
// the caller canceled: the handler must then write nothing.
func (s *filesService) resolveTarget(ctx context.Context, owner, sandboxID string) (CommandTarget, *apierr.Error, bool) {
	deadline := time.Now().Add(s.readyDeadline)
	waitCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	lastState := SandboxState("")
	for {
		target, err := s.lookup.TargetLookup(waitCtx, owner, sandboxID)
		if err != nil {
			if ctx.Err() != nil {
				return CommandTarget{}, nil, false
			}
			if waitCtx.Err() != nil {
				return CommandTarget{}, fileSandboxStarting(lastState), true
			}
			if errors.Is(err, ErrCommandTargetNotFound) {
				return CommandTarget{}, fileNotFound(), true
			}
			if apiErr, ok := errors.AsType[*apierr.Error](err); ok {
				// A lookup only distinguishes found from missing. Do not
				// forward its message: storage errors can echo query text
				// or a credential.
				s.logger.ErrorContext(ctx, "api: file target lookup failed",
					"sandbox_id", sandboxID, "status", apiErr.Status, "code", apiErr.Code)
				if apiErr.Status == http.StatusNotFound {
					return CommandTarget{}, fileNotFound(), true
				}
				return CommandTarget{}, fileInternal(), true
			}
			s.logger.ErrorContext(ctx, "api: file target lookup failed",
				"sandbox_id", sandboxID, "error", fileErrorName(err))
			return CommandTarget{}, fileInternal(), true
		}
		lastState = target.State
		switch {
		case fileStateReady(target.State):
			if target.NodeID == "" {
				s.logger.ErrorContext(ctx, "api: ready sandbox has no node",
					"sandbox_id", sandboxID, "state", target.State)
				return CommandTarget{}, fileSandboxNotReady(target.State), true
			}
			return target, nil, true
		case fileStateStarting(target.State):
			if ctx.Err() != nil {
				return CommandTarget{}, nil, false
			}
			remaining := time.Until(deadline)
			if remaining <= 0 {
				return CommandTarget{}, fileSandboxStarting(lastState), true
			}
			timer := time.NewTimer(min(s.readyPoll, remaining))
			select {
			case <-ctx.Done():
				timer.Stop()
				return CommandTarget{}, nil, false
			case <-waitCtx.Done():
				timer.Stop()
				if ctx.Err() != nil {
					return CommandTarget{}, nil, false
				}
				return CommandTarget{}, fileSandboxStarting(lastState), true
			case <-timer.C:
			}
		default:
			return CommandTarget{}, fileStateError(target.State), true
		}
	}
}

// fileStateReady reports whether a sandbox accepts file operations.
func fileStateReady(state SandboxState) bool {
	switch state {
	case StateReady, StateIdle, StateRunning:
		return true
	default:
		return false
	}
}

// fileStateStarting reports whether a sandbox is still coming up and a file
// call should wait for it.
func fileStateStarting(state SandboxState) bool {
	switch state {
	case StateInit, StateProvisioning, StateProvisioned, StateCloning:
		return true
	default:
		return false
	}
}

// fileStateError maps a settled sandbox that cannot take a file call. A
// stopped (archived) sandbox has no machine; every other settled state is a
// state conflict.
func fileStateError(state SandboxState) *apierr.Error {
	if state == StateArchived {
		return fileMachineNotRunning(state)
	}
	return fileSandboxNotReady(state)
}

// dial returns the node client of target and a function that releases it.
func (s *filesService) dial(nodeID string) (nodev1.NodeClient, func() error, *apierr.Error) {
	client, closeClient, err := s.clients(nodeID)
	if err != nil {
		s.logger.Error("api: file node client", "node_id", nodeID, "error", fileErrorName(err))
		return nil, nil, fileDirectFailed(true)
	}
	if client == nil {
		s.logger.Error("api: file node client is nil", "node_id", nodeID)
		if closeClient != nil {
			if err := closeClient(); err != nil {
				s.logger.Debug("api: close file node client", "error", fileErrorName(err))
			}
		}
		return nil, nil, fileInternal()
	}
	if closeClient == nil {
		closeClient = func() error { return nil }
	}
	return client, closeClient, nil
}

// closeClient releases a node client, logging a failure without failing the
// already-finished response.
func (s *filesService) closeClient(closeClient func() error) {
	if err := closeClient(); err != nil {
		s.logger.Debug("api: close file node client", "error", fileErrorName(err))
	}
}

// dialForRequest dials the target node for one request and stops before the
// RPC when the caller canceled during the factory call.
func (s *filesService) dialForRequest(w http.ResponseWriter, r *http.Request, nodeID string) (nodev1.NodeClient, func() error, bool) {
	client, closeClient, apiErr := s.dial(nodeID)
	if apiErr != nil {
		fileWriteError(w, r, apiErr)
		return nil, nil, false
	}
	if r.Context().Err() != nil {
		s.closeClient(closeClient)
		return nil, nil, false
	}
	return client, closeClient, true
}

// mapNodeError converts a failed file RPC to the approved HTTP table. Native
// error text never reaches the wire or the log; only the gRPC code is used.
func (s *filesService) mapNodeError(ctx context.Context, op fileNodeOp, sandboxID string, err error) *apierr.Error {
	st, ok := status.FromError(err)
	if !ok {
		s.fileNodeFailure(ctx, op, sandboxID, "not_a_status")
		return fileDirectFailed(op.retryable())
	}
	switch st.Code() {
	case codes.NotFound:
		return fileNotFound()
	case codes.InvalidArgument:
		return fileInvalidPath()
	case codes.FailedPrecondition:
		return fileSandboxNotReady("")
	case codes.AlreadyExists:
		switch op {
		case fileOpMakeDir:
			return fileInvalidPathMessage("path exists and is not a directory")
		case fileOpMove:
			return fileInvalidPathMessage("destination exists; set overwrite")
		}
		s.fileNodeFailure(ctx, op, sandboxID, st.Code().String())
		return fileDirectFailed(op.retryable())
	case codes.ResourceExhausted:
		// File RPCs never boot a VM, so no capacity answer applies here.
		// An unexpected exhaustion is an ordinary node failure.
		s.fileNodeFailure(ctx, op, sandboxID, st.Code().String())
		return fileDirectFailed(op.retryable())
	default:
		s.fileNodeFailure(ctx, op, sandboxID, st.Code().String())
		return fileDirectFailed(op.retryable())
	}
}

// fileNodeFailure logs the gRPC code only.
func (s *filesService) fileNodeFailure(ctx context.Context, op fileNodeOp, sandboxID, code string) {
	s.logger.ErrorContext(ctx, "api: file node call failed",
		"sandbox_id", sandboxID, "op", op.logName(), "code", code)
}

// fileEntry validates one node metadata record and converts it. A record that
// violates the frozen contract is an ambiguous node failure; the native
// values are never echoed.
func (s *filesService) fileEntry(ctx context.Context, op fileNodeOp, sandboxID string, info *nodev1.FileInfo) (FileEntry, *apierr.Error) {
	entry, err := FileEntryFromInfo(info)
	if err != nil {
		s.logger.ErrorContext(ctx, "api: file metadata violates the node contract",
			"sandbox_id", sandboxID, "op", op.logName())
		return FileEntry{}, fileDirectFailed(op.retryable())
	}
	return entry, nil
}

// unaryContractViolation reports a unary response that does not match the
// request as an ambiguous node failure.
func (s *filesService) unaryContractViolation(ctx context.Context, op fileNodeOp, sandboxID, reason string) *apierr.Error {
	s.logger.ErrorContext(ctx, "api: file response violates the node contract",
		"sandbox_id", sandboxID, "op", op.logName(), "reason", reason)
	return fileDirectFailed(op.retryable())
}

// fileWriteSuccess writes a success envelope only while the caller is still
// present. A cancellation after a successful lookup, factory or RPC must not
// turn into a response.
func fileWriteSuccess(w http.ResponseWriter, r *http.Request, status int, typ string, payload any) {
	if r.Context().Err() != nil {
		return
	}
	apierr.Write(w, status, typ, payload)
}

// logName names one operation for logs.
func (op fileNodeOp) logName() string {
	switch op {
	case fileOpRead:
		return "read"
	case fileOpWrite:
		return "write"
	case fileOpDelete:
		return "delete"
	case fileOpList:
		return "list"
	case fileOpStat:
		return "stat"
	case fileOpMakeDir:
		return "mkdir"
	case fileOpMove:
		return "move"
	case fileOpChmod:
		return "chmod"
	}
	return "unknown"
}

// fileReadResult is one fully buffered bounded file.
type fileReadResult struct {
	path string
	data []byte
}

// readFileStream reads one regular file through the node's ReadFile stream. It
// validates the first-info rule, chunk size and order, and the actual byte
// count; the node's info size is only a snapshot and is never trusted for the
// bound. proceed is false only for caller cancellation.
func (s *filesService) readFileStream(ctx context.Context, client nodev1.NodeClient, sandboxID, canonical string) (fileReadResult, *apierr.Error, bool) {
	rpcCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := client.ReadFile(rpcCtx, &nodev1.ReadFileRequest{
		SandboxId: sandboxID,
		Path:      canonical,
		MaxBytes:  FileInlineLimitBytes,
	})
	if err != nil {
		if ctx.Err() != nil {
			return fileReadResult{}, nil, false
		}
		if status.Code(err) == codes.OutOfRange {
			return fileReadResult{}, s.readTooLarge(ctx, client, sandboxID, canonical), true
		}
		return fileReadResult{}, s.mapNodeError(ctx, fileOpRead, sandboxID, err), true
	}
	if stream == nil {
		return fileReadResult{}, s.readContractViolation(ctx, sandboxID, "nil_stream"), true
	}
	if ctx.Err() != nil {
		return fileReadResult{}, nil, false
	}
	var info *nodev1.FileInfo
	var data []byte
	for {
		if ctx.Err() != nil {
			return fileReadResult{}, nil, false
		}
		message, recvErr := stream.Recv()
		if ctx.Err() != nil {
			return fileReadResult{}, nil, false
		}
		if errors.Is(recvErr, io.EOF) {
			break
		}
		if recvErr != nil {
			if status.Code(recvErr) == codes.OutOfRange {
				return fileReadResult{}, s.readTooLarge(ctx, client, sandboxID, canonical), true
			}
			return fileReadResult{}, s.mapNodeError(ctx, fileOpRead, sandboxID, recvErr), true
		}
		if message == nil {
			return fileReadResult{}, s.readContractViolation(ctx, sandboxID, "nil_message"), true
		}
		switch part := message.GetPart().(type) {
		case *nodev1.ReadFileResponse_Info:
			if part == nil {
				return fileReadResult{}, s.readContractViolation(ctx, sandboxID, "typed_nil_info"), true
			}
			if info != nil || data != nil {
				return fileReadResult{}, s.readContractViolation(ctx, sandboxID, "late_info"), true
			}
			info = part.Info
			if info == nil {
				return fileReadResult{}, s.readContractViolation(ctx, sandboxID, "nil_info"), true
			}
			if _, err := FileEntryFromInfo(info); err != nil {
				return fileReadResult{}, s.readContractViolation(ctx, sandboxID, "metadata"), true
			}
			if info.GetPath() != "" && info.GetPath() != canonical {
				return fileReadResult{}, s.readContractViolation(ctx, sandboxID, "info_path"), true
			}
			if info.GetType() != nodev1.FileType_FILE_TYPE_REGULAR {
				return fileReadResult{}, fileInvalidPath(), true
			}
		case *nodev1.ReadFileResponse_Data:
			if part == nil {
				return fileReadResult{}, s.readContractViolation(ctx, sandboxID, "typed_nil_data"), true
			}
			if info == nil {
				return fileReadResult{}, s.readContractViolation(ctx, sandboxID, "data_before_info"), true
			}
			if len(part.Data) > FileChunkBytes {
				return fileReadResult{}, s.readContractViolation(ctx, sandboxID, "chunk_bounds"), true
			}
			if int64(len(data))+int64(len(part.Data)) > FileInlineLimitBytes {
				// The node should have refused this file before sending
				// data. Stop the stream instead of buffering past the
				// bound; the actual size is unknown.
				cancel()
				return fileReadResult{}, NewFileTooLargeError(FileInlineLimitBytes, 0, false), true
			}
			data = append(data, part.Data...)
		default:
			return fileReadResult{}, s.readContractViolation(ctx, sandboxID, "unknown_part"), true
		}
	}
	if info == nil {
		return fileReadResult{}, s.readContractViolation(ctx, sandboxID, "missing_info"), true
	}
	if ctx.Err() != nil {
		return fileReadResult{}, nil, false
	}
	return fileReadResult{path: canonical, data: data}, nil, true
}

// readTooLarge builds the pinned 413 with one best-effort StatFile for the
// size detail. Only a valid regular record for the same canonical path may
// supply sizeBytes; any other answer leaves it out. The native text is never
// logged.
func (s *filesService) readTooLarge(ctx context.Context, client nodev1.NodeClient, sandboxID, canonical string) *apierr.Error {
	if ctx.Err() != nil {
		return NewFileTooLargeError(FileInlineLimitBytes, 0, false)
	}
	info, err := client.StatFile(ctx, &nodev1.StatFileRequest{SandboxId: sandboxID, Path: canonical})
	if err != nil || info == nil {
		if err != nil && ctx.Err() == nil {
			s.logger.DebugContext(ctx, "api: file size lookup failed",
				"sandbox_id", sandboxID, "code", fileStatusCode(err))
		}
		return NewFileTooLargeError(FileInlineLimitBytes, 0, false)
	}
	if _, err := FileEntryFromInfo(info); err != nil ||
		info.GetPath() != canonical || info.GetType() != nodev1.FileType_FILE_TYPE_REGULAR {
		return NewFileTooLargeError(FileInlineLimitBytes, 0, false)
	}
	if info.GetSize() > FileInlineLimitBytes {
		return NewFileTooLargeError(FileInlineLimitBytes, info.GetSize(), true)
	}
	return NewFileTooLargeError(FileInlineLimitBytes, 0, false)
}

// readContractViolation reports a node that breaks the frozen ReadFile frame
// contract as an ambiguous node failure.
func (s *filesService) readContractViolation(ctx context.Context, sandboxID, reason string) *apierr.Error {
	s.logger.ErrorContext(ctx, "api: file read stream violates the node contract",
		"sandbox_id", sandboxID, "reason", reason)
	return fileDirectFailed(true)
}

// putFile sends one bounded file through the node's PutFile stream: the
// header first, then data messages of at most FileChunkBytes, then one final
// receive. It runs under a child RPC context that is canceled on every exit,
// including Send failures, even when the client's close function is a no-op.
//
// gRPC commonly reports a rejected stream as io.EOF from Send while the native
// final status is only available from CloseAndRecv. On that path the final
// status is fetched exactly once and mapped (so INVALID_ARGUMENT stays 400
// invalid_path); a successful final receive after a failed Send is a protocol
// failure, never a write success. The rest of the payload is never sent.
//
// The returned response must name the canonical path and the exact decoded
// size. proceed is false only for caller cancellation.
func (s *filesService) putFileStream(ctx context.Context, client nodev1.NodeClient, sandboxID, canonical string, data []byte) (*nodev1.PutFileResponse, *apierr.Error, bool) {
	rpcCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := client.PutFile(rpcCtx)
	if err != nil {
		if ctx.Err() != nil {
			return nil, nil, false
		}
		return nil, s.mapNodeError(ctx, fileOpWrite, sandboxID, err), true
	}
	if stream == nil {
		return nil, s.unaryContractViolation(ctx, fileOpWrite, sandboxID, "nil_stream"), true
	}
	if ctx.Err() != nil {
		return nil, nil, false
	}
	sendFailed := func(sendErr error) (*nodev1.PutFileResponse, *apierr.Error, bool) {
		if ctx.Err() != nil {
			return nil, nil, false
		}
		if errors.Is(sendErr, io.EOF) {
			_, finalErr := stream.CloseAndRecv()
			if finalErr != nil {
				if ctx.Err() != nil {
					return nil, nil, false
				}
				return nil, s.mapNodeError(ctx, fileOpWrite, sandboxID, finalErr), true
			}
			return nil, s.unaryContractViolation(ctx, fileOpWrite, sandboxID, "early_success"), true
		}
		return nil, s.mapNodeError(ctx, fileOpWrite, sandboxID, sendErr), true
	}
	header := &nodev1.PutFileRequest{
		Part: &nodev1.PutFileRequest_Header_{Header: &nodev1.PutFileRequest_Header{
			SandboxId: sandboxID,
			Path:      canonical,
		}},
	}
	if err := stream.Send(header); err != nil {
		return sendFailed(err)
	}
	for offset := 0; offset < len(data); offset += FileChunkBytes {
		if ctx.Err() != nil {
			return nil, nil, false
		}
		end := min(offset+FileChunkBytes, len(data))
		chunk := &nodev1.PutFileRequest{Part: &nodev1.PutFileRequest_Data{Data: data[offset:end]}}
		if err := stream.Send(chunk); err != nil {
			return sendFailed(err)
		}
	}
	if ctx.Err() != nil {
		return nil, nil, false
	}
	response, err := stream.CloseAndRecv()
	if err != nil {
		if ctx.Err() != nil {
			return nil, nil, false
		}
		return nil, s.mapNodeError(ctx, fileOpWrite, sandboxID, err), true
	}
	if ctx.Err() != nil {
		return nil, nil, false
	}
	if response == nil || response.GetPath() != canonical || response.GetSize() != int64(len(data)) {
		return nil, s.unaryContractViolation(ctx, fileOpWrite, sandboxID, "write_response"), true
	}
	return response, nil, true
}

// fileStatusCode returns a safe gRPC code label for logs.
func fileStatusCode(err error) string {
	if st, ok := status.FromError(err); ok {
		return st.Code().String()
	}
	return fmt.Sprintf("%T", err)
}

// fileErrorName classifies an error for logs without echoing its text.
func fileErrorName(err error) string {
	switch {
	case errors.Is(err, ErrCommandTargetNotFound):
		return "not_found"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	}
	return fileStatusCode(err)
}

// fileWriteError writes an error envelope unless the caller already went away.
func fileWriteError(w http.ResponseWriter, r *http.Request, apiErr *apierr.Error) {
	if r.Context().Err() != nil {
		return
	}
	apierr.WriteError(w, r, apiErr)
}

// fileRespondTarget writes a resolved target's error and reports whether the
// caller may continue. A canceled caller writes nothing.
func fileRespondTarget(w http.ResponseWriter, r *http.Request, apiErr *apierr.Error, proceed bool) bool {
	if !proceed {
		return false
	}
	if apiErr != nil {
		fileWriteError(w, r, apiErr)
		return false
	}
	if r.Context().Err() != nil {
		return false
	}
	return true
}

// fileNotFound is the owner-scoped missing-sandbox answer. It is the same for
// every tenant and every operation.
func fileNotFound() *apierr.Error {
	return apierr.New(http.StatusNotFound, apierr.CodeNotFound, "not found")
}

// fileUnauthorized is the pinned missing-identity answer.
func fileUnauthorized() *apierr.Error {
	return apierr.New(http.StatusUnauthorized, apierr.CodeUnauthorized, "unauthorized")
}

// fileInvalidPath is the generic bad-path answer.
func fileInvalidPath() *apierr.Error {
	return fileInvalidPathMessage("path must resolve under /home/user or /tmp")
}

// fileInvalidPathMessage is a pinned 400 invalid_path with a fixed message.
func fileInvalidPathMessage(message string) *apierr.Error {
	return apierr.New(http.StatusBadRequest, apierr.CodeInvalidPath, message)
}

// fileInvalidQuery is the pinned 400 invalid_query answer.
func fileInvalidQuery(param, reason string) *apierr.Error {
	return apierr.New(http.StatusBadRequest, apierr.CodeInvalidQuery,
		fmt.Sprintf("query parameter %q: %s", param, reason))
}

// fileInvalidJSON is the pinned 400 invalid_json answer.
func fileInvalidJSON(message string) *apierr.Error {
	return apierr.New(http.StatusBadRequest, apierr.CodeInvalidJSON, message)
}

// NewFileTooLargeError is the pinned 413 for a file above the inline limit.
// sizeBytes is present only when the file size is known; callers never invent
// it. It is exported for reuse by the signed-download receiver; that package
// does not use it yet.
func NewFileTooLargeError(maxBytes, sizeBytes int64, sizeKnown bool) *apierr.Error {
	apiErr := apierr.New(http.StatusRequestEntityTooLarge, apierr.CodeFileTooLarge,
		"file is larger than the inline limit").WithDetail("maxBytes", maxBytes)
	if sizeKnown {
		apiErr.WithDetail("sizeBytes", sizeBytes)
	}
	return apiErr
}

// fileMachineNotRunning is the pinned 409 for a stopped sandbox.
func fileMachineNotRunning(state SandboxState) *apierr.Error {
	apiErr := apierr.New(http.StatusConflict, apierr.CodeMachineNotRunning,
		"sandbox has no running machine").Retryable(false)
	if state != "" {
		apiErr.WithDetail("state", string(state))
	}
	return apiErr
}

// fileSandboxNotReady is the pinned 409 for a sandbox in a state that cannot
// take file calls.
func fileSandboxNotReady(state SandboxState) *apierr.Error {
	apiErr := apierr.New(http.StatusConflict, apierr.CodeSandboxNotReady,
		"sandbox is not ready").Retryable(false)
	if state != "" {
		apiErr.WithDetail("state", string(state))
	}
	return apiErr
}

// fileSandboxStarting is the retryable pinned 409 after a sandbox stayed
// starting for the readiness deadline.
func fileSandboxStarting(state SandboxState) *apierr.Error {
	apiErr := apierr.New(http.StatusConflict, apierr.CodeSandboxStarting,
		"sandbox is still starting").Retryable(true)
	if state != "" {
		apiErr.WithDetail("state", string(state))
	}
	return apiErr
}

// fileDirectFailed is the ambiguous node failure. Reads and listing are safe
// to repeat; mutations may already have taken effect.
func fileDirectFailed(retryable bool) *apierr.Error {
	return apierr.New(http.StatusBadGateway, apierr.CodeSandboxDirectFailed,
		"the sandbox machine could not complete the call").Retryable(retryable)
}

// fileInternal is the safe 500 for failures that are not the node's answer.
func fileInternal() *apierr.Error {
	return apierr.New(http.StatusInternalServerError, apierr.CodeInternal, "internal server error")
}
