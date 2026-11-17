package node

import (
	"context"
	"errors"
	"io"
	"path"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/proto/guestv1"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// MaxReadFileChunk is the largest ReadFile data message the node forwards,
// matching nodev1 and the guest's own limit.
const MaxReadFileChunk = 1 << 20

// --- Detached processes -------------------------------------------------------

// StartProcess starts a detached process in the guest, merging the VM
// environment under the request's values. The server chooses process_id; the
// guest owns the registry, idempotency and log paths.
func (m *Manager) StartProcess(ctx context.Context, req *nodev1.StartProcessRequest) (*nodev1.Process, error) {
	client, env, err := m.guestTarget(req.GetSandboxId())
	if err != nil {
		return nil, err
	}
	return client.StartProcess(ctx, &guestv1.StartProcessRequest{
		ProcessId: req.GetProcessId(),
		Command:   req.GetCommand(),
		Cwd:       req.GetCwd(),
		Env:       mergeEnv(env, req.GetEnv()),
	})
}

// GetProcess returns the guest's process record and one output window per
// stream. The optional request offsets keep their presence: nil selects a tail
// window instead of an offset.
func (m *Manager) GetProcess(ctx context.Context, req *nodev1.GetProcessRequest) (*nodev1.ProcessOutput, error) {
	client, err := m.ready(req.GetSandboxId())
	if err != nil {
		return nil, err
	}
	return client.GetProcess(ctx, &guestv1.GetProcessRequest{
		ProcessId:    req.GetProcessId(),
		StdoutOffset: req.StdoutOffset,
		StderrOffset: req.StderrOffset,
		LimitBytes:   req.GetLimitBytes(),
	})
}

// AttachProcess streams a detached process's output and validates the frozen
// frame order before forwarding it. Output starts exactly at the attached
// offsets and advances contiguously, one attached frame is followed by chunks
// and exactly one terminal EXITED or LOST record. Cancelling the RPC or a
// failed downstream send ends the guest stream too.
func (m *Manager) AttachProcess(req *nodev1.AttachProcessRequest, stream grpc.ServerStreamingServer[nodev1.ProcessEvent]) error {
	client, err := m.ready(req.GetSandboxId())
	if err != nil {
		return err
	}
	opCtx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	guest, err := client.AttachProcess(opCtx, &guestv1.AttachProcessRequest{
		ProcessId:    req.GetProcessId(),
		StdoutOffset: req.GetStdoutOffset(),
		StderrOffset: req.GetStderrOffset(),
	})
	if err != nil {
		return err
	}
	return forwardProcessEvents(guest, stream, req.GetProcessId())
}

// forwardProcessEvents relays one AttachProcess stream, rejecting malformed
// guest frames so a compromised sandbox cannot present an invalid stream as
// valid. The guest is sandbox-controlled, so no frame is forwarded before it
// is checked.
func forwardProcessEvents(src grpc.ServerStreamingClient[nodev1.ProcessEvent], dst grpc.ServerStreamingServer[nodev1.ProcessEvent], wantID uint32) error {
	attached, exited := false, false
	var stdoutNext, stderrNext int64
	for {
		ev, err := src.Recv()
		if errors.Is(err, io.EOF) {
			if !exited {
				return status.Error(codes.Internal, "guest protocol: process stream ended before exit")
			}
			return nil
		}
		if err != nil {
			if exited {
				// The terminal record already reached the caller; a transport
				// error afterwards cannot change the result.
				return nil
			}
			return err
		}
		if err := checkProcessFrame(ev, wantID, &attached, &exited, &stdoutNext, &stderrNext); err != nil {
			return err
		}
		if err := dst.Send(ev); err != nil {
			return err
		}
	}
}

// checkProcessFrame enforces nodev1's AttachProcess order, process identity
// and offset arithmetic. Offsets are checked nonnegative and nondecreasing
// before the subtraction, so a hostile next_offset cannot wrap around.
func checkProcessFrame(ev *nodev1.ProcessEvent, wantID uint32, attached, exited *bool, stdoutNext, stderrNext *int64) error {
	if ev == nil || ev.GetEvent() == nil {
		return status.Error(codes.Internal, "guest protocol: empty process event")
	}
	if *exited {
		return status.Error(codes.Internal, "guest protocol: process event after exit")
	}
	switch e := ev.GetEvent().(type) {
	case *nodev1.ProcessEvent_Attached_:
		if *attached {
			return status.Error(codes.Internal, "guest protocol: duplicate attached")
		}
		a := e.Attached
		p := a.GetProcess()
		if wantID == 0 || p == nil || p.GetProcessId() != wantID || !validProcessState(p.GetState()) {
			return status.Error(codes.Internal, "guest protocol: invalid attached process")
		}
		if a.GetStdoutOffset() < 0 || a.GetStderrOffset() < 0 {
			return status.Error(codes.Internal, "guest protocol: invalid attached offsets")
		}
		*attached = true
		*stdoutNext, *stderrNext = a.GetStdoutOffset(), a.GetStderrOffset()
	case *nodev1.ProcessEvent_Stdout:
		if !*attached {
			return status.Error(codes.Internal, "guest protocol: stdout before attached")
		}
		chunk := e.Stdout
		if chunk == nil || chunk.GetNextOffset() < 0 || chunk.GetNextOffset() < *stdoutNext ||
			chunk.GetNextOffset()-*stdoutNext != int64(len(chunk.GetData())) {
			return status.Error(codes.Internal, "guest protocol: non-contiguous stdout chunk")
		}
		*stdoutNext = chunk.GetNextOffset()
	case *nodev1.ProcessEvent_Stderr:
		if !*attached {
			return status.Error(codes.Internal, "guest protocol: stderr before attached")
		}
		chunk := e.Stderr
		if chunk == nil || chunk.GetNextOffset() < 0 || chunk.GetNextOffset() < *stderrNext ||
			chunk.GetNextOffset()-*stderrNext != int64(len(chunk.GetData())) {
			return status.Error(codes.Internal, "guest protocol: non-contiguous stderr chunk")
		}
		*stderrNext = chunk.GetNextOffset()
	case *nodev1.ProcessEvent_Exit:
		if !*attached {
			return status.Error(codes.Internal, "guest protocol: exit before attached")
		}
		p := e.Exit
		if p == nil || p.GetProcessId() != wantID {
			return status.Error(codes.Internal, "guest protocol: invalid terminal process")
		}
		switch p.GetState() {
		case nodev1.ProcessState_PROCESS_STATE_EXITED:
			exit := p.GetExit()
			if exit == nil || (exit.ExitCode == nil && exit.GetSignal() == "") {
				return status.Error(codes.Internal, "guest protocol: exited process without a status")
			}
		case nodev1.ProcessState_PROCESS_STATE_LOST:
			if p.GetExit() != nil {
				return status.Error(codes.Internal, "guest protocol: lost process with an exit status")
			}
		default:
			return status.Error(codes.Internal, "guest protocol: invalid terminal process")
		}
		*exited = true
	default:
		return status.Error(codes.Internal, "guest protocol: unknown process event")
	}
	return nil
}

// validProcessState reports whether s is a state the frozen enum defines for
// a process record. An unknown wire value from a guest is a protocol error.
func validProcessState(s nodev1.ProcessState) bool {
	switch s {
	case nodev1.ProcessState_PROCESS_STATE_RUNNING,
		nodev1.ProcessState_PROCESS_STATE_EXITED,
		nodev1.ProcessState_PROCESS_STATE_LOST:
		return true
	default:
		return false
	}
}

// KillProcess signals a detached process's group and returns its final record.
func (m *Manager) KillProcess(ctx context.Context, req *nodev1.KillProcessRequest) (*nodev1.KillProcessResponse, error) {
	client, err := m.ready(req.GetSandboxId())
	if err != nil {
		return nil, err
	}
	return client.KillProcess(ctx, &guestv1.KillProcessRequest{
		ProcessId:    req.GetProcessId(),
		GraceSeconds: req.GetGraceSeconds(),
	})
}

// --- Files --------------------------------------------------------------------

// ReadFile streams one guest file, checking that exactly one info message
// comes first and that data chunks stay within the contract's 1 MiB. An empty
// file is a valid info followed by EOF.
func (m *Manager) ReadFile(req *nodev1.ReadFileRequest, stream grpc.ServerStreamingServer[nodev1.ReadFileResponse]) error {
	client, err := m.ready(req.GetSandboxId())
	if err != nil {
		return err
	}
	opCtx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	guest, err := client.ReadFile(opCtx, &guestv1.ReadFileRequest{
		Path:     req.GetPath(),
		MaxBytes: req.GetMaxBytes(),
	})
	if err != nil {
		return err
	}
	info := false
	for {
		msg, err := guest.Recv()
		if errors.Is(err, io.EOF) {
			if !info {
				return status.Error(codes.Internal, "guest protocol: file stream ended before info")
			}
			return nil
		}
		if err != nil {
			return err
		}
		if err := checkReadFileFrame(msg, &info); err != nil {
			return err
		}
		if err := stream.Send(msg); err != nil {
			return err
		}
	}
}

// checkReadFileFrame enforces nodev1's ReadFile order, chunk bound and the
// metadata facts the contract fixes for a regular file: a canonical absolute
// path, a non-empty name, a regular type and a nonnegative size. Modes and
// timestamps are guest data, never validated here.
func checkReadFileFrame(msg *nodev1.ReadFileResponse, info *bool) error {
	if msg == nil || msg.GetPart() == nil {
		return status.Error(codes.Internal, "guest protocol: empty read file message")
	}
	switch part := msg.GetPart().(type) {
	case *nodev1.ReadFileResponse_Info:
		if *info {
			return status.Error(codes.Internal, "guest protocol: duplicate file info")
		}
		fi := part.Info
		if fi == nil || fi.GetType() != nodev1.FileType_FILE_TYPE_REGULAR ||
			fi.GetPath() == "" || !path.IsAbs(fi.GetPath()) || path.Clean(fi.GetPath()) != fi.GetPath() ||
			fi.GetName() == "" || fi.GetName() != path.Base(fi.GetPath()) ||
			fi.GetSize() < 0 {
			return status.Error(codes.Internal, "guest protocol: invalid file info")
		}
		*info = true
	case *nodev1.ReadFileResponse_Data:
		if !*info {
			return status.Error(codes.Internal, "guest protocol: file data before info")
		}
		if len(part.Data) > MaxReadFileChunk {
			return status.Errorf(codes.Internal, "guest protocol: file chunk of %d bytes exceeds %d", len(part.Data), MaxReadFileChunk)
		}
	default:
		return status.Error(codes.Internal, "guest protocol: unknown read file part")
	}
	return nil
}

// StatFile describes one guest path.
func (m *Manager) StatFile(ctx context.Context, req *nodev1.StatFileRequest) (*nodev1.FileInfo, error) {
	client, err := m.ready(req.GetSandboxId())
	if err != nil {
		return nil, err
	}
	return client.StatFile(ctx, &guestv1.StatFileRequest{Path: req.GetPath()})
}

// ListFiles lists one guest directory.
func (m *Manager) ListFiles(ctx context.Context, req *nodev1.ListFilesRequest) (*nodev1.ListFilesResponse, error) {
	client, err := m.ready(req.GetSandboxId())
	if err != nil {
		return nil, err
	}
	return client.ListFiles(ctx, &guestv1.ListFilesRequest{Path: req.GetPath(), Limit: req.GetLimit()})
}

// DeleteFile removes one guest path.
func (m *Manager) DeleteFile(ctx context.Context, req *nodev1.DeleteFileRequest) (*nodev1.DeleteFileResponse, error) {
	client, err := m.ready(req.GetSandboxId())
	if err != nil {
		return nil, err
	}
	return client.DeleteFile(ctx, &guestv1.DeleteFileRequest{Path: req.GetPath(), Recursive: req.GetRecursive()})
}

// MakeDir creates one guest directory.
func (m *Manager) MakeDir(ctx context.Context, req *nodev1.MakeDirRequest) (*nodev1.FileInfo, error) {
	client, err := m.ready(req.GetSandboxId())
	if err != nil {
		return nil, err
	}
	return client.MakeDir(ctx, &guestv1.MakeDirRequest{Path: req.GetPath(), Mode: req.GetMode()})
}

// MoveFile renames one guest path.
func (m *Manager) MoveFile(ctx context.Context, req *nodev1.MoveFileRequest) (*nodev1.MoveFileResponse, error) {
	client, err := m.ready(req.GetSandboxId())
	if err != nil {
		return nil, err
	}
	return client.MoveFile(ctx, &guestv1.MoveFileRequest{
		Source:      req.GetSource(),
		Destination: req.GetDestination(),
		Overwrite:   req.GetOverwrite(),
	})
}

// ChmodFile changes one guest path's permission bits.
func (m *Manager) ChmodFile(ctx context.Context, req *nodev1.ChmodFileRequest) (*nodev1.FileInfo, error) {
	client, err := m.ready(req.GetSandboxId())
	if err != nil {
		return nil, err
	}
	return client.ChmodFile(ctx, &guestv1.ChmodFileRequest{Path: req.GetPath(), Mode: req.GetMode()})
}
