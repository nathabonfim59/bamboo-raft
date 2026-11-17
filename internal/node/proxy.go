package node

import (
	"context"
	"errors"
	"io"
	"maps"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/proto/guestv1"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// MaxPutFileChunk is the largest PutFile data message the node accepts and
// forwards. The total upload is unbounded.
const MaxPutFileChunk = 1 << 20

// Exec proxies one command to the guest and forwards its event stream
// unchanged: started, stdout/stderr in write order, then exit. The guest owns
// process-group cancellation and timeouts.
func (m *Manager) Exec(req *nodev1.ExecRequest, stream grpc.ServerStreamingServer[nodev1.ExecEvent]) error {
	client, env, err := m.guestTarget(req.GetSandboxId())
	if err != nil {
		return err
	}
	// A child context lets an early downstream Send failure cancel the guest
	// command immediately instead of leaving it running to its timeout.
	opCtx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	guest, err := client.Exec(opCtx, &guestv1.ExecRequest{
		Command:        req.GetCommand(),
		Cwd:            req.GetCwd(),
		Env:            mergeEnv(env, req.GetEnv()),
		TimeoutSeconds: req.GetTimeoutSeconds(),
	})
	if err != nil {
		return err
	}
	// Validate the frozen frame order while forwarding, with constant
	// memory: exactly one started, any stdout/stderr after it, then exactly
	// one exit. The guest is sandbox-controlled, so a malformed stream must
	// never reach the server as if it were valid.
	started, exited := false, false
	for {
		ev, err := guest.Recv()
		if errors.Is(err, io.EOF) {
			if !exited {
				return status.Error(codes.Internal, "guest protocol: event stream ended before exit")
			}
			return nil
		}
		if err != nil {
			if exited {
				// The terminal exit already reached the caller; a guest
				// transport error afterwards cannot change the result.
				return nil
			}
			return err
		}
		if err := checkExecFrame(ev, &started, &exited); err != nil {
			return err
		}
		if err := stream.Send(ev); err != nil {
			return err
		}
	}
}

// checkExecFrame enforces nodev1's Exec order. A malformed frame is never
// forwarded; the caller cancels the guest operation on the returned error.
func checkExecFrame(ev *nodev1.ExecEvent, started, exited *bool) error {
	if ev == nil || ev.GetEvent() == nil {
		return status.Error(codes.Internal, "guest protocol: empty exec event")
	}
	if *exited {
		return status.Error(codes.Internal, "guest protocol: event after exit")
	}
	switch ev.GetEvent().(type) {
	case *nodev1.ExecEvent_Started_:
		if *started {
			return status.Error(codes.Internal, "guest protocol: duplicate started")
		}
		*started = true
	case *nodev1.ExecEvent_Stdout, *nodev1.ExecEvent_Stderr:
		if !*started {
			return status.Error(codes.Internal, "guest protocol: output before started")
		}
	case *nodev1.ExecEvent_Exit_:
		if !*started {
			return status.Error(codes.Internal, "guest protocol: exit before started")
		}
		*exited = true
	default:
		return status.Error(codes.Internal, "guest protocol: unknown exec event")
	}
	return nil
}

// guestTarget returns the guest client and a snapshot of the VM environment
// for one command. A VM being deleted is NotFound, as lookup and ready report
// it; a VM that exists but cannot serve commands is FAILED_PRECONDITION.
func (m *Manager) guestTarget(id string) (guestv1.GuestClient, map[string]string, error) {
	v, err := m.lookup(id)
	if err != nil {
		return nil, nil, err
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.deleting {
		return nil, nil, status.Errorf(codes.NotFound, "sandbox %s not found", id)
	}
	if v.state != nodev1.VMState_VM_STATE_READY || v.client == nil {
		return nil, nil, status.Errorf(codes.FailedPrecondition, "sandbox %s is not ready", id)
	}
	return v.client, maps.Clone(v.meta.Env), nil
}

// mergeEnv combines the VM environment with a request's environment. Request
// values win. HOME is dropped so neither the VM nor the command can point it
// outside the guest's base home.
func mergeEnv(vmEnv, requestEnv map[string]string) map[string]string {
	if len(vmEnv) == 0 && len(requestEnv) == 0 {
		return nil
	}
	out := maps.Clone(vmEnv)
	if out == nil {
		out = make(map[string]string, len(requestEnv))
	}
	maps.Copy(out, requestEnv)
	delete(out, "HOME")
	if len(out) == 0 {
		return nil
	}
	return out
}

// PutFile proxies a client upload to the guest. The node consumes the first
// message as the header and forwards only data chunks of at most
// MaxPutFileChunk. Path validation and the atomic replace belong to the guest.
func (m *Manager) PutFile(stream grpc.ClientStreamingServer[nodev1.PutFileRequest, nodev1.PutFileResponse]) error {
	first, err := stream.Recv()
	if errors.Is(err, io.EOF) {
		return status.Error(codes.InvalidArgument, "put file: missing header")
	}
	if err != nil {
		return err
	}
	header := first.GetHeader()
	if header == nil {
		return status.Error(codes.InvalidArgument, "put file: first message must be the header")
	}
	client, err := m.ready(header.GetSandboxId())
	if err != nil {
		return err
	}
	opCtx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	guest, err := client.PutFile(opCtx)
	if err != nil {
		return err
	}
	if err := guest.Send(&guestv1.PutFileRequest{
		Part: &guestv1.PutFileRequest_Header_{Header: &guestv1.PutFileRequest_Header{
			Path: header.GetPath(),
			Mode: header.GetMode(),
		}},
	}); err != nil {
		return err
	}
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		if msg.GetHeader() != nil {
			return status.Error(codes.InvalidArgument, "put file: duplicate header")
		}
		if msg.GetPart() == nil {
			return status.Error(codes.InvalidArgument, "put file: message has neither header nor data")
		}
		data := msg.GetData()
		if len(data) > MaxPutFileChunk {
			return status.Errorf(codes.InvalidArgument,
				"put file: chunk of %d bytes exceeds %d", len(data), MaxPutFileChunk)
		}
		if err := guest.Send(&guestv1.PutFileRequest{Part: &guestv1.PutFileRequest_Data{Data: data}}); err != nil {
			return err
		}
	}
	resp, err := guest.CloseAndRecv()
	if err != nil {
		return err
	}
	return stream.SendAndClose(resp)
}
