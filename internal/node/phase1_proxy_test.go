package node

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"net"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"

	"github.com/nathabonfim59/bamboo-raft/internal/ids"
	"github.com/nathabonfim59/bamboo-raft/internal/nodelink"
	"github.com/nathabonfim59/bamboo-raft/internal/nodetest"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/guestv1"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// phase1Guest is a scriptable guestv1.GuestServer. A nil operation is
// Unimplemented, which keeps each test focused on one RPC family.
type phase1Guest struct {
	guestv1.UnimplementedGuestServer
	startProcess  func(context.Context, *guestv1.StartProcessRequest) (*nodev1.Process, error)
	getProcess    func(context.Context, *guestv1.GetProcessRequest) (*nodev1.ProcessOutput, error)
	attachProcess func(*guestv1.AttachProcessRequest, grpc.ServerStreamingServer[nodev1.ProcessEvent]) error
	killProcess   func(context.Context, *guestv1.KillProcessRequest) (*nodev1.KillProcessResponse, error)
	readFile      func(*guestv1.ReadFileRequest, grpc.ServerStreamingServer[nodev1.ReadFileResponse]) error
	statFile      func(context.Context, *guestv1.StatFileRequest) (*nodev1.FileInfo, error)
	listFiles     func(context.Context, *guestv1.ListFilesRequest) (*nodev1.ListFilesResponse, error)
	deleteFile    func(context.Context, *guestv1.DeleteFileRequest) (*nodev1.DeleteFileResponse, error)
	makeDir       func(context.Context, *guestv1.MakeDirRequest) (*nodev1.FileInfo, error)
	moveFile      func(context.Context, *guestv1.MoveFileRequest) (*nodev1.MoveFileResponse, error)
	chmodFile     func(context.Context, *guestv1.ChmodFileRequest) (*nodev1.FileInfo, error)
}

func (g *phase1Guest) Ping(context.Context, *guestv1.PingRequest) (*guestv1.PingResponse, error) {
	return &guestv1.PingResponse{Version: "phase1"}, nil
}

func (g *phase1Guest) StartProcess(ctx context.Context, req *guestv1.StartProcessRequest) (*nodev1.Process, error) {
	if g.startProcess == nil {
		return nil, status.Error(codes.Unimplemented, "StartProcess")
	}
	return g.startProcess(ctx, req)
}

func (g *phase1Guest) GetProcess(ctx context.Context, req *guestv1.GetProcessRequest) (*nodev1.ProcessOutput, error) {
	if g.getProcess == nil {
		return nil, status.Error(codes.Unimplemented, "GetProcess")
	}
	return g.getProcess(ctx, req)
}

func (g *phase1Guest) AttachProcess(req *guestv1.AttachProcessRequest, stream grpc.ServerStreamingServer[nodev1.ProcessEvent]) error {
	if g.attachProcess == nil {
		return status.Error(codes.Unimplemented, "AttachProcess")
	}
	return g.attachProcess(req, stream)
}

func (g *phase1Guest) KillProcess(ctx context.Context, req *guestv1.KillProcessRequest) (*nodev1.KillProcessResponse, error) {
	if g.killProcess == nil {
		return nil, status.Error(codes.Unimplemented, "KillProcess")
	}
	return g.killProcess(ctx, req)
}

func (g *phase1Guest) ReadFile(req *guestv1.ReadFileRequest, stream grpc.ServerStreamingServer[nodev1.ReadFileResponse]) error {
	if g.readFile == nil {
		return status.Error(codes.Unimplemented, "ReadFile")
	}
	return g.readFile(req, stream)
}

func (g *phase1Guest) StatFile(ctx context.Context, req *guestv1.StatFileRequest) (*nodev1.FileInfo, error) {
	if g.statFile == nil {
		return nil, status.Error(codes.Unimplemented, "StatFile")
	}
	return g.statFile(ctx, req)
}

func (g *phase1Guest) ListFiles(ctx context.Context, req *guestv1.ListFilesRequest) (*nodev1.ListFilesResponse, error) {
	if g.listFiles == nil {
		return nil, status.Error(codes.Unimplemented, "ListFiles")
	}
	return g.listFiles(ctx, req)
}

func (g *phase1Guest) DeleteFile(ctx context.Context, req *guestv1.DeleteFileRequest) (*nodev1.DeleteFileResponse, error) {
	if g.deleteFile == nil {
		return nil, status.Error(codes.Unimplemented, "DeleteFile")
	}
	return g.deleteFile(ctx, req)
}

func (g *phase1Guest) MakeDir(ctx context.Context, req *guestv1.MakeDirRequest) (*nodev1.FileInfo, error) {
	if g.makeDir == nil {
		return nil, status.Error(codes.Unimplemented, "MakeDir")
	}
	return g.makeDir(ctx, req)
}

func (g *phase1Guest) MoveFile(ctx context.Context, req *guestv1.MoveFileRequest) (*nodev1.MoveFileResponse, error) {
	if g.moveFile == nil {
		return nil, status.Error(codes.Unimplemented, "MoveFile")
	}
	return g.moveFile(ctx, req)
}

func (g *phase1Guest) ChmodFile(ctx context.Context, req *guestv1.ChmodFileRequest) (*nodev1.FileInfo, error) {
	if g.chmodFile == nil {
		return nil, status.Error(codes.Unimplemented, "ChmodFile")
	}
	return g.chmodFile(ctx, req)
}

// phase1Harness boots one VM whose guest is g (nil: the real internal/guest
// Service) with env set for every command.
func phase1Harness(t *testing.T, g guestv1.GuestServer, env map[string]string) (*harness, string) {
	t.Helper()
	h := newHarness(t, harnessOptions{})
	if g != nil {
		useGuest(h, g)
	}
	id := ids.Sandbox()
	if _, err := h.mg.CreateVM(t.Context(), &nodev1.CreateVMRequest{
		SandboxId: id,
		Resources: &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: defaultTestBaseGiB},
		Env:       env,
	}); err != nil {
		t.Fatalf("CreateVM: %v", err)
	}
	return h, id
}

func TestPhase1StartProcessEnvMerge(t *testing.T) {
	cases := []struct {
		name string
		vm   map[string]string
		req  map[string]string
		want map[string]string
	}{
		{"request wins and HOME dropped",
			map[string]string{"A": "vm", "B": "vm", "HOME": "/vm"},
			map[string]string{"B": "req", "C": "req", "HOME": "/req"},
			map[string]string{"A": "vm", "B": "req", "C": "req"}},
		{"vm only", map[string]string{"A": "vm"}, nil, map[string]string{"A": "vm"}},
		{"request only", nil, map[string]string{"B": "req"}, map[string]string{"B": "req"}},
		{"both empty", nil, nil, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var got *guestv1.StartProcessRequest
			g := &phase1Guest{startProcess: func(_ context.Context, req *guestv1.StartProcessRequest) (*nodev1.Process, error) {
				got = req
				return &nodev1.Process{
					ProcessId: req.GetProcessId(), Pid: 99, State: nodev1.ProcessState_PROCESS_STATE_RUNNING,
					Command: req.GetCommand(), Cwd: req.GetCwd(),
				}, nil
			}}
			h, id := phase1Harness(t, g, tc.vm)
			client := startNodeClient(t, h)
			resp, err := client.StartProcess(t.Context(), &nodev1.StartProcessRequest{
				SandboxId: id, ProcessId: 7, Command: "echo hi", Cwd: "/tmp", Env: tc.req,
			})
			if err != nil {
				t.Fatalf("StartProcess: %v", err)
			}
			if !maps.Equal(got.GetEnv(), tc.want) {
				t.Fatalf("guest env = %v, want %v", got.GetEnv(), tc.want)
			}
			if tc.want == nil && got.GetEnv() != nil {
				t.Fatalf("guest env = %v, want nil for an empty merge", got.GetEnv())
			}
			if got.GetProcessId() != 7 || got.GetCommand() != "echo hi" || got.GetCwd() != "/tmp" {
				t.Fatalf("guest request = %v", got)
			}
			if resp.GetProcessId() != 7 || resp.GetPid() != 99 || resp.GetCommand() != "echo hi" || resp.GetCwd() != "/tmp" {
				t.Fatalf("response = %v", resp)
			}
			v, err := h.mg.lookup(id)
			if err != nil {
				t.Fatal(err)
			}
			v.mu.Lock()
			vmEnv := maps.Clone(v.meta.Env)
			v.mu.Unlock()
			if !maps.Equal(vmEnv, tc.vm) {
				t.Fatalf("VM env changed to %v, want %v", vmEnv, tc.vm)
			}
		})
	}
}

func TestPhase1GetProcessPreservesOffsets(t *testing.T) {
	var got *guestv1.GetProcessRequest
	g := &phase1Guest{getProcess: func(_ context.Context, req *guestv1.GetProcessRequest) (*nodev1.ProcessOutput, error) {
		got = req
		return &nodev1.ProcessOutput{
			Process:      &nodev1.Process{ProcessId: req.GetProcessId(), State: nodev1.ProcessState_PROCESS_STATE_EXITED},
			Stdout:       []byte("out"),
			Stderr:       []byte("err"),
			StdoutOffset: 3,
			StderrOffset: 1,
		}, nil
	}}
	h, id := phase1Harness(t, g, nil)
	client := startNodeClient(t, h)

	cases := []struct {
		name           string
		stdout, stderr *int64
		limit          uint32
	}{
		{"tail window", nil, nil, 0},
		{"explicit zero offsets", new(int64(0)), new(int64(0)), 4},
		{"stdout offset only", new(int64(5)), nil, 8},
		{"both offsets", new(int64(5)), new(int64(2)), 8 << 20},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got = nil
			out, err := client.GetProcess(t.Context(), &nodev1.GetProcessRequest{
				SandboxId: id, ProcessId: 3, StdoutOffset: tc.stdout, StderrOffset: tc.stderr, LimitBytes: tc.limit,
			})
			if err != nil {
				t.Fatalf("GetProcess: %v", err)
			}
			if (got.StdoutOffset == nil) != (tc.stdout == nil) ||
				(got.StdoutOffset != nil && *got.StdoutOffset != *tc.stdout) {
				t.Fatalf("guest stdout offset = %v, want presence/value %v", got.StdoutOffset, tc.stdout)
			}
			if (got.StderrOffset == nil) != (tc.stderr == nil) ||
				(got.StderrOffset != nil && *got.StderrOffset != *tc.stderr) {
				t.Fatalf("guest stderr offset = %v, want presence/value %v", got.StderrOffset, tc.stderr)
			}
			if got.GetProcessId() != 3 || got.GetLimitBytes() != tc.limit {
				t.Fatalf("guest request = %v", got)
			}
			if string(out.GetStdout()) != "out" || string(out.GetStderr()) != "err" ||
				out.GetStdoutOffset() != 3 || out.GetStderrOffset() != 1 ||
				out.GetProcess().GetState() != nodev1.ProcessState_PROCESS_STATE_EXITED {
				t.Fatalf("response = %v", out)
			}
		})
	}
}

func TestPhase1KillProcessForwarding(t *testing.T) {
	var got *guestv1.KillProcessRequest
	g := &phase1Guest{killProcess: func(_ context.Context, req *guestv1.KillProcessRequest) (*nodev1.KillProcessResponse, error) {
		got = req
		return &nodev1.KillProcessResponse{
			Process:  &nodev1.Process{ProcessId: req.GetProcessId(), State: nodev1.ProcessState_PROCESS_STATE_EXITED},
			Signaled: true,
		}, nil
	}}
	h, id := phase1Harness(t, g, nil)
	client := startNodeClient(t, h)
	resp, err := client.KillProcess(t.Context(), &nodev1.KillProcessRequest{SandboxId: id, ProcessId: 11, GraceSeconds: 3})
	if err != nil {
		t.Fatalf("KillProcess: %v", err)
	}
	if got.GetProcessId() != 11 || got.GetGraceSeconds() != 3 {
		t.Fatalf("guest request = %v", got)
	}
	if !resp.GetSignaled() || resp.GetProcess().GetProcessId() != 11 {
		t.Fatalf("response = %v", resp)
	}
}

// fileGuestCalls records the fields the node forwards to the guest.
type fileGuestCalls struct {
	statPath   string
	listPath   string
	listLimit  uint32
	deletePath string
	deleteRec  bool
	mkdirPath  string
	mkdirMode  uint32
	moveSrc    string
	moveDst    string
	moveOver   bool
	chmodPath  string
	chmodMode  uint32
	readPath   string
	readMax    int64
}

func TestPhase1FileForwarding(t *testing.T) {
	calls := &fileGuestCalls{}
	g := &phase1Guest{
		statFile: func(_ context.Context, req *guestv1.StatFileRequest) (*nodev1.FileInfo, error) {
			calls.statPath = req.GetPath()
			return &nodev1.FileInfo{Path: "/home/user/stat", Name: "stat", Type: nodev1.FileType_FILE_TYPE_REGULAR}, nil
		},
		listFiles: func(_ context.Context, req *guestv1.ListFilesRequest) (*nodev1.ListFilesResponse, error) {
			calls.listPath, calls.listLimit = req.GetPath(), req.GetLimit()
			return &nodev1.ListFilesResponse{Path: "/home/user/dir", Entries: []*nodev1.FileInfo{{Name: "e"}}, Truncated: true}, nil
		},
		deleteFile: func(_ context.Context, req *guestv1.DeleteFileRequest) (*nodev1.DeleteFileResponse, error) {
			calls.deletePath, calls.deleteRec = req.GetPath(), req.GetRecursive()
			return &nodev1.DeleteFileResponse{Path: "/home/user/deleted"}, nil
		},
		makeDir: func(_ context.Context, req *guestv1.MakeDirRequest) (*nodev1.FileInfo, error) {
			calls.mkdirPath, calls.mkdirMode = req.GetPath(), req.GetMode()
			return &nodev1.FileInfo{Path: "/home/user/new", Type: nodev1.FileType_FILE_TYPE_DIRECTORY, Mode: req.GetMode()}, nil
		},
		moveFile: func(_ context.Context, req *guestv1.MoveFileRequest) (*nodev1.MoveFileResponse, error) {
			calls.moveSrc, calls.moveDst, calls.moveOver = req.GetSource(), req.GetDestination(), req.GetOverwrite()
			return &nodev1.MoveFileResponse{Source: "/home/user/src", Destination: "/tmp/dst"}, nil
		},
		chmodFile: func(_ context.Context, req *guestv1.ChmodFileRequest) (*nodev1.FileInfo, error) {
			calls.chmodPath, calls.chmodMode = req.GetPath(), req.GetMode()
			return &nodev1.FileInfo{Path: "/home/user/mode", Mode: req.GetMode()}, nil
		},
		readFile: func(req *guestv1.ReadFileRequest, stream grpc.ServerStreamingServer[nodev1.ReadFileResponse]) error {
			calls.readPath, calls.readMax = req.GetPath(), req.GetMaxBytes()
			if err := stream.Send(&nodev1.ReadFileResponse{Part: &nodev1.ReadFileResponse_Info{
				Info: &nodev1.FileInfo{Path: "/home/user/read", Name: "read", Type: nodev1.FileType_FILE_TYPE_REGULAR, Size: 3},
			}}); err != nil {
				return err
			}
			return stream.Send(&nodev1.ReadFileResponse{Part: &nodev1.ReadFileResponse_Data{Data: []byte("abc")}})
		},
	}
	h, id := phase1Harness(t, g, nil)
	client := startNodeClient(t, h)
	ctx := t.Context()

	info, err := client.StatFile(ctx, &nodev1.StatFileRequest{SandboxId: id, Path: "stat"})
	if err != nil || calls.statPath != "stat" || info.GetPath() != "/home/user/stat" {
		t.Fatalf("StatFile = %v, %v; guest path %q", info, err, calls.statPath)
	}
	list, err := client.ListFiles(ctx, &nodev1.ListFilesRequest{SandboxId: id, Path: "dir", Limit: 7})
	if err != nil || calls.listPath != "dir" || calls.listLimit != 7 ||
		list.GetPath() != "/home/user/dir" || !list.GetTruncated() {
		t.Fatalf("ListFiles = %v, %v; guest %q/%d", list, err, calls.listPath, calls.listLimit)
	}
	del, err := client.DeleteFile(ctx, &nodev1.DeleteFileRequest{SandboxId: id, Path: "gone", Recursive: true})
	if err != nil || calls.deletePath != "gone" || !calls.deleteRec || del.GetPath() != "/home/user/deleted" {
		t.Fatalf("DeleteFile = %v, %v; guest %q/%t", del, err, calls.deletePath, calls.deleteRec)
	}
	md, err := client.MakeDir(ctx, &nodev1.MakeDirRequest{SandboxId: id, Path: "new", Mode: 0o700})
	if err != nil || calls.mkdirPath != "new" || calls.mkdirMode != 0o700 || md.GetMode() != 0o700 {
		t.Fatalf("MakeDir = %v, %v; guest %q/%o", md, err, calls.mkdirPath, calls.mkdirMode)
	}
	mv, err := client.MoveFile(ctx, &nodev1.MoveFileRequest{
		SandboxId: id, Source: "src", Destination: "/tmp/dst", Overwrite: true,
	})
	if err != nil || calls.moveSrc != "src" || calls.moveDst != "/tmp/dst" || !calls.moveOver ||
		mv.GetDestination() != "/tmp/dst" {
		t.Fatalf("MoveFile = %v, %v; guest %q->%q/%t", mv, err, calls.moveSrc, calls.moveDst, calls.moveOver)
	}
	ch, err := client.ChmodFile(ctx, &nodev1.ChmodFileRequest{SandboxId: id, Path: "mode", Mode: 0o1600})
	if err != nil || calls.chmodPath != "mode" || calls.chmodMode != 0o1600 || ch.GetMode() != 0o1600 {
		t.Fatalf("ChmodFile = %v, %v; guest %q/%o", ch, err, calls.chmodPath, calls.chmodMode)
	}
	rf, data, err := nodetest.ReadFile(ctx, client, &nodev1.ReadFileRequest{SandboxId: id, Path: "read", MaxBytes: 12})
	if err != nil || calls.readPath != "read" || calls.readMax != 12 || rf.GetPath() != "/home/user/read" || string(data) != "abc" {
		t.Fatalf("ReadFile = %v %q, %v; guest %q/%d", rf, data, err, calls.readPath, calls.readMax)
	}
}

// phase1GateCalls returns one call per Phase 1 proxy with a payload the guest
// would reject. A state gate must win over that payload error.
func phase1GateCalls(id string) []struct {
	name string
	run  func(context.Context, nodev1.NodeClient) error
} {
	return []struct {
		name string
		run  func(context.Context, nodev1.NodeClient) error
	}{
		{"StartProcess", func(ctx context.Context, c nodev1.NodeClient) error {
			_, err := c.StartProcess(ctx, &nodev1.StartProcessRequest{SandboxId: id, ProcessId: 0, Command: "true", Cwd: "/etc"})
			return err
		}},
		{"GetProcess", func(ctx context.Context, c nodev1.NodeClient) error {
			_, err := c.GetProcess(ctx, &nodev1.GetProcessRequest{SandboxId: id, ProcessId: 0, LimitBytes: 8<<20 + 1})
			return err
		}},
		{"AttachProcess", func(ctx context.Context, c nodev1.NodeClient) error {
			st, err := c.AttachProcess(ctx, &nodev1.AttachProcessRequest{SandboxId: id, ProcessId: 0})
			return firstRecvErr(st, err)
		}},
		{"KillProcess", func(ctx context.Context, c nodev1.NodeClient) error {
			_, err := c.KillProcess(ctx, &nodev1.KillProcessRequest{SandboxId: id, ProcessId: 0})
			return err
		}},
		{"ReadFile", func(ctx context.Context, c nodev1.NodeClient) error {
			st, err := c.ReadFile(ctx, &nodev1.ReadFileRequest{SandboxId: id, Path: "/etc/passwd", MaxBytes: -1})
			return firstRecvErr(st, err)
		}},
		{"StatFile", func(ctx context.Context, c nodev1.NodeClient) error {
			_, err := c.StatFile(ctx, &nodev1.StatFileRequest{SandboxId: id, Path: "/etc/passwd"})
			return err
		}},
		{"ListFiles", func(ctx context.Context, c nodev1.NodeClient) error {
			_, err := c.ListFiles(ctx, &nodev1.ListFilesRequest{SandboxId: id, Path: "/etc", Limit: 10001})
			return err
		}},
		{"DeleteFile", func(ctx context.Context, c nodev1.NodeClient) error {
			_, err := c.DeleteFile(ctx, &nodev1.DeleteFileRequest{SandboxId: id, Path: "/home/user", Recursive: true})
			return err
		}},
		{"MakeDir", func(ctx context.Context, c nodev1.NodeClient) error {
			_, err := c.MakeDir(ctx, &nodev1.MakeDirRequest{SandboxId: id, Path: "/etc/x"})
			return err
		}},
		{"MoveFile", func(ctx context.Context, c nodev1.NodeClient) error {
			_, err := c.MoveFile(ctx, &nodev1.MoveFileRequest{SandboxId: id, Source: "/etc/passwd", Destination: "/tmp/x"})
			return err
		}},
		{"ChmodFile", func(ctx context.Context, c nodev1.NodeClient) error {
			_, err := c.ChmodFile(ctx, &nodev1.ChmodFileRequest{SandboxId: id, Path: "/etc/passwd", Mode: 0o10000})
			return err
		}},
	}
}

// firstRecvErr surfaces a server-streaming RPC's status: stream creation
// normally succeeds and the first Recv carries it.
func firstRecvErr[T any](st grpc.ServerStreamingClient[T], err error) error {
	if err != nil {
		return err
	}
	_, err = st.Recv()
	if errors.Is(err, io.EOF) {
		return status.Error(codes.Internal, "stream ended without an error")
	}
	return err
}

func TestPhase1ProxyGates(t *testing.T) {
	t.Run("unknown", func(t *testing.T) {
		h := newHarness(t, harnessOptions{})
		client := startNodeClient(t, h)
		for _, call := range phase1GateCalls(ids.Sandbox()) {
			t.Run(call.name, func(t *testing.T) {
				if err := call.run(t.Context(), client); status.Code(err) != codes.NotFound {
					t.Fatalf("%s = %v, want NotFound", call.name, err)
				}
			})
		}
	})
	t.Run("stopped", func(t *testing.T) {
		h, id := phase1Harness(t, &phase1Guest{}, nil)
		if _, err := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1}); err != nil {
			t.Fatalf("StopVM: %v", err)
		}
		client := startNodeClient(t, h)
		for _, call := range phase1GateCalls(id) {
			t.Run(call.name, func(t *testing.T) {
				if err := call.run(t.Context(), client); status.Code(err) != codes.FailedPrecondition {
					t.Fatalf("%s = %v, want FailedPrecondition", call.name, err)
				}
			})
		}
	})
	t.Run("deleting", func(t *testing.T) {
		h, id := phase1Harness(t, &phase1Guest{}, nil)
		v, err := h.mg.lookup(id)
		if err != nil {
			t.Fatal(err)
		}
		v.mu.Lock()
		v.deleting = true
		v.mu.Unlock()
		client := startNodeClient(t, h)
		for _, call := range phase1GateCalls(id) {
			t.Run(call.name, func(t *testing.T) {
				if err := call.run(t.Context(), client); status.Code(err) != codes.NotFound {
					t.Fatalf("%s = %v, want NotFound", call.name, err)
				}
			})
		}
	})
}

func TestPhase1ConcurrentDelete(t *testing.T) {
	entered := make(chan struct{})
	var once sync.Once
	g := &phase1Guest{getProcess: func(ctx context.Context, _ *guestv1.GetProcessRequest) (*nodev1.ProcessOutput, error) {
		once.Do(func() { close(entered) })
		<-ctx.Done()
		return nil, status.FromContextError(ctx.Err()).Err()
	}}
	h, id := phase1Harness(t, g, nil)
	client := startNodeClient(t, h)
	errCh := make(chan error, 1)
	go func() {
		_, err := client.GetProcess(context.Background(), &nodev1.GetProcessRequest{SandboxId: id, ProcessId: 1})
		errCh <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("guest never received the proxied GetProcess")
	}
	h.delete(id)
	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("GetProcess succeeded across a DeleteVM")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("in-flight GetProcess did not return after DeleteVM")
	}
	_, err := client.GetProcess(t.Context(), &nodev1.GetProcessRequest{SandboxId: id, ProcessId: 1})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("GetProcess after delete = %v, want NotFound", err)
	}
}

// --- Stream frame validation ---------------------------------------------------

func attachedFrame(stdoutOffset, stderrOffset int64) *nodev1.ProcessEvent {
	return attachedFrameID(1, stdoutOffset, stderrOffset)
}

func attachedFrameID(id uint32, stdoutOffset, stderrOffset int64) *nodev1.ProcessEvent {
	return attachedFrameState(id, nodev1.ProcessState_PROCESS_STATE_RUNNING, stdoutOffset, stderrOffset)
}

func attachedFrameState(id uint32, state nodev1.ProcessState, stdoutOffset, stderrOffset int64) *nodev1.ProcessEvent {
	return &nodev1.ProcessEvent{Event: &nodev1.ProcessEvent_Attached_{Attached: &nodev1.ProcessEvent_Attached{
		Process:      &nodev1.Process{ProcessId: id, State: state},
		StdoutOffset: stdoutOffset,
		StderrOffset: stderrOffset,
	}}}
}

// attachedWithoutProcessFrame has valid offsets but no process record, which
// the frozen Attached message requires.
func attachedWithoutProcessFrame() *nodev1.ProcessEvent {
	return &nodev1.ProcessEvent{Event: &nodev1.ProcessEvent_Attached_{Attached: &nodev1.ProcessEvent_Attached{}}}
}

func stdoutFrame(next int64, data string) *nodev1.ProcessEvent {
	return &nodev1.ProcessEvent{Event: &nodev1.ProcessEvent_Stdout{Stdout: &nodev1.ProcessEvent_Chunk{
		Data: []byte(data), NextOffset: next,
	}}}
}

func stderrFrame(next int64, data string) *nodev1.ProcessEvent {
	return &nodev1.ProcessEvent{Event: &nodev1.ProcessEvent_Stderr{Stderr: &nodev1.ProcessEvent_Chunk{
		Data: []byte(data), NextOffset: next,
	}}}
}

func exitFrame(state nodev1.ProcessState) *nodev1.ProcessEvent { return exitFrameID(1, state) }

func exitFrameID(id uint32, state nodev1.ProcessState) *nodev1.ProcessEvent {
	p := &nodev1.Process{ProcessId: id, State: state}
	if state == nodev1.ProcessState_PROCESS_STATE_EXITED {
		p.Exit = &nodev1.ExecEvent_Exit{ExitCode: new(int32(0))}
	}
	return &nodev1.ProcessEvent{Event: &nodev1.ProcessEvent_Exit{Exit: p}}
}

// exitedWithoutStatusFrame is an EXITED record with no ExecEvent.Exit.
func exitedWithoutStatusFrame() *nodev1.ProcessEvent {
	return &nodev1.ProcessEvent{Event: &nodev1.ProcessEvent_Exit{Exit: &nodev1.Process{
		ProcessId: 1, State: nodev1.ProcessState_PROCESS_STATE_EXITED,
	}}}
}

// lostWithExitFrame gives a LOST record an exit status it cannot have.
func lostWithExitFrame() *nodev1.ProcessEvent {
	return &nodev1.ProcessEvent{Event: &nodev1.ProcessEvent_Exit{Exit: &nodev1.Process{
		ProcessId: 1, State: nodev1.ProcessState_PROCESS_STATE_LOST,
		Exit: &nodev1.ExecEvent_Exit{ExitCode: new(int32(0))},
	}}}
}

// attachProbeGuest sends a fixed frame sequence and, for malformed sequences,
// waits for the node to cancel the stream so the test can see the cancel.
type attachProbeGuest struct {
	guestv1.UnimplementedGuestServer
	events     []*nodev1.ProcessEvent
	waitCancel bool
	canceled   chan struct{}
	once       sync.Once
}

func (g *attachProbeGuest) Ping(context.Context, *guestv1.PingRequest) (*guestv1.PingResponse, error) {
	return &guestv1.PingResponse{Version: "attach-probe"}, nil
}

func (g *attachProbeGuest) signalCanceled() { g.once.Do(func() { close(g.canceled) }) }

func (g *attachProbeGuest) AttachProcess(_ *guestv1.AttachProcessRequest, stream grpc.ServerStreamingServer[nodev1.ProcessEvent]) error {
	for _, ev := range g.events {
		if err := stream.Send(ev); err != nil {
			g.signalCanceled()
			return err
		}
	}
	if g.waitCancel {
		<-stream.Context().Done()
		g.signalCanceled()
		return status.FromContextError(stream.Context().Err()).Err()
	}
	return nil
}

// readProbeGuest mirrors attachProbeGuest for ReadFile.
type readProbeGuest struct {
	guestv1.UnimplementedGuestServer
	messages   []*nodev1.ReadFileResponse
	waitCancel bool
	canceled   chan struct{}
	once       sync.Once
}

func (g *readProbeGuest) Ping(context.Context, *guestv1.PingRequest) (*guestv1.PingResponse, error) {
	return &guestv1.PingResponse{Version: "read-probe"}, nil
}

func (g *readProbeGuest) signalCanceled() { g.once.Do(func() { close(g.canceled) }) }

func (g *readProbeGuest) ReadFile(_ *guestv1.ReadFileRequest, stream grpc.ServerStreamingServer[nodev1.ReadFileResponse]) error {
	for _, msg := range g.messages {
		if err := stream.Send(msg); err != nil {
			g.signalCanceled()
			return err
		}
	}
	if g.waitCancel {
		<-stream.Context().Done()
		g.signalCanceled()
		return status.FromContextError(stream.Context().Err()).Err()
	}
	return nil
}

func collectAttach(ctx context.Context, c nodev1.NodeClient, req *nodev1.AttachProcessRequest) ([]*nodev1.ProcessEvent, error) {
	st, err := c.AttachProcess(ctx, req)
	if err != nil {
		return nil, err
	}
	var frames []*nodev1.ProcessEvent
	for {
		ev, err := st.Recv()
		if errors.Is(err, io.EOF) {
			return frames, nil
		}
		if err != nil {
			return frames, err
		}
		frames = append(frames, ev)
	}
}

func collectReadFile(ctx context.Context, c nodev1.NodeClient, req *nodev1.ReadFileRequest) ([]*nodev1.ReadFileResponse, error) {
	st, err := c.ReadFile(ctx, req)
	if err != nil {
		return nil, err
	}
	var msgs []*nodev1.ReadFileResponse
	for {
		msg, err := st.Recv()
		if errors.Is(err, io.EOF) {
			return msgs, nil
		}
		if err != nil {
			return msgs, err
		}
		msgs = append(msgs, msg)
	}
}

// Nil records only reach the validators through a direct in-process call:
// proto encoding turns a nil oneof message into an empty decoded message. The
// branches stay defensive for a future non-gRPC caller.
func TestPhase1FrameValidatorsRejectNilRecords(t *testing.T) {
	info := false
	if err := checkReadFileFrame(&nodev1.ReadFileResponse{Part: &nodev1.ReadFileResponse_Info{}}, &info); status.Code(err) != codes.Internal {
		t.Fatalf("nil info = %v, want Internal", err)
	}
	if err := checkReadFileFrame(&nodev1.ReadFileResponse{}, &info); status.Code(err) != codes.Internal {
		t.Fatalf("empty read message = %v, want Internal", err)
	}

	attached, exited := false, false
	var stdoutNext, stderrNext int64
	if err := checkProcessFrame(attachedFrame(0, 0), 1, &attached, &exited, &stdoutNext, &stderrNext); err != nil {
		t.Fatalf("valid attached frame: %v", err)
	}
	if err := checkProcessFrame(&nodev1.ProcessEvent{Event: &nodev1.ProcessEvent_Exit{}}, 1, &attached, &exited, &stdoutNext, &stderrNext); status.Code(err) != codes.Internal {
		t.Fatalf("nil exit record = %v, want Internal", err)
	}
}

func TestPhase1AttachFrameValidation(t *testing.T) {
	cases := []struct {
		name       string
		events     []*nodev1.ProcessEvent
		waitCancel bool
		wantCancel bool
		wantErr    bool
		// wantFrames pins the exact safe prefix that may reach the client.
		wantFrames int
	}{
		{"valid exited",
			[]*nodev1.ProcessEvent{attachedFrame(0, 0), stdoutFrame(3, "abc"), stderrFrame(2, "xy"), exitFrame(nodev1.ProcessState_PROCESS_STATE_EXITED)},
			false, false, false, 4},
		{"valid lost at nonzero offsets",
			[]*nodev1.ProcessEvent{attachedFrame(5, 1), exitFrame(nodev1.ProcessState_PROCESS_STATE_LOST)},
			false, false, false, 2},
		{"empty event", []*nodev1.ProcessEvent{{Event: nil}}, true, true, true, 0},
		{"negative attached stdout offset", []*nodev1.ProcessEvent{attachedFrame(-1, 0)}, true, true, true, 0},
		{"negative attached stderr offset", []*nodev1.ProcessEvent{attachedFrame(0, -1)}, true, true, true, 0},
		{"attached without process", []*nodev1.ProcessEvent{attachedWithoutProcessFrame()}, true, true, true, 0},
		{"attached process id mismatch", []*nodev1.ProcessEvent{attachedFrameID(2, 0, 0)}, true, true, true, 0},
		{"attached process state unspecified", []*nodev1.ProcessEvent{attachedFrameState(1, nodev1.ProcessState_PROCESS_STATE_UNSPECIFIED, 0, 0)}, true, true, true, 0},
		{"attached process state unknown", []*nodev1.ProcessEvent{attachedFrameState(1, nodev1.ProcessState(99), 0, 0)}, true, true, true, 0},
		{"duplicate attached", []*nodev1.ProcessEvent{attachedFrame(0, 0), attachedFrame(0, 0)}, true, true, true, 1},
		{"stdout before attached", []*nodev1.ProcessEvent{stdoutFrame(1, "a")}, true, true, true, 0},
		{"stderr before attached", []*nodev1.ProcessEvent{stderrFrame(1, "a")}, true, true, true, 0},
		{"stdout gap", []*nodev1.ProcessEvent{attachedFrame(2, 0), stdoutFrame(5, "ab")}, true, true, true, 1},
		{"stdout repeat", []*nodev1.ProcessEvent{attachedFrame(0, 0), stdoutFrame(2, "abc")}, true, true, true, 1},
		{"stdout offset overflow",
			[]*nodev1.ProcessEvent{attachedFrame(math.MaxInt64, 0), stdoutFrame(math.MinInt64, "x")},
			true, true, true, 1},
		{"stderr gap", []*nodev1.ProcessEvent{attachedFrame(0, 1), stderrFrame(2, "xy")}, true, true, true, 1},
		{"stderr offset overflow",
			[]*nodev1.ProcessEvent{attachedFrame(0, math.MaxInt64), stderrFrame(math.MinInt64, "x")},
			true, true, true, 1},
		{"empty chunk", []*nodev1.ProcessEvent{{Event: &nodev1.ProcessEvent_Stdout{Stdout: nil}}}, true, true, true, 0},
		{"exit before attached", []*nodev1.ProcessEvent{exitFrame(nodev1.ProcessState_PROCESS_STATE_EXITED)}, true, true, true, 0},
		{"exit still running", []*nodev1.ProcessEvent{attachedFrame(0, 0), exitFrame(nodev1.ProcessState_PROCESS_STATE_RUNNING)}, true, true, true, 1},
		{"terminal process id mismatch",
			[]*nodev1.ProcessEvent{attachedFrame(0, 0), exitFrameID(2, nodev1.ProcessState_PROCESS_STATE_EXITED)},
			true, true, true, 1},
		{"exited without status",
			[]*nodev1.ProcessEvent{attachedFrame(0, 0), exitedWithoutStatusFrame()}, true, true, true, 1},
		{"lost with exit status",
			[]*nodev1.ProcessEvent{attachedFrame(0, 0), lostWithExitFrame()}, true, true, true, 1},
		{"duplicate exit", []*nodev1.ProcessEvent{
			attachedFrame(0, 0), exitFrame(nodev1.ProcessState_PROCESS_STATE_EXITED), exitFrame(nodev1.ProcessState_PROCESS_STATE_EXITED),
		}, true, true, true, 2},
		{"output after exit", []*nodev1.ProcessEvent{
			attachedFrame(0, 0), exitFrame(nodev1.ProcessState_PROCESS_STATE_EXITED), stdoutFrame(1, "late"),
		}, true, true, true, 2},
		{"eof without exit", []*nodev1.ProcessEvent{attachedFrame(0, 0), stdoutFrame(1, "x")}, false, false, true, 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g := &attachProbeGuest{events: tc.events, waitCancel: tc.waitCancel, canceled: make(chan struct{})}
			h := newHarness(t, harnessOptions{fallback: g, stubGuest: true})
			id := ids.Sandbox()
			h.create(id, nil)
			client := startNodeClient(t, h)
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			frames, err := collectAttach(ctx, client, &nodev1.AttachProcessRequest{SandboxId: id, ProcessId: 1})
			if tc.wantErr {
				if status.Code(err) != codes.Internal {
					t.Fatalf("AttachProcess = %v, want Internal protocol error", err)
				}
			} else if err != nil {
				t.Fatalf("AttachProcess = %v", err)
			}
			if len(frames) != tc.wantFrames {
				t.Fatalf("forwarded %d frames, want exactly %d", len(frames), tc.wantFrames)
			}
			for i, want := range tc.events[:tc.wantFrames] {
				if !proto.Equal(frames[i], want) {
					t.Fatalf("forwarded frame %d = %v, want %v", i, frames[i], want)
				}
			}
			if tc.wantCancel {
				select {
				case <-g.canceled:
				case <-time.After(5 * time.Second):
					t.Fatal("guest stream was not canceled after a protocol error")
				}
			}
		})
	}
}

func infoFrame(path string, size int64) *nodev1.ReadFileResponse {
	return infoFrameWith(&nodev1.FileInfo{Path: path, Name: "f", Type: nodev1.FileType_FILE_TYPE_REGULAR, Size: size})
}

func infoFrameWith(fi *nodev1.FileInfo) *nodev1.ReadFileResponse {
	return &nodev1.ReadFileResponse{Part: &nodev1.ReadFileResponse_Info{Info: fi}}
}

func dataFrame(data string) *nodev1.ReadFileResponse {
	return &nodev1.ReadFileResponse{Part: &nodev1.ReadFileResponse_Data{Data: []byte(data)}}
}

func TestPhase1AttachRejectsZeroProcessID(t *testing.T) {
	g := &attachProbeGuest{
		events:     []*nodev1.ProcessEvent{attachedFrameID(0, 0, 0)},
		waitCancel: true,
		canceled:   make(chan struct{}),
	}
	h := newHarness(t, harnessOptions{fallback: g, stubGuest: true})
	id := ids.Sandbox()
	h.create(id, nil)
	client := startNodeClient(t, h)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	frames, err := collectAttach(ctx, client, &nodev1.AttachProcessRequest{SandboxId: id, ProcessId: 0})
	if status.Code(err) != codes.Internal || len(frames) != 0 {
		t.Fatalf("AttachProcess with process id 0 = %v after %d frames, want Internal with none forwarded", err, len(frames))
	}
	select {
	case <-g.canceled:
	case <-time.After(5 * time.Second):
		t.Fatal("guest stream was not canceled after an invalid attached record")
	}
}

func TestPhase1ReadFileFrameValidation(t *testing.T) {
	cases := []struct {
		name       string
		messages   []*nodev1.ReadFileResponse
		waitCancel bool
		wantCancel bool
		wantErr    bool
		// wantFrames pins the exact safe prefix that may reach the client.
		wantFrames int
	}{
		{"valid", []*nodev1.ReadFileResponse{infoFrame("/home/user/f", 3), dataFrame("abc")}, false, false, false, 2},
		{"empty file", []*nodev1.ReadFileResponse{infoFrame("/home/user/f", 0)}, false, false, false, 1},
		{"data before info", []*nodev1.ReadFileResponse{dataFrame("x")}, true, true, true, 0},
		{"duplicate info", []*nodev1.ReadFileResponse{infoFrame("/home/user/f", 0), infoFrame("/home/user/f", 0)}, true, true, true, 1},
		{"empty info", []*nodev1.ReadFileResponse{infoFrameWith(&nodev1.FileInfo{})}, true, true, true, 0},
		{"non-regular info", []*nodev1.ReadFileResponse{infoFrameWith(&nodev1.FileInfo{
			Path: "/home/user/dir", Name: "dir", Type: nodev1.FileType_FILE_TYPE_DIRECTORY,
		})}, true, true, true, 0},
		{"missing path", []*nodev1.ReadFileResponse{infoFrameWith(&nodev1.FileInfo{
			Name: "f", Type: nodev1.FileType_FILE_TYPE_REGULAR,
		})}, true, true, true, 0},
		{"relative path", []*nodev1.ReadFileResponse{infoFrameWith(&nodev1.FileInfo{
			Path: "relative/f", Name: "f", Type: nodev1.FileType_FILE_TYPE_REGULAR,
		})}, true, true, true, 0},
		{"non-clean path", []*nodev1.ReadFileResponse{infoFrameWith(&nodev1.FileInfo{
			Path: "/home/user//f", Name: "f", Type: nodev1.FileType_FILE_TYPE_REGULAR,
		})}, true, true, true, 0},
		{"name not path base", []*nodev1.ReadFileResponse{infoFrameWith(&nodev1.FileInfo{
			Path: "/home/user/f", Name: "g", Type: nodev1.FileType_FILE_TYPE_REGULAR,
		})}, true, true, true, 0},
		{"missing name", []*nodev1.ReadFileResponse{infoFrameWith(&nodev1.FileInfo{
			Path: "/home/user/f", Type: nodev1.FileType_FILE_TYPE_REGULAR,
		})}, true, true, true, 0},
		{"negative size", []*nodev1.ReadFileResponse{infoFrameWith(&nodev1.FileInfo{
			Path: "/home/user/f", Name: "f", Type: nodev1.FileType_FILE_TYPE_REGULAR, Size: -1,
		})}, true, true, true, 0},
		{"empty message", []*nodev1.ReadFileResponse{{Part: nil}}, true, true, true, 0},
		{"oversized chunk", []*nodev1.ReadFileResponse{infoFrame("/home/user/f", 1<<20+1), dataFrame(strings.Repeat("x", MaxReadFileChunk+1))}, true, true, true, 1},
		{"eof without info", nil, false, false, true, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g := &readProbeGuest{messages: tc.messages, waitCancel: tc.waitCancel, canceled: make(chan struct{})}
			h := newHarness(t, harnessOptions{fallback: g, stubGuest: true})
			id := ids.Sandbox()
			h.create(id, nil)
			client := startNodeClient(t, h)
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			msgs, err := collectReadFile(ctx, client, &nodev1.ReadFileRequest{SandboxId: id, Path: "f"})
			if tc.wantErr {
				if status.Code(err) != codes.Internal {
					t.Fatalf("ReadFile = %v, want Internal protocol error", err)
				}
			} else if err != nil {
				t.Fatalf("ReadFile = %v", err)
			}
			if len(msgs) != tc.wantFrames {
				t.Fatalf("forwarded %d messages, want exactly %d", len(msgs), tc.wantFrames)
			}
			for i, want := range tc.messages[:tc.wantFrames] {
				if !proto.Equal(msgs[i], want) {
					t.Fatalf("forwarded message %d = %v, want %v", i, msgs[i], want)
				}
			}
			if tc.wantCancel {
				select {
				case <-g.canceled:
				case <-time.After(5 * time.Second):
					t.Fatal("guest stream was not canceled after a protocol error")
				}
			}
		})
	}
}

func TestPhase1AttachCancelPropagates(t *testing.T) {
	g := &attachProbeGuest{
		events:     []*nodev1.ProcessEvent{attachedFrame(0, 0)},
		waitCancel: true,
		canceled:   make(chan struct{}),
	}
	h := newHarness(t, harnessOptions{fallback: g, stubGuest: true})
	id := ids.Sandbox()
	h.create(id, nil)
	client := startNodeClient(t, h)
	ctx, cancel := context.WithCancel(t.Context())
	st, err := client.AttachProcess(ctx, &nodev1.AttachProcessRequest{SandboxId: id, ProcessId: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Recv(); err != nil {
		t.Fatalf("first frame: %v", err)
	}
	cancel()
	select {
	case <-g.canceled:
	case <-time.After(5 * time.Second):
		t.Fatal("guest AttachProcess was not canceled with the client")
	}
}

func TestPhase1ReadFileCancelPropagates(t *testing.T) {
	g := &readProbeGuest{
		messages:   []*nodev1.ReadFileResponse{infoFrame("/home/user/f", 0)},
		waitCancel: true,
		canceled:   make(chan struct{}),
	}
	h := newHarness(t, harnessOptions{fallback: g, stubGuest: true})
	id := ids.Sandbox()
	h.create(id, nil)
	client := startNodeClient(t, h)
	ctx, cancel := context.WithCancel(t.Context())
	st, err := client.ReadFile(ctx, &nodev1.ReadFileRequest{SandboxId: id, Path: "f"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Recv(); err != nil {
		t.Fatalf("first message: %v", err)
	}
	cancel()
	select {
	case <-g.canceled:
	case <-time.After(5 * time.Second):
		t.Fatal("guest ReadFile was not canceled with the client")
	}
}

type testProcessStream struct {
	grpc.ServerStream
	ctx       context.Context
	sendErr   error
	failAfter int
	events    []*nodev1.ProcessEvent
}

func (s *testProcessStream) Context() context.Context { return s.ctx }

func (s *testProcessStream) Send(ev *nodev1.ProcessEvent) error {
	if s.sendErr != nil && len(s.events) >= s.failAfter {
		return s.sendErr
	}
	s.events = append(s.events, ev)
	return nil
}

type testReadFileStream struct {
	grpc.ServerStream
	ctx       context.Context
	sendErr   error
	failAfter int
	msgs      []*nodev1.ReadFileResponse
}

func (s *testReadFileStream) Context() context.Context { return s.ctx }

func (s *testReadFileStream) Send(msg *nodev1.ReadFileResponse) error {
	if s.sendErr != nil && len(s.msgs) >= s.failAfter {
		return s.sendErr
	}
	s.msgs = append(s.msgs, msg)
	return nil
}

func TestPhase1StreamSendFailureCancelsGuest(t *testing.T) {
	t.Run("attach", func(t *testing.T) {
		g := &attachProbeGuest{
			events:     []*nodev1.ProcessEvent{attachedFrame(0, 0)},
			waitCancel: true,
			canceled:   make(chan struct{}),
		}
		h := newHarness(t, harnessOptions{fallback: g, stubGuest: true})
		id := ids.Sandbox()
		h.create(id, nil)
		err := h.mg.AttachProcess(&nodev1.AttachProcessRequest{SandboxId: id, ProcessId: 1}, &testProcessStream{
			ctx: t.Context(), sendErr: errors.New("client went away"), failAfter: 0,
		})
		if err == nil {
			t.Fatal("AttachProcess reported success after a failed Send")
		}
		select {
		case <-g.canceled:
		case <-time.After(5 * time.Second):
			t.Fatal("guest AttachProcess was not canceled after the node stream failed")
		}
	})
	t.Run("read file", func(t *testing.T) {
		g := &readProbeGuest{
			messages:   []*nodev1.ReadFileResponse{infoFrame("/home/user/f", 0)},
			waitCancel: true,
			canceled:   make(chan struct{}),
		}
		h := newHarness(t, harnessOptions{fallback: g, stubGuest: true})
		id := ids.Sandbox()
		h.create(id, nil)
		err := h.mg.ReadFile(&nodev1.ReadFileRequest{SandboxId: id, Path: "f"}, &testReadFileStream{
			ctx: t.Context(), sendErr: errors.New("client went away"), failAfter: 0,
		})
		if err == nil {
			t.Fatal("ReadFile reported success after a failed Send")
		}
		select {
		case <-g.canceled:
		case <-time.After(5 * time.Second):
			t.Fatal("guest ReadFile was not canceled after the node stream failed")
		}
	})
}

// TestPhase1FilesAgainstRealGuest runs the file proxies through the real
// guest Service over gRPC, so the translation is checked end to end without a
// VM (A2 is integrated).
func TestPhase1FilesAgainstRealGuest(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	id := ids.Sandbox()
	h.create(id, nil)
	client := startNodeClient(t, h)
	ctx := t.Context()
	payload := []byte("node proxy file\n")

	if _, err := nodetest.PutFile(ctx, client, id, "nodefiles/a.txt", 0o640, payload, 1<<20); err != nil {
		t.Fatalf("PutFile: %v", err)
	}
	fi, err := client.StatFile(ctx, &nodev1.StatFileRequest{SandboxId: id, Path: "nodefiles/a.txt"})
	if err != nil {
		t.Fatalf("StatFile: %v", err)
	}
	if fi.GetPath() != "/home/user/nodefiles/a.txt" || fi.GetType() != nodev1.FileType_FILE_TYPE_REGULAR || fi.GetMode() != 0o640 {
		t.Fatalf("StatFile = %v", fi)
	}
	list, err := client.ListFiles(ctx, &nodev1.ListFilesRequest{SandboxId: id, Path: "nodefiles"})
	if err != nil || len(list.GetEntries()) != 1 || list.GetEntries()[0].GetName() != "a.txt" {
		t.Fatalf("ListFiles = %v, %v", list, err)
	}
	readInfo, data, err := nodetest.ReadFile(ctx, client, &nodev1.ReadFileRequest{SandboxId: id, Path: "nodefiles/a.txt"})
	if err != nil || !bytes.Equal(data, payload) || readInfo.GetPath() != "/home/user/nodefiles/a.txt" {
		t.Fatalf("ReadFile = %v %q, %v", readInfo, data, err)
	}
	ch, err := client.ChmodFile(ctx, &nodev1.ChmodFileRequest{SandboxId: id, Path: "nodefiles/a.txt", Mode: 0o1600})
	if err != nil || ch.GetMode() != 0o1600 {
		t.Fatalf("ChmodFile = %v, %v", ch, err)
	}
	md, err := client.MakeDir(ctx, &nodev1.MakeDirRequest{SandboxId: id, Path: "nodefiles/sub/deep", Mode: 0o700})
	if err != nil || md.GetPath() != "/home/user/nodefiles/sub/deep" || md.GetMode() != 0o700 {
		t.Fatalf("MakeDir = %v, %v", md, err)
	}
	mv, err := client.MoveFile(ctx, &nodev1.MoveFileRequest{
		SandboxId: id, Source: "nodefiles/a.txt", Destination: "/tmp/nodefiles/renamed",
	})
	if err != nil || mv.GetSource() != "/home/user/nodefiles/a.txt" || mv.GetDestination() != "/tmp/nodefiles/renamed" {
		t.Fatalf("MoveFile = %v, %v", mv, err)
	}
	if _, moved, err := nodetest.ReadFile(ctx, client, &nodev1.ReadFileRequest{SandboxId: id, Path: "/tmp/nodefiles/renamed"}); err != nil || !bytes.Equal(moved, payload) {
		t.Fatalf("moved file = %q, %v", moved, err)
	}
	del, err := client.DeleteFile(ctx, &nodev1.DeleteFileRequest{SandboxId: id, Path: "/tmp/nodefiles/renamed"})
	if err != nil || del.GetPath() != "/tmp/nodefiles/renamed" {
		t.Fatalf("DeleteFile = %v, %v", del, err)
	}
	if _, err := client.StatFile(ctx, &nodev1.StatFileRequest{SandboxId: id, Path: "/tmp/nodefiles/renamed"}); status.Code(err) != codes.NotFound {
		t.Fatalf("StatFile deleted = %v, want NotFound", err)
	}
	if _, _, err := nodetest.ReadFile(ctx, client, &nodev1.ReadFileRequest{SandboxId: id, Path: "/etc/passwd"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("ReadFile outside = %v, want InvalidArgument", err)
	}
}

// --- Guest response cap ---------------------------------------------------------

// guestBufconn serves g over bufconn with the production response send budget.
func guestBufconn(t *testing.T, g guestv1.GuestServer) *bufconn.Listener {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer(grpc.MaxSendMsgSize(nodelink.MaxResponseBytes))
	guestv1.RegisterGuestServer(srv, g)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(func() {
		srv.Stop()
		lis.Close()
	})
	return lis
}

// largeProcessOutput is a legal GetProcess response holding two full 8 MiB
// windows, the largest the frozen contract allows.
func largeProcessOutput() *nodev1.ProcessOutput {
	return &nodev1.ProcessOutput{
		Process: &nodev1.Process{ProcessId: 1, State: nodev1.ProcessState_PROCESS_STATE_EXITED},
		Stdout:  bytes.Repeat([]byte("o"), 8<<20),
		Stderr:  bytes.Repeat([]byte("e"), 8<<20),
	}
}

// largeFileList is a legal ListFiles response above gRPC's 4 MiB default:
// 10000 sorted unique names of at most 255 bytes, each entry directly under
// the response's canonical directory path.
func largeFileList() *nodev1.ListFilesResponse {
	const (
		count = 10000
		dir   = "/home/user/dir"
	)
	entries := make([]*nodev1.FileInfo, count)
	for i := range entries {
		name := fmt.Sprintf("%05d%s", i, strings.Repeat("n", 245))
		entries[i] = &nodev1.FileInfo{
			Path: path.Join(dir, name),
			Name: name,
			Type: nodev1.FileType_FILE_TYPE_REGULAR,
		}
	}
	return &nodev1.ListFilesResponse{Path: dir, Entries: entries}
}

func TestGuestResponseCap(t *testing.T) {
	t.Run("get process through guestConn", func(t *testing.T) {
		g := &phase1Guest{getProcess: func(context.Context, *guestv1.GetProcessRequest) (*nodev1.ProcessOutput, error) {
			return largeProcessOutput(), nil
		}}
		lis := guestBufconn(t, g)
		conn, err := guestConn(func(ctx context.Context) (net.Conn, error) {
			return lis.DialContext(ctx)
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.Close() })
		out, err := guestv1.NewGuestClient(conn).GetProcess(t.Context(), &guestv1.GetProcessRequest{ProcessId: 1})
		if err != nil {
			t.Fatalf("GetProcess: %v", err)
		}
		if len(out.GetStdout()) != 8<<20 || len(out.GetStderr()) != 8<<20 {
			t.Fatalf("windows = %d/%d bytes, want 8 MiB each", len(out.GetStdout()), len(out.GetStderr()))
		}
	})
	t.Run("list files through guestConn", func(t *testing.T) {
		want := largeFileList()
		if size := proto.Size(want); size <= 4<<20 {
			t.Fatalf("fixture list is %d bytes, want above the 4 MiB default", size)
		}
		g := &phase1Guest{listFiles: func(_ context.Context, req *guestv1.ListFilesRequest) (*nodev1.ListFilesResponse, error) {
			if req.GetLimit() != 10000 {
				return nil, status.Errorf(codes.InvalidArgument, "limit %d, want 10000", req.GetLimit())
			}
			return want, nil
		}}
		lis := guestBufconn(t, g)
		conn, err := guestConn(func(ctx context.Context) (net.Conn, error) {
			return lis.DialContext(ctx)
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.Close() })
		list, err := guestv1.NewGuestClient(conn).ListFiles(t.Context(), &guestv1.ListFilesRequest{Path: "dir", Limit: 10000})
		if err != nil {
			t.Fatalf("ListFiles: %v", err)
		}
		if !proto.Equal(list, want) {
			t.Fatalf("ListFiles roundtrip mismatch: %d entries, want %d", len(list.GetEntries()), len(want.GetEntries()))
		}
	})
	// The negative control proves the assertions above are sensitive to the
	// cap: the same legal responses fail on a plain gRPC connection, whose
	// client receive limit stays at the 4 MiB default.
	t.Run("without cap get process fails for size", func(t *testing.T) {
		g := &phase1Guest{getProcess: func(context.Context, *guestv1.GetProcessRequest) (*nodev1.ProcessOutput, error) {
			return largeProcessOutput(), nil
		}}
		lis := guestBufconn(t, g)
		conn, err := grpc.NewClient(
			"passthrough:///guest",
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
				return lis.DialContext(ctx)
			}),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.Close() })
		_, err = guestv1.NewGuestClient(conn).GetProcess(t.Context(), &guestv1.GetProcessRequest{ProcessId: 1})
		if status.Code(err) != codes.ResourceExhausted {
			t.Fatalf("GetProcess without cap = %v, want ResourceExhausted", err)
		}
	})
	t.Run("without cap list files fails for size", func(t *testing.T) {
		g := &phase1Guest{listFiles: func(context.Context, *guestv1.ListFilesRequest) (*nodev1.ListFilesResponse, error) {
			return largeFileList(), nil
		}}
		lis := guestBufconn(t, g)
		conn, err := grpc.NewClient(
			"passthrough:///guest",
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
				return lis.DialContext(ctx)
			}),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.Close() })
		_, err = guestv1.NewGuestClient(conn).ListFiles(t.Context(), &guestv1.ListFilesRequest{Path: "dir"})
		if status.Code(err) != codes.ResourceExhausted {
			t.Fatalf("ListFiles without cap = %v, want ResourceExhausted", err)
		}
	})
}

// TestPhase1ProcessesAgainstRealGuest runs the process proxies through the
// real guest Service over gRPC: Start with VM/request env merge and
// idempotency, Get windows, Attach contiguous stdout/stderr to the terminal
// record, and Kill. It is the A6 handoff check against the provisional A5
// dependency; the copied guest files are not part of the final node slice.
func TestPhase1ProcessesAgainstRealGuest(t *testing.T) {
	h, id := phase1Harness(t, nil, map[string]string{"VM_ONLY": "vm", "BOTH": "vm"})
	client := startNodeClient(t, h)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	const command = `printf 'out:%s:%s:%s\n' "$VM_ONLY" "$BOTH" "$REQ_ONLY"; printf 'err-line\n' >&2; sleep 0.3; echo done`
	p, err := client.StartProcess(ctx, &nodev1.StartProcessRequest{
		SandboxId: id, ProcessId: 501, Command: command, Cwd: "/tmp",
		Env: map[string]string{"BOTH": "req", "REQ_ONLY": "req"},
	})
	if err != nil {
		t.Fatalf("StartProcess: %v", err)
	}
	if p.GetProcessId() != 501 || p.GetPid() <= 0 || p.GetCwd() != "/tmp" || p.GetCommand() != command ||
		p.GetStartedAtUnixMs() == 0 || p.GetLogPath() == "" || p.GetErrLogPath() == "" || p.GetLogPath() == p.GetErrLogPath() {
		t.Fatalf("StartProcess = %v", p)
	}
	if p.GetState() != nodev1.ProcessState_PROCESS_STATE_RUNNING && p.GetState() != nodev1.ProcessState_PROCESS_STATE_EXITED {
		t.Fatalf("StartProcess state = %s", p.GetState())
	}
	again, err := client.StartProcess(ctx, &nodev1.StartProcessRequest{SandboxId: id, ProcessId: 501, Command: "echo other"})
	if err != nil || again.GetPid() != p.GetPid() || again.GetCommand() != command {
		t.Fatalf("StartProcess again = %v, %v; want the first process", again, err)
	}

	// Attach from zero: the helper enforces contiguous per-stream offsets and
	// a terminal record, and our proxy rejects any malformed guest frame.
	a, err := nodetest.Attach(ctx, client, &nodev1.AttachProcessRequest{SandboxId: id, ProcessId: 501})
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	const wantOut = "out:vm:req:req\ndone\n"
	if a.Stdout != wantOut || a.Stderr != "err-line\n" {
		t.Fatalf("attached stdout %q, stderr %q; want the merged env output", a.Stdout, a.Stderr)
	}
	if a.First.GetStdoutOffset() != 0 || a.First.GetStderrOffset() != 0 {
		t.Fatalf("attached offsets = %d/%d, want 0/0", a.First.GetStdoutOffset(), a.First.GetStderrOffset())
	}
	term := a.Exit
	if term.GetState() != nodev1.ProcessState_PROCESS_STATE_EXITED ||
		term.GetExit().GetExitCode() != 0 || term.GetFinishedAtUnixMs() < term.GetStartedAtUnixMs() ||
		term.GetStdoutSize() != int64(len(a.Stdout)) || term.GetStderrSize() != int64(len(a.Stderr)) {
		t.Fatalf("terminal record = %v", term)
	}

	// Get windows: tail and explicit offsets, then offsets past the end.
	tail, err := client.GetProcess(ctx, &nodev1.GetProcessRequest{SandboxId: id, ProcessId: 501, LimitBytes: 4})
	if err != nil {
		t.Fatalf("GetProcess tail: %v", err)
	}
	if string(tail.GetStdout()) != "one\n" || string(tail.GetStderr()) != "ine\n" ||
		tail.GetStdoutOffset() != int64(len(wantOut)-4) || tail.GetStderrOffset() != int64(len("err-line\n")-4) ||
		!tail.GetStdoutTruncated() || !tail.GetStderrTruncated() || tail.GetMore() {
		t.Fatalf("tail window = %+v", tail)
	}
	win, err := client.GetProcess(ctx, &nodev1.GetProcessRequest{
		SandboxId: id, ProcessId: 501, StdoutOffset: new(int64(4)), StderrOffset: new(int64(0)), LimitBytes: 5,
	})
	if err != nil {
		t.Fatalf("GetProcess window: %v", err)
	}
	if string(win.GetStdout()) != "vm:re" || string(win.GetStderr()) != "err-l" ||
		win.GetStdoutOffset() != 4 || win.GetStderrOffset() != 0 || !win.GetMore() {
		t.Fatalf("offset window = %+v", win)
	}
	past, err := client.GetProcess(ctx, &nodev1.GetProcessRequest{
		SandboxId: id, ProcessId: 501, StdoutOffset: new(int64(1 << 20)), StderrOffset: new(int64(1 << 20)),
	})
	if err != nil {
		t.Fatalf("GetProcess past end: %v", err)
	}
	if len(past.GetStdout()) != 0 || len(past.GetStderr()) != 0 ||
		past.GetStdoutOffset() != int64(len(wantOut)) || past.GetStderrOffset() != int64(len("err-line\n")) {
		t.Fatalf("past-end window = %+v", past)
	}

	// An ended process replays from the requested offsets; a past-end stderr
	// offset starts at the log end.
	replay, err := nodetest.Attach(ctx, client, &nodev1.AttachProcessRequest{
		SandboxId: id, ProcessId: 501, StdoutOffset: int64(len(wantOut) - len("done\n")), StderrOffset: 1 << 20,
	})
	if err != nil {
		t.Fatalf("Attach replay: %v", err)
	}
	if replay.Stdout != "done\n" || replay.Stderr != "" ||
		replay.First.GetStdoutOffset() != int64(len(wantOut)-len("done\n")) ||
		replay.First.GetStderrOffset() != int64(len("err-line\n")) ||
		replay.Exit.GetState() != nodev1.ProcessState_PROCESS_STATE_EXITED {
		t.Fatalf("replay = %+v", replay)
	}

	// Kill ends the whole process group, then is idempotent.
	if _, err := client.StartProcess(ctx, &nodev1.StartProcessRequest{
		SandboxId: id, ProcessId: 502, Command: "sleep 600 & sleep 600; wait", Cwd: "/tmp",
	}); err != nil {
		t.Fatalf("StartProcess for kill: %v", err)
	}
	k, err := client.KillProcess(ctx, &nodev1.KillProcessRequest{SandboxId: id, ProcessId: 502, GraceSeconds: 2})
	if err != nil {
		t.Fatalf("KillProcess: %v", err)
	}
	if !k.GetSignaled() || k.GetProcess().GetState() != nodev1.ProcessState_PROCESS_STATE_EXITED ||
		k.GetProcess().GetExit() == nil || k.GetProcess().GetExit().ExitCode != nil ||
		k.GetProcess().GetExit().GetSignal() == "" {
		t.Fatalf("KillProcess = %v; want EXITED by a signal", k)
	}
	k2, err := client.KillProcess(ctx, &nodev1.KillProcessRequest{SandboxId: id, ProcessId: 502})
	if err != nil || k2.GetSignaled() {
		t.Fatalf("KillProcess again = %v, %v; want OK, not signaled", k2, err)
	}
}

func TestPhase1LargeResponsesThroughNode(t *testing.T) {
	wantList := largeFileList()
	if size := proto.Size(wantList); size <= 4<<20 {
		t.Fatalf("fixture list is %d bytes, want above the 4 MiB default", size)
	}
	g := &phase1Guest{
		getProcess: func(context.Context, *guestv1.GetProcessRequest) (*nodev1.ProcessOutput, error) {
			return largeProcessOutput(), nil
		},
		listFiles: func(_ context.Context, req *guestv1.ListFilesRequest) (*nodev1.ListFilesResponse, error) {
			if req.GetLimit() != 10000 {
				return nil, status.Errorf(codes.InvalidArgument, "limit %d, want 10000", req.GetLimit())
			}
			return wantList, nil
		},
	}
	h, id := phase1Harness(t, g, nil)
	client := startNodeClient(t, h)
	out, err := client.GetProcess(t.Context(), &nodev1.GetProcessRequest{SandboxId: id, ProcessId: 1})
	if err != nil {
		t.Fatalf("GetProcess: %v", err)
	}
	if len(out.GetStdout()) != 8<<20 || len(out.GetStderr()) != 8<<20 {
		t.Fatalf("windows = %d/%d bytes, want 8 MiB each", len(out.GetStdout()), len(out.GetStderr()))
	}
	list, err := client.ListFiles(t.Context(), &nodev1.ListFilesRequest{SandboxId: id, Path: "dir", Limit: 10000})
	if err != nil {
		t.Fatalf("ListFiles: %v", err)
	}
	if !proto.Equal(list, wantList) {
		t.Fatalf("ListFiles roundtrip mismatch: %d entries, want %d", len(list.GetEntries()), len(wantList.GetEntries()))
	}
}
