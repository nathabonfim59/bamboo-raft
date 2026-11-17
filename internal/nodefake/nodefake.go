// Package nodefake is an in-process nodev1.Node without VMs, for control-plane
// tests and local development. Each "VM" is a directory holding home/user and
// tmp; commands run on the host with `bash -c`, in the mapped directory, with
// HOME pointing at the VM's home/user.
//
// Limits: commands are not isolated, and absolute /home/user or /tmp paths
// INSIDE a command string are the host's, not the VM's. Use relative paths
// (the conformance suite does). PutFile and cwd paths are mapped properly.
// Never expose a fake node to untrusted callers: it runs anything.
package nodefake

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"math"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// Image is the only image a fake node has.
const Image = "fake-v0"

// Node implements nodev1.NodeServer.
type Node struct {
	nodev1.UnimplementedNodeServer

	id   string
	root string

	// BootDelay is how long CreateVM takes. Set before use.
	BootDelay time.Duration
	// VCPUs and MemoryMiB are the capacity CreateVM checks against. DiskGiB is
	// the disk offer Info reports free space against; CreateVM does not reject
	// on disk. Set all three before use.
	VCPUs     uint32
	MemoryMiB uint64
	DiskGiB   uint64

	mu    sync.Mutex
	vms   map[string]*nodev1.VM
	env   map[string]map[string]string // CreateVM env per sandbox
	boots map[string]*booting          // boots in flight
	snaps map[string]*nodev1.Snapshot
	procs map[string]map[uint32]*proc // detached processes per sandbox (phase1.go)
}

// New returns a fake node that keeps its VMs under root.
func New(nodeID, root string) *Node {
	return &Node{
		id:        nodeID,
		root:      root,
		VCPUs:     64,
		MemoryMiB: 256 << 10,
		DiskGiB:   1024,
		vms:       map[string]*nodev1.VM{},
		env:       map[string]map[string]string{},
		boots:     map[string]*booting{},
		snaps:     map[string]*nodev1.Snapshot{},
		procs:     map[string]map[uint32]*proc{},
	}
}

// Register adds the fake to a gRPC server.
func (n *Node) Register(s *grpc.Server) { nodev1.RegisterNodeServer(s, n) }

func (n *Node) vmDir(id string) string   { return filepath.Join(n.root, "vms", id) }
func (n *Node) snapDir(id string) string { return filepath.Join(n.root, "snapshots", id) }

func (n *Node) Info(context.Context, *nodev1.InfoRequest) (*nodev1.InfoResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	snaps := make([]*nodev1.Snapshot, 0, len(n.snaps))
	for _, id := range slices.Sorted(maps.Keys(n.snaps)) {
		snaps = append(snaps, n.snaps[id])
	}
	return &nodev1.InfoResponse{
		NodeId:       n.id,
		Vcpus:        n.VCPUs,
		MemoryMib:    n.MemoryMiB,
		DiskFreeMib:  n.diskFreeMibLocked(),
		CpuModel:     "fake",
		QemuVersion:  "none",
		Images:       []*nodev1.Image{{Id: Image}},
		Snapshots:    snaps,
		DefaultImage: Image,
	}, nil
}

func (n *Node) CreateVM(ctx context.Context, req *nodev1.CreateVMRequest) (*nodev1.VM, error) {
	if req.GetSandboxId() == "" {
		return nil, status.Error(codes.InvalidArgument, "sandbox_id is required")
	}
	n.mu.Lock()
	if vm, ok := n.vms[req.SandboxId]; ok {
		if vm.State == nodev1.VMState_VM_STATE_STOPPING || vm.State == nodev1.VMState_VM_STATE_STOPPED {
			out := cloneVM(vm) // CreateVM never boots an existing VM; StartVM does
			n.mu.Unlock()
			return out, nil
		}
		n.mu.Unlock()
		return n.waitReady(ctx, vm)
	}
	if img := req.GetImage(); img != "" && img != Image {
		n.mu.Unlock()
		return nil, status.Errorf(codes.NotFound, "image %q not found", img)
	}
	if !n.roomLocked("", req.GetResources()) {
		n.mu.Unlock()
		return nil, status.Error(codes.ResourceExhausted, "no room for this VM")
	}
	if s := req.GetFromSnapshot(); s != "" {
		if _, ok := n.snaps[s]; !ok {
			n.mu.Unlock()
			return nil, status.Errorf(codes.NotFound, "snapshot %q not found", s)
		}
	}
	vm := &nodev1.VM{
		SandboxId:       req.SandboxId,
		State:           nodev1.VMState_VM_STATE_BOOTING,
		Image:           Image,
		FromSnapshot:    req.GetFromSnapshot(),
		Resources:       req.GetResources(),
		CreatedAtUnixMs: time.Now().UnixMilli(),
	}
	// The boot belongs to the node, not to this call: it goes on if the
	// caller gives up, and only DeleteVM stops it (proto: DeleteVM fences).
	bctx, cancel := context.WithCancel(context.Background())
	bt := &booting{cancel: cancel, done: make(chan struct{})}
	n.vms[req.SandboxId] = vm
	n.env[req.SandboxId] = maps.Clone(req.GetEnv())
	n.boots[req.SandboxId] = bt
	n.mu.Unlock()

	go func() {
		defer close(bt.done)
		defer cancel()
		err := n.boot(bctx, req)
		n.mu.Lock()
		defer n.mu.Unlock()
		delete(n.boots, req.SandboxId)
		if n.vms[req.SandboxId] != vm {
			return // deleted meanwhile; DeleteVM removes the files
		}
		if err != nil {
			vm.State = nodev1.VMState_VM_STATE_FAILED
			vm.Error = err.Error()
			return
		}
		vm.State = nodev1.VMState_VM_STATE_READY
		vm.StartedAtUnixMs = time.Now().UnixMilli()
	}()
	return n.waitReady(ctx, vm)
}

// diskFreeMibLocked is the disk offer minus the disk of every VM the node
// still holds, stopped ones included (a stopped VM keeps its disk), floored at
// zero. Callers hold n.mu.
func (n *Node) diskFreeMibLocked() uint64 {
	var used uint64 // GiB; saturating, a VM record can carry any uint64
	for _, vm := range n.vms {
		g := vm.GetResources().GetDiskGib()
		if used > math.MaxUint64-g {
			used = math.MaxUint64
			break
		}
		used += g
	}
	if used >= n.DiskGiB {
		return 0
	}
	left := n.DiskGiB - used
	if left > math.MaxUint64>>10 {
		return math.MaxUint64 // the MiB conversion would overflow
	}
	return left << 10
}

// roomLocked reports whether res fits next to every VM that holds capacity
// (all but STOPPED ones), leaving out except. Callers hold n.mu.
func (n *Node) roomLocked(except string, res *nodev1.Resources) bool {
	var used uint32
	var usedMem uint64
	for id, vm := range n.vms {
		if id == except || vm.State == nodev1.VMState_VM_STATE_STOPPED {
			continue
		}
		used += vm.GetResources().GetVcpus()
		usedMem += vm.GetResources().GetMemoryMib()
	}
	return used+res.GetVcpus() <= n.VCPUs && usedMem+res.GetMemoryMib() <= n.MemoryMiB
}

// booting is a boot in flight.
type booting struct {
	cancel context.CancelFunc
	done   chan struct{}
}

func (n *Node) boot(ctx context.Context, req *nodev1.CreateVMRequest) error {
	dir := n.vmDir(req.SandboxId)
	if s := req.GetFromSnapshot(); s != "" {
		if err := os.CopyFS(dir, os.DirFS(n.snapDir(s))); err != nil {
			return err
		}
	} else {
		for _, d := range []string{"home/user", "tmp"} {
			if err := os.MkdirAll(filepath.Join(dir, d), 0o755); err != nil {
				return err
			}
		}
	}
	if n.BootDelay > 0 {
		select {
		case <-time.After(n.BootDelay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// waitReady waits until vm is READY or FAILED, or until it is deleted
// (ABORTED), or ctx ends.
func (n *Node) waitReady(ctx context.Context, vm *nodev1.VM) (*nodev1.VM, error) {
	for {
		n.mu.Lock()
		current := n.vms[vm.SandboxId] == vm
		out := cloneVM(vm)
		n.mu.Unlock()
		switch {
		case !current:
			return nil, status.Errorf(codes.Aborted, "sandbox %s was deleted while booting", vm.SandboxId)
		case out.State == nodev1.VMState_VM_STATE_READY:
			return out, nil
		case out.State == nodev1.VMState_VM_STATE_FAILED:
			return nil, status.Errorf(codes.Internal, "boot failed: %s", out.Error)
		}
		select {
		case <-time.After(10 * time.Millisecond):
		case <-ctx.Done():
			return nil, status.FromContextError(ctx.Err()).Err()
		}
	}
}

func (n *Node) GetVM(_ context.Context, req *nodev1.GetVMRequest) (*nodev1.VM, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	vm, ok := n.vms[req.GetSandboxId()]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "sandbox %s not found", req.GetSandboxId())
	}
	return cloneVM(vm), nil
}

func (n *Node) ListVMs(context.Context, *nodev1.ListVMsRequest) (*nodev1.ListVMsResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := &nodev1.ListVMsResponse{}
	for _, id := range slices.Sorted(maps.Keys(n.vms)) {
		out.Vms = append(out.Vms, cloneVM(n.vms[id]))
	}
	return out, nil
}

func (n *Node) DeleteVM(_ context.Context, req *nodev1.DeleteVMRequest) (*nodev1.DeleteVMResponse, error) {
	n.mu.Lock()
	bt := n.boots[req.GetSandboxId()]
	n.mu.Unlock()
	if bt != nil { // fence: stop the boot and wait for it before removing files
		bt.cancel()
		<-bt.done
	}
	n.killProcs(req.GetSandboxId(), false)
	n.mu.Lock()
	delete(n.vms, req.GetSandboxId())
	delete(n.env, req.GetSandboxId())
	delete(n.procs, req.GetSandboxId())
	n.mu.Unlock()
	if req.GetSandboxId() != "" {
		if err := os.RemoveAll(n.vmDir(req.GetSandboxId())); err != nil {
			return nil, status.Errorf(codes.Internal, "remove disk: %v", err)
		}
	}
	return &nodev1.DeleteVMResponse{}, nil
}

// readyDir returns the directory of a READY VM.
func (n *Node) readyDir(id string) (string, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	vm, ok := n.vms[id]
	if !ok {
		return "", status.Errorf(codes.NotFound, "sandbox %s not found", id)
	}
	if vm.State != nodev1.VMState_VM_STATE_READY {
		return "", status.Errorf(codes.FailedPrecondition, "sandbox %s is %s", id, vm.State)
	}
	return n.vmDir(id), nil
}

// snapshotDir returns the directory of a VM SaveSnapshot accepts: READY or
// STOPPED.
func (n *Node) snapshotDir(id string) (string, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	vm, ok := n.vms[id]
	if !ok {
		return "", status.Errorf(codes.NotFound, "sandbox %s not found", id)
	}
	if vm.State != nodev1.VMState_VM_STATE_READY && vm.State != nodev1.VMState_VM_STATE_STOPPED {
		return "", status.Errorf(codes.FailedPrecondition, "sandbox %s is %s", id, vm.State)
	}
	return n.vmDir(id), nil
}

// guestPath resolves a v1 API path to a path relative to the VM dir:
// relative paths start at /home/user, and the result must be under
// /home/user or /tmp.
func guestPath(p string) (rel, canonical string, err error) {
	if p == "" {
		return "", "", status.Error(codes.InvalidArgument, "path is required")
	}
	if !path.IsAbs(p) {
		p = "/home/user/" + p
	}
	p = path.Clean(p)
	for _, base := range []string{"/home/user", "/tmp"} {
		if p == base || strings.HasPrefix(p, base+"/") {
			return strings.TrimPrefix(p, "/"), p, nil
		}
	}
	return "", "", status.Errorf(codes.InvalidArgument, "path %q is outside /home/user and /tmp", p)
}

func (n *Node) Exec(req *nodev1.ExecRequest, stream grpc.ServerStreamingServer[nodev1.ExecEvent]) error {
	dir, err := n.readyDir(req.GetSandboxId())
	if err != nil {
		return err
	}
	cwd := req.GetCwd()
	if cwd == "" {
		cwd = "/home/user"
	}
	rel, _, err := guestPath(cwd)
	if err != nil {
		return err
	}

	n.mu.Lock()
	vmEnv := n.env[req.SandboxId]
	n.mu.Unlock()

	ctx := stream.Context()
	var cancel context.CancelFunc = func() {}
	if t := req.GetTimeoutSeconds(); t > 0 {
		ctx, cancel = context.WithTimeout(ctx, time.Duration(t)*time.Second)
	}
	defer cancel()

	cmd := exec.CommandContext(ctx, "bash", "-c", req.GetCommand())
	cmd.Dir = filepath.Join(dir, rel)
	cmd.Env = []string{
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + filepath.Join(dir, "home/user"),
		"USER=user",
		"LANG=C.UTF-8",
	}
	env := maps.Clone(vmEnv)
	if env == nil {
		env = map[string]string{}
	}
	maps.Copy(env, req.GetEnv())
	for _, k := range slices.Sorted(maps.Keys(env)) {
		cmd.Env = append(cmd.Env, k+"="+env[k])
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	cmd.WaitDelay = 5 * time.Second
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}
	start := time.Now()
	if err := cmd.Start(); err != nil {
		return status.Errorf(codes.Internal, "start: %v", err)
	}

	var sendMu sync.Mutex
	send := func(ev *nodev1.ExecEvent) error {
		sendMu.Lock()
		defer sendMu.Unlock()
		return stream.Send(ev)
	}
	if err := send(&nodev1.ExecEvent{Event: &nodev1.ExecEvent_Started_{Started: &nodev1.ExecEvent_Started{Pid: int32(cmd.Process.Pid)}}}); err != nil {
		cmd.Cancel()
		cmd.Wait()
		return err
	}
	var wg sync.WaitGroup
	pump := func(r io.Reader, wrap func([]byte) *nodev1.ExecEvent) {
		buf := make([]byte, 32<<10)
		for {
			k, err := r.Read(buf)
			if k > 0 {
				send(wrap(slices.Clone(buf[:k])))
			}
			if err != nil {
				return
			}
		}
	}
	wg.Go(func() {
		pump(stdout, func(b []byte) *nodev1.ExecEvent { return &nodev1.ExecEvent{Event: &nodev1.ExecEvent_Stdout{Stdout: b}} })
	})
	wg.Go(func() {
		pump(stderr, func(b []byte) *nodev1.ExecEvent { return &nodev1.ExecEvent{Event: &nodev1.ExecEvent_Stderr{Stderr: b}} })
	})
	wg.Wait()
	waitErr := cmd.Wait()

	if stream.Context().Err() != nil {
		return status.FromContextError(stream.Context().Err()).Err()
	}
	exit := &nodev1.ExecEvent_Exit{DurationMs: time.Since(start).Milliseconds()}
	if ctx.Err() != nil {
		exit.TimedOut = true
	}
	if ps := cmd.ProcessState; ps != nil {
		ws := ps.Sys().(syscall.WaitStatus)
		switch {
		case ws.Exited() && !exit.TimedOut:
			exit.ExitCode = new(int32(ws.ExitStatus()))
		case ws.Signaled():
			exit.Signal = signalName(ws.Signal())
		}
	} else if waitErr != nil {
		return status.Errorf(codes.Internal, "wait: %v", waitErr)
	}
	return send(&nodev1.ExecEvent{Event: &nodev1.ExecEvent_Exit_{Exit: exit}})
}

func signalName(s syscall.Signal) string {
	switch s {
	case syscall.SIGKILL:
		return "SIGKILL"
	case syscall.SIGTERM:
		return "SIGTERM"
	case syscall.SIGINT:
		return "SIGINT"
	case syscall.SIGSEGV:
		return "SIGSEGV"
	case syscall.SIGABRT:
		return "SIGABRT"
	}
	return fmt.Sprintf("SIG%d", int(s))
}

func (n *Node) PutFile(stream grpc.ClientStreamingServer[nodev1.PutFileRequest, nodev1.PutFileResponse]) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	hdr := first.GetHeader()
	if hdr == nil {
		return status.Error(codes.InvalidArgument, "first PutFile message must be the header")
	}
	dir, err := n.readyDir(hdr.GetSandboxId())
	if err != nil {
		return err
	}
	rel, canonical, err := guestPath(hdr.GetPath())
	if err != nil {
		return err
	}
	if canonical == "/home/user" || canonical == "/tmp" {
		return status.Errorf(codes.InvalidArgument, "path %q is a directory", canonical)
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return status.Errorf(codes.Internal, "open vm root: %v", err)
	}
	defer root.Close()
	if err := root.MkdirAll(path.Dir(rel), 0o755); err != nil {
		return status.Errorf(codes.InvalidArgument, "create parents: %v", err)
	}
	mode := fs.FileMode(hdr.GetMode()) & fs.ModePerm
	if mode == 0 {
		mode = 0o644
	}
	tmp := rel + ".bbraft-put"
	f, err := root.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, mode)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "open: %v", err)
	}
	var size int64
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			f.Close()
			root.Remove(tmp)
			return err
		}
		k, err := f.Write(msg.GetData())
		size += int64(k)
		if err != nil {
			f.Close()
			root.Remove(tmp)
			return status.Errorf(codes.Internal, "write: %v", err)
		}
	}
	if err := f.Chmod(mode); err != nil { // OpenFile's mode is filtered by umask
		f.Close()
		root.Remove(tmp)
		return status.Errorf(codes.Internal, "chmod: %v", err)
	}
	if err := f.Close(); err != nil {
		root.Remove(tmp)
		return status.Errorf(codes.Internal, "close: %v", err)
	}
	if err := root.Rename(tmp, rel); err != nil {
		root.Remove(tmp)
		return status.Errorf(codes.Internal, "rename: %v", err)
	}
	return stream.SendAndClose(&nodev1.PutFileResponse{Path: canonical, Size: size})
}

func (n *Node) SaveSnapshot(_ context.Context, req *nodev1.SaveSnapshotRequest) (*nodev1.Snapshot, error) {
	if req.GetSnapshotId() == "" {
		return nil, status.Error(codes.InvalidArgument, "snapshot_id is required")
	}
	dir, err := n.snapshotDir(req.GetSandboxId())
	if err != nil {
		return nil, err
	}
	n.mu.Lock()
	if _, ok := n.snaps[req.SnapshotId]; ok {
		n.mu.Unlock()
		return nil, status.Errorf(codes.AlreadyExists, "snapshot %q exists", req.SnapshotId)
	}
	n.mu.Unlock()
	if err := os.CopyFS(n.snapDir(req.SnapshotId), os.DirFS(dir)); err != nil {
		os.RemoveAll(n.snapDir(req.SnapshotId))
		return nil, status.Errorf(codes.Internal, "copy: %v", err)
	}
	s := &nodev1.Snapshot{
		SnapshotId:      req.SnapshotId,
		SourceSandboxId: req.SandboxId,
		Image:           Image,
		CreatedAtUnixMs: time.Now().UnixMilli(),
	}
	n.mu.Lock()
	n.snaps[req.SnapshotId] = s
	n.mu.Unlock()
	return s, nil
}

func (n *Node) DeleteSnapshot(_ context.Context, req *nodev1.DeleteSnapshotRequest) (*nodev1.DeleteSnapshotResponse, error) {
	n.mu.Lock()
	delete(n.snaps, req.GetSnapshotId())
	n.mu.Unlock()
	if req.GetSnapshotId() != "" {
		if err := os.RemoveAll(n.snapDir(req.GetSnapshotId())); err != nil {
			return nil, status.Errorf(codes.Internal, "remove: %v", err)
		}
	}
	return &nodev1.DeleteSnapshotResponse{}, nil
}

func cloneVM(vm *nodev1.VM) *nodev1.VM {
	return &nodev1.VM{
		SandboxId:       vm.SandboxId,
		State:           vm.State,
		Image:           vm.Image,
		FromSnapshot:    vm.FromSnapshot,
		Resources:       vm.Resources,
		CreatedAtUnixMs: vm.CreatedAtUnixMs,
		Error:           vm.Error,
		StartedAtUnixMs: vm.StartedAtUnixMs,
	}
}
