package nodefake

import (
	"bytes"
	"cmp"
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"io"
	"io/fs"
	"maps"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// HostKey is the fake guest's SSH host public key (a well-formed ed25519
// line with a fixed blob). AddSSHKey returns it; no sshd runs behind it.
var HostKey = func() string {
	blob := binary.BigEndian.AppendUint32(nil, 11)
	blob = append(blob, "ssh-ed25519"...)
	blob = binary.BigEndian.AppendUint32(blob, 32)
	blob = append(blob, bytes.Repeat([]byte{0xbb}, 32)...)
	return "ssh-ed25519 " + base64.StdEncoding.EncodeToString(blob) + " fake-host"
}()

// processDir is where detached process logs live, relative to the VM dir
// (the guest path is /home/user/.bbraft/processes).
const processDir = "home/user/.bbraft/processes"

// proc is one detached process of a fake VM.
type proc struct {
	rec  *nodev1.Process // guarded by Node.mu
	cmd  *exec.Cmd
	done chan struct{} // closed once rec is final
	// lost is set by StopVM/DeleteVM before killing, so the waiter records
	// LOST instead of the kill's signal.
	lost bool
}

// --- Stop and start ----------------------------------------------------------

func (n *Node) StopVM(_ context.Context, req *nodev1.StopVMRequest) (*nodev1.VM, error) {
	id := req.GetSandboxId()
	n.mu.Lock()
	vm, ok := n.vms[id]
	if !ok {
		n.mu.Unlock()
		return nil, status.Errorf(codes.NotFound, "sandbox %s not found", id)
	}
	switch vm.State {
	case nodev1.VMState_VM_STATE_STOPPED:
		out := cloneVM(vm)
		n.mu.Unlock()
		return out, nil
	case nodev1.VMState_VM_STATE_BOOTING:
		n.mu.Unlock()
		return nil, status.Errorf(codes.FailedPrecondition, "sandbox %s is booting", id)
	case nodev1.VMState_VM_STATE_STOPPING:
		n.mu.Unlock()
		return n.waitState(id, vm, nodev1.VMState_VM_STATE_STOPPED)
	}
	vm.State = nodev1.VMState_VM_STATE_STOPPING
	n.mu.Unlock()

	n.killProcs(id, true)

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.vms[id] != vm {
		return nil, status.Errorf(codes.Aborted, "sandbox %s was deleted while stopping", id)
	}
	vm.State = nodev1.VMState_VM_STATE_STOPPED
	vm.Error = ""
	return cloneVM(vm), nil
}

// waitState polls until vm reaches want (or is replaced or deleted).
func (n *Node) waitState(id string, vm *nodev1.VM, want nodev1.VMState) (*nodev1.VM, error) {
	for {
		n.mu.Lock()
		current := n.vms[id] == vm
		out := cloneVM(vm)
		n.mu.Unlock()
		if !current {
			return nil, status.Errorf(codes.Aborted, "sandbox %s was deleted", id)
		}
		if out.State == want {
			return out, nil
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (n *Node) StartVM(ctx context.Context, req *nodev1.StartVMRequest) (*nodev1.VM, error) {
	id := req.GetSandboxId()
	for {
		n.mu.Lock()
		vm, ok := n.vms[id]
		if !ok {
			n.mu.Unlock()
			return nil, status.Errorf(codes.NotFound, "sandbox %s not found", id)
		}
		switch vm.State {
		case nodev1.VMState_VM_STATE_READY, nodev1.VMState_VM_STATE_BOOTING:
			n.mu.Unlock()
			return n.waitReady(ctx, vm)
		case nodev1.VMState_VM_STATE_STOPPING:
			n.mu.Unlock()
			if _, err := n.waitState(id, vm, nodev1.VMState_VM_STATE_STOPPED); err != nil {
				return nil, err
			}
			continue
		case nodev1.VMState_VM_STATE_FAILED:
			n.mu.Unlock()
			return nil, status.Errorf(codes.FailedPrecondition, "sandbox %s failed; stop it first", id)
		}
		// STOPPED
		res := vm.GetResources()
		if r := req.GetResources(); r != nil {
			if r.GetDiskGib() < res.GetDiskGib() {
				n.mu.Unlock()
				return nil, status.Errorf(codes.InvalidArgument, "disk cannot shrink from %d to %d GiB", res.GetDiskGib(), r.GetDiskGib())
			}
			res = r
		}
		if !n.roomLocked(id, res) {
			n.mu.Unlock()
			return nil, status.Error(codes.ResourceExhausted, "no room for this VM")
		}
		vm.Resources = res
		vm.State = nodev1.VMState_VM_STATE_BOOTING
		n.env[id] = maps.Clone(req.GetEnv())
		bctx, cancel := context.WithCancel(context.Background())
		bt := &booting{cancel: cancel, done: make(chan struct{})}
		n.boots[id] = bt
		n.mu.Unlock()

		go func() {
			defer close(bt.done)
			defer cancel()
			var err error
			if n.BootDelay > 0 {
				select {
				case <-time.After(n.BootDelay):
				case <-bctx.Done():
					err = bctx.Err()
				}
			}
			n.mu.Lock()
			defer n.mu.Unlock()
			delete(n.boots, id)
			if n.vms[id] != vm {
				return
			}
			if err != nil {
				vm.State = nodev1.VMState_VM_STATE_FAILED
				vm.Error = err.Error()
				return
			}
			n.markLostLocked(id)
			vm.State = nodev1.VMState_VM_STATE_READY
			vm.StartedAtUnixMs = time.Now().UnixMilli()
		}()
		return n.waitReady(ctx, vm)
	}
}

// killProcs kills every running detached process of a VM and waits for it.
// With lost, the processes record LOST (the VM stopped) instead of a signal.
func (n *Node) killProcs(id string, lost bool) {
	n.mu.Lock()
	var wait []*proc
	for _, p := range n.procs[id] {
		if p.rec.State == nodev1.ProcessState_PROCESS_STATE_RUNNING {
			p.lost = lost
			syscall.Kill(-p.cmd.Process.Pid, syscall.SIGKILL)
			wait = append(wait, p)
		}
	}
	n.mu.Unlock()
	for _, p := range wait {
		<-p.done
	}
}

// markLostLocked turns any process without an exit into LOST (after a
// restart nothing of the previous boot runs). Callers hold n.mu.
func (n *Node) markLostLocked(id string) {
	for _, p := range n.procs[id] {
		if p.rec.State == nodev1.ProcessState_PROCESS_STATE_RUNNING {
			p.rec.State = nodev1.ProcessState_PROCESS_STATE_LOST
		}
	}
}

// --- Detached processes --------------------------------------------------------

// commandEnv is Exec's environment: the fake's base variables, then the VM's
// env, then the call's.
func (n *Node) commandEnv(dir, id string, extra map[string]string) []string {
	env := []string{
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + filepath.Join(dir, "home/user"),
		"USER=user",
		"LANG=C.UTF-8",
	}
	n.mu.Lock()
	merged := maps.Clone(n.env[id])
	n.mu.Unlock()
	if merged == nil {
		merged = map[string]string{}
	}
	maps.Copy(merged, extra)
	for _, k := range slices.Sorted(maps.Keys(merged)) {
		env = append(env, k+"="+merged[k])
	}
	return env
}

func logNames(pid uint32) (out, errLog string) {
	s := strconv.FormatUint(uint64(pid), 10)
	return path.Join(processDir, s+".log"), path.Join(processDir, s+".err.log")
}

func (n *Node) StartProcess(_ context.Context, req *nodev1.StartProcessRequest) (*nodev1.Process, error) {
	id, pid := req.GetSandboxId(), req.GetProcessId()
	dir, err := n.readyDir(id)
	if err != nil {
		return nil, err
	}
	if pid == 0 {
		return nil, status.Error(codes.InvalidArgument, "process_id must be at least 1")
	}
	n.mu.Lock()
	if p, ok := n.procs[id][pid]; ok {
		out := cloneProcess(p.rec)
		n.mu.Unlock()
		return out, nil
	}
	n.mu.Unlock()

	cwd := cmp.Or(req.GetCwd(), "/home/user")
	rel, canonical, err := guestPath(cwd)
	if err != nil {
		return nil, err
	}
	outRel, errRel := logNames(pid)
	if err := os.MkdirAll(filepath.Join(dir, processDir), 0o755); err != nil {
		return nil, status.Errorf(codes.Internal, "log dir: %v", err)
	}
	stdout, err := os.Create(filepath.Join(dir, outRel))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "log: %v", err)
	}
	defer stdout.Close()
	stderr, err := os.Create(filepath.Join(dir, errRel))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "log: %v", err)
	}
	defer stderr.Close()

	cmd := exec.Command("bash", "-c", req.GetCommand())
	cmd.Dir = filepath.Join(dir, rel)
	cmd.Env = n.commandEnv(dir, id, req.GetEnv())
	cmd.Stdout, cmd.Stderr = stdout, stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	start := time.Now()
	if err := cmd.Start(); err != nil {
		return nil, status.Errorf(codes.Internal, "start: %v", err)
	}
	p := &proc{
		cmd:  cmd,
		done: make(chan struct{}),
		rec: &nodev1.Process{
			ProcessId:       pid,
			Pid:             int32(cmd.Process.Pid),
			State:           nodev1.ProcessState_PROCESS_STATE_RUNNING,
			Command:         req.GetCommand(),
			Cwd:             canonical,
			StartedAtUnixMs: start.UnixMilli(),
			LogPath:         "/" + outRel,
			ErrLogPath:      "/" + errRel,
		},
	}
	n.mu.Lock()
	if n.procs[id] == nil {
		n.procs[id] = map[uint32]*proc{}
	}
	if other, ok := n.procs[id][pid]; ok { // lost a race with the same id
		n.mu.Unlock()
		syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		cmd.Wait()
		return n.processOf(id, other.rec.ProcessId)
	}
	n.procs[id][pid] = p
	out := cloneProcess(p.rec)
	n.mu.Unlock()

	go func() {
		cmd.Wait()
		n.mu.Lock()
		defer n.mu.Unlock()
		defer close(p.done)
		p.rec.FinishedAtUnixMs = time.Now().UnixMilli()
		if p.lost {
			p.rec.State = nodev1.ProcessState_PROCESS_STATE_LOST
			p.rec.FinishedAtUnixMs = 0
			return
		}
		p.rec.State = nodev1.ProcessState_PROCESS_STATE_EXITED
		exit := &nodev1.ExecEvent_Exit{DurationMs: time.Since(start).Milliseconds()}
		ws := cmd.ProcessState.Sys().(syscall.WaitStatus)
		switch {
		case ws.Exited():
			exit.ExitCode = new(int32(ws.ExitStatus()))
		case ws.Signaled():
			exit.Signal = signalName(ws.Signal())
		}
		p.rec.Exit = exit
	}()
	return out, nil
}

// processOf returns a copy of a process record with its current log sizes.
func (n *Node) processOf(id string, pid uint32) (*nodev1.Process, error) {
	n.mu.Lock()
	p, ok := n.procs[id][pid]
	if !ok {
		n.mu.Unlock()
		return nil, status.Errorf(codes.NotFound, "process %d not found", pid)
	}
	out := cloneProcess(p.rec)
	n.mu.Unlock()
	dir := n.vmDir(id)
	outRel, errRel := logNames(pid)
	out.StdoutSize = fileSize(filepath.Join(dir, outRel))
	out.StderrSize = fileSize(filepath.Join(dir, errRel))
	return out, nil
}

func fileSize(p string) int64 {
	fi, err := os.Stat(p)
	if err != nil {
		return 0
	}
	return fi.Size()
}

// processVM checks the VM is READY and the process exists.
func (n *Node) processVM(id string, pid uint32) (string, error) {
	dir, err := n.readyDir(id)
	if err != nil {
		return "", err
	}
	if _, err := n.processOf(id, pid); err != nil {
		return "", err
	}
	return dir, nil
}

const (
	defaultLimit = 1 << 20
	maxLimit     = 8 << 20
)

func (n *Node) GetProcess(_ context.Context, req *nodev1.GetProcessRequest) (*nodev1.ProcessOutput, error) {
	id, pid := req.GetSandboxId(), req.GetProcessId()
	dir, err := n.processVM(id, pid)
	if err != nil {
		return nil, err
	}
	limit := int64(cmp.Or(req.GetLimitBytes(), defaultLimit))
	if limit > maxLimit {
		return nil, status.Errorf(codes.InvalidArgument, "limit_bytes %d is above %d", limit, maxLimit)
	}
	rec, err := n.processOf(id, pid)
	if err != nil {
		return nil, err
	}
	outRel, errRel := logNames(pid)
	res := &nodev1.ProcessOutput{Process: rec}
	var more bool
	res.Stdout, res.StdoutOffset, res.StdoutTruncated, more = window(filepath.Join(dir, outRel), req.StdoutOffset, limit)
	res.More = more
	res.Stderr, res.StderrOffset, res.StderrTruncated, more = window(filepath.Join(dir, errRel), req.StderrOffset, limit)
	res.More = res.More || more
	return res, nil
}

// window reads one log window: the tail when offset is nil, else from offset.
func window(p string, offset *int64, limit int64) (data []byte, start int64, truncated, more bool) {
	all, _ := os.ReadFile(p)
	size := int64(len(all))
	if offset == nil {
		start = max(size-limit, 0)
		return all[start:], start, start > 0, false
	}
	start = min(max(*offset, 0), size)
	end := min(start+limit, size)
	return all[start:end], start, false, end < size
}

func (n *Node) AttachProcess(req *nodev1.AttachProcessRequest, stream grpc.ServerStreamingServer[nodev1.ProcessEvent]) error {
	id, pid := req.GetSandboxId(), req.GetProcessId()
	dir, err := n.processVM(id, pid)
	if err != nil {
		return err
	}
	rec, err := n.processOf(id, pid)
	if err != nil {
		return err
	}
	outOff := min(max(req.GetStdoutOffset(), 0), rec.StdoutSize)
	errOff := min(max(req.GetStderrOffset(), 0), rec.StderrSize)
	if err := stream.Send(&nodev1.ProcessEvent{Event: &nodev1.ProcessEvent_Attached_{Attached: &nodev1.ProcessEvent_Attached{
		Process: rec, StdoutOffset: outOff, StderrOffset: errOff,
	}}}); err != nil {
		return err
	}
	n.mu.Lock()
	done := n.procs[id][pid].done
	n.mu.Unlock()
	outRel, errRel := logNames(pid)
	pump := func(rel string, off *int64, wrap func(*nodev1.ProcessEvent_Chunk) *nodev1.ProcessEvent) error {
		all, _ := os.ReadFile(filepath.Join(dir, rel))
		for int64(len(all)) > *off {
			end := min(*off+defaultLimit, int64(len(all)))
			chunk := &nodev1.ProcessEvent_Chunk{Data: all[*off:end], NextOffset: end}
			if err := stream.Send(wrap(chunk)); err != nil {
				return err
			}
			*off = end
		}
		return nil
	}
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		ended := false
		select {
		case <-done:
			ended = true
		default:
		}
		if err := pump(outRel, &outOff, func(c *nodev1.ProcessEvent_Chunk) *nodev1.ProcessEvent {
			return &nodev1.ProcessEvent{Event: &nodev1.ProcessEvent_Stdout{Stdout: c}}
		}); err != nil {
			return err
		}
		if err := pump(errRel, &errOff, func(c *nodev1.ProcessEvent_Chunk) *nodev1.ProcessEvent {
			return &nodev1.ProcessEvent{Event: &nodev1.ProcessEvent_Stderr{Stderr: c}}
		}); err != nil {
			return err
		}
		if ended {
			final, err := n.processOf(id, pid)
			if err != nil {
				return err
			}
			return stream.Send(&nodev1.ProcessEvent{Event: &nodev1.ProcessEvent_Exit{Exit: final}})
		}
		select {
		case <-tick.C:
		case <-done:
		case <-stream.Context().Done():
			return status.FromContextError(stream.Context().Err()).Err()
		}
	}
}

func (n *Node) KillProcess(_ context.Context, req *nodev1.KillProcessRequest) (*nodev1.KillProcessResponse, error) {
	id, pid := req.GetSandboxId(), req.GetProcessId()
	if _, err := n.processVM(id, pid); err != nil {
		return nil, err
	}
	n.mu.Lock()
	p := n.procs[id][pid]
	running := p.rec.State == nodev1.ProcessState_PROCESS_STATE_RUNNING
	n.mu.Unlock()
	if running {
		syscall.Kill(-p.cmd.Process.Pid, syscall.SIGTERM)
		grace := time.Duration(cmp.Or(req.GetGraceSeconds(), 5)) * time.Second
		select {
		case <-p.done:
		case <-time.After(grace):
			syscall.Kill(-p.cmd.Process.Pid, syscall.SIGKILL)
			<-p.done
		}
	}
	rec, err := n.processOf(id, pid)
	if err != nil {
		return nil, err
	}
	return &nodev1.KillProcessResponse{Process: rec, Signaled: running}, nil
}

func cloneProcess(p *nodev1.Process) *nodev1.Process {
	out := &nodev1.Process{
		ProcessId:        p.ProcessId,
		Pid:              p.Pid,
		State:            p.State,
		Command:          p.Command,
		Cwd:              p.Cwd,
		StartedAtUnixMs:  p.StartedAtUnixMs,
		FinishedAtUnixMs: p.FinishedAtUnixMs,
		StdoutSize:       p.StdoutSize,
		StderrSize:       p.StderrSize,
		LogPath:          p.LogPath,
		ErrLogPath:       p.ErrLogPath,
	}
	if e := p.Exit; e != nil {
		out.Exit = &nodev1.ExecEvent_Exit{Signal: e.Signal, TimedOut: e.TimedOut, OomKilled: e.OomKilled, DurationMs: e.DurationMs}
		if e.ExitCode != nil {
			out.Exit.ExitCode = new(*e.ExitCode)
		}
	}
	return out
}

// --- Files -----------------------------------------------------------------------

// fileRoot opens a READY VM's directory as an os.Root and resolves p.
func (n *Node) fileRoot(id, p string) (root *os.Root, rel, canonical string, err error) {
	dir, err := n.readyDir(id)
	if err != nil {
		return nil, "", "", err
	}
	rel, canonical, err = guestPath(p)
	if err != nil {
		return nil, "", "", err
	}
	root, err = os.OpenRoot(dir)
	if err != nil {
		return nil, "", "", status.Errorf(codes.Internal, "open vm root: %v", err)
	}
	return root, rel, canonical, nil
}

func isRootPath(canonical string) bool { return canonical == "/home/user" || canonical == "/tmp" }

// fileErr maps a filesystem error to the contract's codes.
func fileErr(op, p string, err error) error {
	// The errno cases come first: syscall.ENOTEMPTY also matches fs.ErrExist.
	switch {
	case errors.Is(err, syscall.ENOTDIR), errors.Is(err, syscall.EISDIR), errors.Is(err, syscall.ENOTEMPTY),
		errors.Is(err, fs.ErrPermission), strings.Contains(err.Error(), "path escapes"):
		return status.Errorf(codes.InvalidArgument, "%s %s: %v", op, p, err)
	case errors.Is(err, fs.ErrNotExist):
		return status.Errorf(codes.NotFound, "%s %s: no such file or directory", op, p)
	case errors.Is(err, fs.ErrExist):
		return status.Errorf(codes.AlreadyExists, "%s %s: exists", op, p)
	}
	return status.Errorf(codes.Internal, "%s %s: %v", op, p, err)
}

// unixMode converts fs.FileMode permission and special bits to 0o7777.
func unixMode(m fs.FileMode) uint32 {
	u := uint32(m.Perm())
	if m&fs.ModeSetuid != 0 {
		u |= 0o4000
	}
	if m&fs.ModeSetgid != 0 {
		u |= 0o2000
	}
	if m&fs.ModeSticky != 0 {
		u |= 0o1000
	}
	return u
}

// fileMode is unixMode's inverse.
func fileMode(u uint32) fs.FileMode {
	m := fs.FileMode(u & 0o777)
	if u&0o4000 != 0 {
		m |= fs.ModeSetuid
	}
	if u&0o2000 != 0 {
		m |= fs.ModeSetgid
	}
	if u&0o1000 != 0 {
		m |= fs.ModeSticky
	}
	return m
}

// info builds a FileInfo for rel (canonical guest path p): lstat for the
// entry, the target for a symlink that stays inside the allowed roots.
func info(root *os.Root, rel, p string) (*nodev1.FileInfo, error) {
	lfi, err := root.Lstat(rel)
	if err != nil {
		return nil, err
	}
	fi := lfi
	out := &nodev1.FileInfo{Path: p, Name: path.Base(p)}
	if lfi.Mode()&fs.ModeSymlink != 0 {
		out.IsSymlink = true
		fi = nil
		if target, err := root.Readlink(rel); err == nil {
			if !path.IsAbs(target) {
				target = path.Join(path.Dir(p), target)
			}
			if trel, _, err := guestPath(target); err == nil {
				if tfi, err := root.Stat(trel); err == nil {
					fi = tfi
				}
			}
		}
		if fi == nil {
			out.Type = nodev1.FileType_FILE_TYPE_OTHER
			out.Size = lfi.Size()
			out.Mode = unixMode(lfi.Mode())
			out.ModifiedAtUnixMs = lfi.ModTime().UnixMilli()
			return out, nil
		}
	}
	switch {
	case fi.Mode().IsRegular():
		out.Type = nodev1.FileType_FILE_TYPE_REGULAR
	case fi.IsDir():
		out.Type = nodev1.FileType_FILE_TYPE_DIRECTORY
	default:
		out.Type = nodev1.FileType_FILE_TYPE_OTHER
	}
	out.Size = fi.Size()
	out.Mode = unixMode(fi.Mode())
	out.ModifiedAtUnixMs = fi.ModTime().UnixMilli()
	return out, nil
}

func (n *Node) ReadFile(req *nodev1.ReadFileRequest, stream grpc.ServerStreamingServer[nodev1.ReadFileResponse]) error {
	root, rel, p, err := n.fileRoot(req.GetSandboxId(), req.GetPath())
	if err != nil {
		return err
	}
	defer root.Close()
	fi, err := info(root, rel, p)
	if err != nil {
		return fileErr("read", p, err)
	}
	if fi.Type != nodev1.FileType_FILE_TYPE_REGULAR {
		return status.Errorf(codes.InvalidArgument, "read %s: not a regular file", p)
	}
	if m := req.GetMaxBytes(); m > 0 && fi.Size > m {
		return status.Errorf(codes.OutOfRange, "read %s: %d bytes is above the %d byte limit", p, fi.Size, m)
	}
	f, err := root.Open(rel)
	if err != nil {
		return fileErr("read", p, err)
	}
	defer f.Close()
	if err := stream.Send(&nodev1.ReadFileResponse{Part: &nodev1.ReadFileResponse_Info{Info: fi}}); err != nil {
		return err
	}
	buf := make([]byte, 1<<20)
	for {
		k, err := f.Read(buf)
		if k > 0 {
			if err := stream.Send(&nodev1.ReadFileResponse{Part: &nodev1.ReadFileResponse_Data{Data: bytes.Clone(buf[:k])}}); err != nil {
				return err
			}
		}
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return status.Errorf(codes.Internal, "read %s: %v", p, err)
		}
	}
}

func (n *Node) StatFile(_ context.Context, req *nodev1.StatFileRequest) (*nodev1.FileInfo, error) {
	root, rel, p, err := n.fileRoot(req.GetSandboxId(), req.GetPath())
	if err != nil {
		return nil, err
	}
	defer root.Close()
	fi, err := info(root, rel, p)
	if err != nil {
		return nil, fileErr("stat", p, err)
	}
	return fi, nil
}

func (n *Node) ListFiles(_ context.Context, req *nodev1.ListFilesRequest) (*nodev1.ListFilesResponse, error) {
	limit := int(cmp.Or(req.GetLimit(), 1000))
	if limit > 10000 {
		return nil, status.Errorf(codes.InvalidArgument, "limit %d is above 10000", limit)
	}
	root, rel, p, err := n.fileRoot(req.GetSandboxId(), cmp.Or(req.GetPath(), "/home/user"))
	if err != nil {
		return nil, err
	}
	defer root.Close()
	dfi, err := info(root, rel, p)
	if err != nil {
		return nil, fileErr("list", p, err)
	}
	if dfi.Type != nodev1.FileType_FILE_TYPE_DIRECTORY {
		return nil, status.Errorf(codes.InvalidArgument, "list %s: not a directory", p)
	}
	entries, err := fs.ReadDir(root.FS(), rel)
	if err != nil {
		return nil, fileErr("list", p, err)
	}
	out := &nodev1.ListFilesResponse{Path: p}
	for _, e := range entries { // fs.ReadDir sorts by name
		if len(out.Entries) == limit {
			out.Truncated = true
			break
		}
		fi, err := info(root, path.Join(rel, e.Name()), path.Join(p, e.Name()))
		if err != nil {
			continue // removed meanwhile
		}
		out.Entries = append(out.Entries, fi)
	}
	return out, nil
}

func (n *Node) DeleteFile(_ context.Context, req *nodev1.DeleteFileRequest) (*nodev1.DeleteFileResponse, error) {
	root, rel, p, err := n.fileRoot(req.GetSandboxId(), req.GetPath())
	if err != nil {
		return nil, err
	}
	defer root.Close()
	if isRootPath(p) {
		return nil, status.Errorf(codes.InvalidArgument, "delete %s: cannot delete a root", p)
	}
	if _, err := root.Lstat(rel); err != nil {
		return nil, fileErr("delete", p, err)
	}
	if req.GetRecursive() {
		err = root.RemoveAll(rel)
	} else {
		err = root.Remove(rel)
	}
	if err != nil {
		return nil, fileErr("delete", p, err)
	}
	return &nodev1.DeleteFileResponse{Path: p}, nil
}

func (n *Node) MakeDir(_ context.Context, req *nodev1.MakeDirRequest) (*nodev1.FileInfo, error) {
	root, rel, p, err := n.fileRoot(req.GetSandboxId(), req.GetPath())
	if err != nil {
		return nil, err
	}
	defer root.Close()
	mode := cmp.Or(req.GetMode(), 0o755)
	if mode > 0o7777 {
		return nil, status.Errorf(codes.InvalidArgument, "mode %o is above 7777", mode)
	}
	if fi, err := root.Lstat(rel); err == nil {
		if !fi.IsDir() {
			return nil, status.Errorf(codes.AlreadyExists, "mkdir %s: exists and is not a directory", p)
		}
		return info(root, rel, p)
	}
	if err := root.MkdirAll(path.Dir(rel), 0o755); err != nil {
		return nil, fileErr("mkdir", p, err)
	}
	if err := root.Mkdir(rel, 0o700); err != nil && !errors.Is(err, fs.ErrExist) {
		return nil, fileErr("mkdir", p, err)
	}
	if err := root.Chmod(rel, fileMode(mode)); err != nil { // Mkdir's mode is filtered by umask
		return nil, fileErr("mkdir", p, err)
	}
	return info(root, rel, p)
}

func (n *Node) MoveFile(_ context.Context, req *nodev1.MoveFileRequest) (*nodev1.MoveFileResponse, error) {
	root, srcRel, src, err := n.fileRoot(req.GetSandboxId(), req.GetSource())
	if err != nil {
		return nil, err
	}
	defer root.Close()
	dstRel, dst, err := guestPath(req.GetDestination())
	if err != nil {
		return nil, err
	}
	if isRootPath(src) || isRootPath(dst) {
		return nil, status.Errorf(codes.InvalidArgument, "move %s: cannot move a root", src)
	}
	if _, err := root.Lstat(srcRel); err != nil {
		return nil, fileErr("move", src, err)
	}
	if dfi, err := root.Lstat(dstRel); err == nil {
		if !req.GetOverwrite() {
			return nil, status.Errorf(codes.AlreadyExists, "move %s: %s exists", src, dst)
		}
		if dfi.IsDir() {
			if err := root.Remove(dstRel); err != nil { // only an empty directory
				return nil, fileErr("move", dst, err)
			}
		}
	}
	if err := root.MkdirAll(path.Dir(dstRel), 0o755); err != nil {
		return nil, fileErr("move", dst, err)
	}
	if err := root.Rename(srcRel, dstRel); err != nil {
		return nil, fileErr("move", src, err)
	}
	return &nodev1.MoveFileResponse{Source: src, Destination: dst}, nil
}

func (n *Node) ChmodFile(_ context.Context, req *nodev1.ChmodFileRequest) (*nodev1.FileInfo, error) {
	if req.GetMode() > 0o7777 {
		return nil, status.Errorf(codes.InvalidArgument, "mode %o is above 7777", req.GetMode())
	}
	root, rel, p, err := n.fileRoot(req.GetSandboxId(), req.GetPath())
	if err != nil {
		return nil, err
	}
	defer root.Close()
	if err := root.Chmod(rel, fileMode(req.GetMode())); err != nil {
		return nil, fileErr("chmod", p, err)
	}
	return info(root, rel, p)
}

// --- SSH ---------------------------------------------------------------------------

// ValidPublicKey reports whether line is one OpenSSH public key: a known key
// type, its base64 blob (whose embedded type matches) and an optional
// comment, with no options and no line breaks. Exported for implementations
// that want the same check as the fake; it does not verify the key math.
func ValidPublicKey(line string) bool {
	if strings.ContainsAny(line, "\r\n\x00") {
		return false
	}
	fields := strings.Fields(line)
	if len(fields) < 2 {
		return false
	}
	switch fields[0] {
	case "ssh-ed25519", "ssh-rsa", "ecdsa-sha2-nistp256", "ecdsa-sha2-nistp384", "ecdsa-sha2-nistp521",
		"sk-ssh-ed25519@openssh.com", "sk-ecdsa-sha2-nistp256@openssh.com":
	default:
		return false
	}
	blob, err := base64.StdEncoding.DecodeString(fields[1])
	if err != nil || len(blob) < 4 {
		return false
	}
	k := binary.BigEndian.Uint32(blob)
	return uint64(k)+4 <= uint64(len(blob)) && string(blob[4:4+k]) == fields[0]
}

func (n *Node) AddSSHKey(_ context.Context, req *nodev1.AddSSHKeyRequest) (*nodev1.SSHAccess, error) {
	dir, err := n.readyDir(req.GetSandboxId())
	if err != nil {
		return nil, err
	}
	key := strings.TrimSpace(req.GetPublicKey())
	if !ValidPublicKey(key) {
		return nil, status.Error(codes.InvalidArgument, "public_key is not one OpenSSH public key line")
	}
	sshDir := filepath.Join(dir, "home/user/.ssh")
	if err := os.MkdirAll(sshDir, 0o700); err != nil {
		return nil, status.Errorf(codes.Internal, "ssh dir: %v", err)
	}
	authPath := filepath.Join(sshDir, "authorized_keys")
	n.mu.Lock() // serialize appends per node; the fake is not performance-bound
	defer n.mu.Unlock()
	have, _ := os.ReadFile(authPath)
	f := strings.Fields(key)
	for line := range strings.SplitSeq(string(have), "\n") {
		if g := strings.Fields(line); len(g) >= 2 && g[0] == f[0] && g[1] == f[1] {
			return n.sshAccess(), nil
		}
	}
	if len(have) > 0 && !bytes.HasSuffix(have, []byte("\n")) {
		have = append(have, '\n')
	}
	have = append(have, key+"\n"...)
	if err := os.WriteFile(authPath, have, 0o600); err != nil {
		return nil, status.Errorf(codes.Internal, "authorized_keys: %v", err)
	}
	return n.sshAccess(), nil
}

func (n *Node) sshAccess() *nodev1.SSHAccess {
	return &nodev1.SSHAccess{User: "user", Host: "127.0.0.1", Port: 22, HostKey: HostKey}
}
