// Package nodetest is the conformance suite for nodev1.Node. Every
// implementation must pass it: nodefake in-process, the real bbraft-node on a
// KVM host (tests gated by BBRAFT_TEST_NODE), and both through nodelink.
//
// The suite uses only relative paths inside commands, so it also holds for
// the fake node, whose commands run on the host.
package nodetest

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/ids"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// Options tune the suite for slow implementations.
type Options struct {
	// Resources for every VM the suite creates. Default 2 vCPU, 2 GiB, 10 GiB.
	Resources *nodev1.Resources
	// BootTimeout bounds each CreateVM. Default 2 minutes.
	BootTimeout time.Duration
	// FenceWatch is how long the DeleteVM fence cases watch for a late boot
	// bringing the VM back. Default 3 s; real nodes should use at least
	// their typical boot time.
	FenceWatch time.Duration
	// Phase1 selects the Phase 1 case groups (phase1.go).
	Phase1 Phase1
	// Phase2 selects the Phase 2 case groups and the object store (phase2.go).
	Phase2 Phase2
}

// Run runs the suite against client.
func Run(t *testing.T, client nodev1.NodeClient, opts Options) {
	if opts.Resources == nil {
		opts.Resources = &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: 10}
	}
	if opts.BootTimeout <= 0 {
		opts.BootTimeout = 2 * time.Minute
	}
	if opts.FenceWatch <= 0 {
		opts.FenceWatch = 3 * time.Second
	}
	s := &suite{c: client, opts: opts}

	t.Run("Info", s.testInfo)
	t.Run("Lifecycle", s.testLifecycle)
	t.Run("UnknownSandbox", s.testUnknownSandbox)
	t.Run("DeleteFencesCancelledBoot", func(t *testing.T) { s.testDeleteFencesBoot(t, true) })
	t.Run("DeleteFencesBoot", func(t *testing.T) { s.testDeleteFencesBoot(t, false) })

	// One shared VM for the command and file cases.
	vm := s.create(t, &nodev1.CreateVMRequest{Env: map[string]string{"BBRAFT_VM_VAR": "from-vm", "BBRAFT_OVERRIDE": "vm"}})
	id := vm.SandboxId
	t.Run("ExecStdout", func(t *testing.T) { s.testExecStdout(t, id) })
	t.Run("ExecExitCode", func(t *testing.T) { s.testExecExitCode(t, id) })
	t.Run("ExecStderr", func(t *testing.T) { s.testExecStderr(t, id) })
	t.Run("ExecEnv", func(t *testing.T) { s.testExecEnv(t, id) })
	t.Run("ExecCwd", func(t *testing.T) { s.testExecCwd(t, id) })
	t.Run("ExecTimeout", func(t *testing.T) { s.testExecTimeout(t, id) })
	t.Run("ExecCancel", func(t *testing.T) { s.testExecCancel(t, id) })
	t.Run("ExecLargeOutput", func(t *testing.T) { s.testExecLargeOutput(t, id) })
	t.Run("PutFile", func(t *testing.T) { s.testPutFile(t, id) })
	t.Run("PutFileMode", func(t *testing.T) { s.testPutFileMode(t, id) })
	t.Run("PutFileLarge", func(t *testing.T) { s.testPutFileLarge(t, id) })
	t.Run("PutFileBadPath", func(t *testing.T) { s.testPutFileBadPath(t, id) })
	t.Run("Snapshot", func(t *testing.T) { s.testSnapshot(t, id) })
	s.runPhase1(t, id)
	s.runPhase2(t)
}

type suite struct {
	c    nodev1.NodeClient
	opts Options
}

// create boots a VM and deletes it when the test ends.
func (s *suite) create(t *testing.T, req *nodev1.CreateVMRequest) *nodev1.VM {
	t.Helper()
	if req.SandboxId == "" {
		req.SandboxId = ids.Sandbox()
	}
	if req.Resources == nil {
		req.Resources = s.opts.Resources
	}
	ctx, cancel := context.WithTimeout(t.Context(), s.opts.BootTimeout)
	defer cancel()
	vm, err := s.c.CreateVM(ctx, req)
	if err != nil {
		t.Fatalf("CreateVM(%s): %v", req.SandboxId, err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if _, err := s.c.DeleteVM(ctx, &nodev1.DeleteVMRequest{SandboxId: req.SandboxId}); err != nil {
			t.Errorf("cleanup DeleteVM(%s): %v", req.SandboxId, err)
		}
	})
	if vm.State != nodev1.VMState_VM_STATE_READY {
		t.Fatalf("CreateVM returned state %s, want READY", vm.State)
	}
	if vm.SandboxId != req.SandboxId {
		t.Fatalf("CreateVM returned id %q, want %q", vm.SandboxId, req.SandboxId)
	}
	return vm
}

// Result is the collected outcome of one Exec.
type Result struct {
	Pid    int32
	Stdout string
	Stderr string
	Exit   *nodev1.ExecEvent_Exit
}

// Exec runs a command and collects its stream, checking the frame order.
func Exec(ctx context.Context, c nodev1.NodeClient, req *nodev1.ExecRequest) (*Result, error) {
	st, err := c.Exec(ctx, req)
	if err != nil {
		return nil, err
	}
	var out, errb bytes.Buffer
	res := &Result{}
	started := false
	for {
		ev, err := st.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		if res.Exit != nil {
			return nil, errors.New("frame after exit")
		}
		switch e := ev.Event.(type) {
		case *nodev1.ExecEvent_Started_:
			if started {
				return nil, errors.New("two started frames")
			}
			started = true
			res.Pid = e.Started.GetPid()
		case *nodev1.ExecEvent_Stdout:
			if !started {
				return nil, errors.New("stdout before started")
			}
			out.Write(e.Stdout)
		case *nodev1.ExecEvent_Stderr:
			if !started {
				return nil, errors.New("stderr before started")
			}
			errb.Write(e.Stderr)
		case *nodev1.ExecEvent_Exit_:
			if !started {
				return nil, errors.New("exit before started")
			}
			res.Exit = e.Exit
		default:
			return nil, errors.New("empty event")
		}
	}
	if res.Exit == nil {
		return nil, errors.New("stream ended without exit")
	}
	res.Stdout, res.Stderr = out.String(), errb.String()
	return res, nil
}

func (s *suite) exec(t *testing.T, id, cmd string) *Result {
	t.Helper()
	res, err := Exec(t.Context(), s.c, &nodev1.ExecRequest{SandboxId: id, Command: cmd, TimeoutSeconds: 60})
	if err != nil {
		t.Fatalf("Exec(%q): %v", cmd, err)
	}
	return res
}

func wantCode(t *testing.T, err error, code codes.Code) {
	t.Helper()
	if got := status.Code(err); got != code {
		t.Fatalf("got %v (%v), want code %v", got, err, code)
	}
}

func (s *suite) testInfo(t *testing.T) {
	info, err := s.c.Info(t.Context(), &nodev1.InfoRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if info.NodeId == "" || info.Vcpus == 0 || info.MemoryMib == 0 {
		t.Fatalf("incomplete info: %v", info)
	}
	if info.DefaultImage == "" {
		t.Fatal("no default image")
	}
	found := false
	for _, img := range info.Images {
		found = found || img.Id == info.DefaultImage
	}
	if !found {
		t.Fatalf("default image %q not in images %v", info.DefaultImage, info.Images)
	}
}

func (s *suite) testLifecycle(t *testing.T) {
	ctx := t.Context()
	vm := s.create(t, &nodev1.CreateVMRequest{})
	id := vm.SandboxId
	if vm.Image == "" || vm.CreatedAtUnixMs == 0 {
		t.Fatalf("incomplete VM: %v", vm)
	}

	again, err := s.c.CreateVM(ctx, &nodev1.CreateVMRequest{SandboxId: id, Resources: s.opts.Resources})
	if err != nil {
		t.Fatalf("second CreateVM: %v", err)
	}
	if again.State != nodev1.VMState_VM_STATE_READY || again.CreatedAtUnixMs != vm.CreatedAtUnixMs {
		t.Fatalf("second CreateVM made a new VM: %v vs %v", again, vm)
	}

	// Existing ids ignore all other fields, including fields that would fail
	// validation for a genuinely new VM.
	ignored, err := s.c.CreateVM(ctx, &nodev1.CreateVMRequest{
		SandboxId:    id,
		Image:        "not-installed",
		FromSnapshot: "not-saved",
		Hostname:     "invalid hostname",
		Env:          map[string]string{"IGNORED": "value"},
	})
	if err != nil {
		t.Fatalf("existing id with ignored fields: %v", err)
	}
	if ignored.State != nodev1.VMState_VM_STATE_READY || ignored.CreatedAtUnixMs != vm.CreatedAtUnixMs || ignored.Image != vm.Image {
		t.Fatalf("existing id with ignored fields changed VM: %v vs %v", ignored, vm)
	}

	got, err := s.c.GetVM(ctx, &nodev1.GetVMRequest{SandboxId: id})
	if err != nil || got.State != nodev1.VMState_VM_STATE_READY {
		t.Fatalf("GetVM = %v, %v", got, err)
	}
	list, err := s.c.ListVMs(ctx, &nodev1.ListVMsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if !containsVM(list.Vms, id) {
		t.Fatalf("ListVMs misses %s", id)
	}

	if _, err := s.c.DeleteVM(ctx, &nodev1.DeleteVMRequest{SandboxId: id}); err != nil {
		t.Fatal(err)
	}
	_, err = s.c.GetVM(ctx, &nodev1.GetVMRequest{SandboxId: id})
	wantCode(t, err, codes.NotFound)
	list, err = s.c.ListVMs(ctx, &nodev1.ListVMsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if containsVM(list.Vms, id) {
		t.Fatalf("ListVMs still has %s after delete", id)
	}
	if _, err := s.c.DeleteVM(ctx, &nodev1.DeleteVMRequest{SandboxId: id}); err != nil {
		t.Fatalf("second DeleteVM: %v", err)
	}
}

// testDeleteFencesBoot checks the DeleteVM fence: a boot that started before
// DeleteVM returned never brings the VM back afterwards, whether its caller
// gave up (cancelled) or is still waiting.
func (s *suite) testDeleteFencesBoot(t *testing.T, cancelCaller bool) {
	id := ids.Sandbox()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	errc := make(chan error, 1)
	go func() {
		_, err := s.c.CreateVM(ctx, &nodev1.CreateVMRequest{SandboxId: id, Resources: s.opts.Resources})
		errc <- err
	}()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		s.c.DeleteVM(ctx, &nodev1.DeleteVMRequest{SandboxId: id})
	})

	// Wait until the node knows the VM (usually still BOOTING).
	deadline := time.Now().Add(s.opts.BootTimeout)
	for {
		if _, err := s.c.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id}); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("CreateVM never registered the VM")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if cancelCaller {
		cancel()
		<-errc
	}
	if _, err := s.c.DeleteVM(t.Context(), &nodev1.DeleteVMRequest{SandboxId: id}); err != nil {
		t.Fatalf("DeleteVM: %v", err)
	}
	if !cancelCaller {
		// The waiting CreateVM ends: ABORTED, or OK if the boot won the race.
		select {
		case err := <-errc:
			if c := status.Code(err); c != codes.OK && c != codes.Aborted {
				t.Fatalf("CreateVM fenced by DeleteVM: %v, want ABORTED or OK", err)
			}
		case <-time.After(time.Minute):
			t.Fatal("CreateVM still blocked after DeleteVM returned")
		}
	}
	// The VM must stay gone: no late boot may recreate it.
	until := time.Now().Add(s.opts.FenceWatch)
	for time.Now().Before(until) {
		_, err := s.c.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id})
		wantCode(t, err, codes.NotFound)
		list, err := s.c.ListVMs(t.Context(), &nodev1.ListVMsRequest{})
		if err != nil {
			t.Fatal(err)
		}
		if containsVM(list.Vms, id) {
			t.Fatalf("ListVMs shows %s after DeleteVM", id)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func containsVM(vms []*nodev1.VM, id string) bool {
	for _, vm := range vms {
		if vm.SandboxId == id {
			return true
		}
	}
	return false
}

func (s *suite) testUnknownSandbox(t *testing.T) {
	ctx := t.Context()
	id := ids.Sandbox()
	_, err := s.c.GetVM(ctx, &nodev1.GetVMRequest{SandboxId: id})
	wantCode(t, err, codes.NotFound)
	_, err = Exec(ctx, s.c, &nodev1.ExecRequest{SandboxId: id, Command: "true"})
	wantCode(t, err, codes.NotFound)
	_, err = s.c.CreateVM(ctx, &nodev1.CreateVMRequest{SandboxId: ids.Sandbox(), Resources: s.opts.Resources, FromSnapshot: "no-such-snapshot"})
	wantCode(t, err, codes.NotFound)
	if _, err := s.c.DeleteSnapshot(ctx, &nodev1.DeleteSnapshotRequest{SnapshotId: "no-such-snapshot"}); err != nil {
		t.Fatalf("DeleteSnapshot of unknown id: %v", err)
	}
}

func (s *suite) testExecStdout(t *testing.T, id string) {
	res := s.exec(t, id, "echo hello; printf 'a\\nb'")
	if res.Stdout != "hello\na\nb" || res.Stderr != "" {
		t.Fatalf("stdout %q stderr %q", res.Stdout, res.Stderr)
	}
	if res.Exit.ExitCode == nil || *res.Exit.ExitCode != 0 || res.Exit.TimedOut || res.Exit.Signal != "" {
		t.Fatalf("exit %v", res.Exit)
	}
	if res.Pid <= 0 {
		t.Fatalf("pid %d", res.Pid)
	}
}

func (s *suite) testExecExitCode(t *testing.T, id string) {
	res := s.exec(t, id, "exit 7")
	if res.Exit.ExitCode == nil || *res.Exit.ExitCode != 7 {
		t.Fatalf("exit %v, want code 7", res.Exit)
	}
}

func (s *suite) testExecStderr(t *testing.T, id string) {
	res := s.exec(t, id, "echo out; echo err >&2")
	if res.Stdout != "out\n" || res.Stderr != "err\n" {
		t.Fatalf("stdout %q stderr %q", res.Stdout, res.Stderr)
	}
}

func (s *suite) testExecEnv(t *testing.T, id string) {
	res, err := Exec(t.Context(), s.c, &nodev1.ExecRequest{
		SandboxId: id,
		Command:   `echo "$BBRAFT_VM_VAR $BBRAFT_OVERRIDE $BBRAFT_REQ_VAR"; [ "$(basename "$HOME")" = user ] && echo home-ok`,
		Env:       map[string]string{"BBRAFT_REQ_VAR": "from-req", "BBRAFT_OVERRIDE": "req"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Stdout != "from-vm req from-req\nhome-ok\n" {
		t.Fatalf("stdout %q", res.Stdout)
	}
}

func (s *suite) testExecCwd(t *testing.T, id string) {
	s.exec(t, id, "mkdir -p cwdtest/sub && echo x > cwdtest/sub/f")
	for _, cwd := range []string{"cwdtest/sub", "/home/user/cwdtest/sub"} {
		res, err := Exec(t.Context(), s.c, &nodev1.ExecRequest{SandboxId: id, Command: "cat f", Cwd: cwd})
		if err != nil {
			t.Fatalf("cwd %q: %v", cwd, err)
		}
		if res.Stdout != "x\n" {
			t.Fatalf("cwd %q: stdout %q stderr %q", cwd, res.Stdout, res.Stderr)
		}
	}
	res, err := Exec(t.Context(), s.c, &nodev1.ExecRequest{SandboxId: id, Command: "true", Cwd: "/etc"})
	if err == nil {
		t.Fatalf("cwd /etc accepted: %v", res.Exit)
	}
	wantCode(t, err, codes.InvalidArgument)
}

func (s *suite) testExecTimeout(t *testing.T, id string) {
	start := time.Now()
	res, err := Exec(t.Context(), s.c, &nodev1.ExecRequest{SandboxId: id, Command: "echo before; sleep 30 & sleep 30; echo after", TimeoutSeconds: 1})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Exit.TimedOut || res.Exit.ExitCode != nil {
		t.Fatalf("exit %v, want timed out without exit code", res.Exit)
	}
	if !strings.Contains(res.Stdout, "before") || strings.Contains(res.Stdout, "after") {
		t.Fatalf("stdout %q", res.Stdout)
	}
	if d := time.Since(start); d > 15*time.Second {
		t.Fatalf("timeout took %v (background child kept the stream open?)", d)
	}
}

func (s *suite) testExecCancel(t *testing.T, id string) {
	ctx, cancel := context.WithCancel(t.Context())
	st, err := s.c.Exec(ctx, &nodev1.ExecRequest{SandboxId: id, Command: "echo $$ > cancel.pid; sleep 30; touch cancel-finished"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Recv(); err != nil { // started
		t.Fatal(err)
	}
	// Wait until the shell wrote its pid, then cancel the RPC.
	deadline := time.Now().Add(10 * time.Second)
	for {
		if r := s.exec(t, id, "test -s cancel.pid && echo yes; true"); r.Stdout == "yes\n" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("command never wrote cancel.pid")
		}
		time.Sleep(100 * time.Millisecond)
	}
	cancel()
	for {
		if _, err := st.Recv(); err != nil {
			break
		}
	}
	// The whole process group must die soon after the cancel.
	deadline = time.Now().Add(10 * time.Second)
	for {
		r := s.exec(t, id, "kill -0 $(cat cancel.pid) 2>/dev/null && echo alive; test -e cancel-finished && echo finished; true")
		if r.Stdout == "" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("after cancel: %q", r.Stdout)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func (s *suite) testExecLargeOutput(t *testing.T, id string) {
	res := s.exec(t, id, "head -c 3000000 /dev/zero | tr '\\0' a")
	if len(res.Stdout) != 3000000 || strings.Trim(res.Stdout, "a") != "" {
		t.Fatalf("got %d bytes of stdout", len(res.Stdout))
	}
}

// PutFile sends header + data chunks of chunk bytes (chunk <= 0: one chunk).
func PutFile(ctx context.Context, c nodev1.NodeClient, id, path string, mode uint32, data []byte, chunk int) (*nodev1.PutFileResponse, error) {
	if chunk <= 0 {
		chunk = max(len(data), 1)
	}
	st, err := c.PutFile(ctx)
	if err != nil {
		return nil, err
	}
	if err := st.Send(&nodev1.PutFileRequest{Part: &nodev1.PutFileRequest_Header_{Header: &nodev1.PutFileRequest_Header{SandboxId: id, Path: path, Mode: mode}}}); err != nil {
		return nil, err
	}
	for len(data) > 0 {
		n := min(chunk, len(data))
		if err := st.Send(&nodev1.PutFileRequest{Part: &nodev1.PutFileRequest_Data{Data: data[:n]}}); err != nil {
			break // the real error comes from CloseAndRecv
		}
		data = data[n:]
	}
	return st.CloseAndRecv()
}

func (s *suite) testPutFile(t *testing.T, id string) {
	ctx := t.Context()
	resp, err := PutFile(ctx, s.c, id, "put/dir/hello.txt", 0, []byte("hello file\n"), 4)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Path != "/home/user/put/dir/hello.txt" || resp.Size != 11 {
		t.Fatalf("resp %v", resp)
	}
	res := s.exec(t, id, "cat put/dir/hello.txt; stat -c %a put/dir/hello.txt; stat -c %U put/dir/hello.txt put/dir")
	// Owner is `user` in a real guest; the fake runs as whoever runs the test.
	lines := strings.Split(res.Stdout, "\n")
	if len(lines) < 3 || lines[0] != "hello file" || lines[1] != "644" {
		t.Fatalf("got %q", res.Stdout)
	}
	if _, err := PutFile(ctx, s.c, id, "/home/user/put/dir/hello.txt", 0, []byte("replaced"), 1<<20); err != nil {
		t.Fatal(err)
	}
	if res := s.exec(t, id, "cat put/dir/hello.txt"); res.Stdout != "replaced" {
		t.Fatalf("after replace: %q", res.Stdout)
	}
	if _, err := PutFile(ctx, s.c, id, "/tmp/bbraft-nodetest-file", 0, []byte("tmp"), 1<<20); err != nil {
		t.Fatalf("PutFile under /tmp: %v", err)
	}
	empty, err := PutFile(ctx, s.c, id, "put/empty", 0, nil, 1)
	if err != nil || empty.Size != 0 {
		t.Fatalf("empty file: %v, %v", empty, err)
	}
}

func (s *suite) testPutFileMode(t *testing.T, id string) {
	if _, err := PutFile(t.Context(), s.c, id, "put/run.sh", 0o755, []byte("#!/bin/sh\necho ran\n"), 1<<20); err != nil {
		t.Fatal(err)
	}
	if res := s.exec(t, id, "./put/run.sh"); res.Stdout != "ran\n" {
		t.Fatalf("stdout %q stderr %q exit %v", res.Stdout, res.Stderr, res.Exit)
	}
}

func (s *suite) testPutFileLarge(t *testing.T, id string) {
	data := bytes.Repeat([]byte("0123456789abcdef"), 1<<20) // 16 MiB
	resp, err := PutFile(t.Context(), s.c, id, "put/large.bin", 0, data, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Size != int64(len(data)) {
		t.Fatalf("size %d", resp.Size)
	}
	if res := s.exec(t, id, "wc -c < put/large.bin"); strings.TrimSpace(res.Stdout) != "16777216" {
		t.Fatalf("wc %q", res.Stdout)
	}
}

func (s *suite) testPutFileBadPath(t *testing.T, id string) {
	for _, p := range []string{"/etc/passwd", "../escape", "/home/user/../../etc/x", "/home/userx/f", ""} {
		_, err := PutFile(t.Context(), s.c, id, p, 0, []byte("x"), 1<<20)
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("PutFile(%q): %v, want InvalidArgument", p, err)
		}
	}
}

func (s *suite) testSnapshot(t *testing.T, id string) {
	ctx := t.Context()
	s.exec(t, id, "mkdir -p snap && echo before-snapshot > snap/state")
	snapID := "nodetest-" + strings.TrimPrefix(ids.Sandbox(), "bx_")
	snap, err := s.c.SaveSnapshot(ctx, &nodev1.SaveSnapshotRequest{SandboxId: id, SnapshotId: snapID})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if _, err := s.c.DeleteSnapshot(ctx, &nodev1.DeleteSnapshotRequest{SnapshotId: snapID}); err != nil {
			t.Errorf("cleanup DeleteSnapshot: %v", err)
		}
	})
	if snap.SnapshotId != snapID || snap.SourceSandboxId != id || snap.CreatedAtUnixMs == 0 {
		t.Fatalf("snapshot %v", snap)
	}
	_, err = s.c.SaveSnapshot(ctx, &nodev1.SaveSnapshotRequest{SandboxId: id, SnapshotId: snapID})
	wantCode(t, err, codes.AlreadyExists)

	// The source keeps running and changes after the snapshot stay its own.
	s.exec(t, id, "echo after-snapshot > snap/state")
	info, err := s.c.Info(ctx, &nodev1.InfoRequest{})
	if err != nil {
		t.Fatal(err)
	}
	listed := false
	for _, sn := range info.Snapshots {
		listed = listed || sn.SnapshotId == snapID
	}
	if !listed {
		t.Fatalf("Info misses snapshot %s", snapID)
	}

	child := s.create(t, &nodev1.CreateVMRequest{FromSnapshot: snapID})
	if child.FromSnapshot != snapID {
		t.Fatalf("child from_snapshot %q", child.FromSnapshot)
	}
	if res := s.exec(t, child.SandboxId, "cat snap/state"); res.Stdout != "before-snapshot\n" {
		t.Fatalf("child sees %q", res.Stdout)
	}
	if res := s.exec(t, id, "cat snap/state"); res.Stdout != "after-snapshot\n" {
		t.Fatalf("source sees %q", res.Stdout)
	}
}
