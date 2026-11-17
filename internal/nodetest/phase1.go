package nodetest

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"

	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// Phase1 selects the Phase 1 case groups. An implementation turns a group on
// once it implements those RPCs; by the end of Phase 1 every implementation
// runs with all of them (nodefake does from the start).
type Phase1 struct {
	Stop      bool // StopVM, StartVM
	Processes bool // StartProcess, GetProcess, AttachProcess, KillProcess
	Files     bool // ReadFile, StatFile, ListFiles, DeleteFile, MakeDir, MoveFile, ChmodFile
	SSH       bool // AddSSHKey
}

// AllPhase1 turns on every Phase 1 group.
var AllPhase1 = Phase1{Stop: true, Processes: true, Files: true, SSH: true}

func (s *suite) runPhase1(t *testing.T, id string) {
	p := s.opts.Phase1
	if p.Files {
		t.Run("Files", func(t *testing.T) { s.testFiles(t, id) })
		t.Run("FilesErrors", func(t *testing.T) { s.testFilesErrors(t, id) })
		t.Run("FilesSymlinkEscape", func(t *testing.T) { s.testFilesSymlinkEscape(t, id) })
	}
	if p.Processes {
		t.Run("Process", func(t *testing.T) { s.testProcess(t, id) })
		t.Run("ProcessWindows", func(t *testing.T) { s.testProcessWindows(t, id) })
		t.Run("ProcessKill", func(t *testing.T) { s.testProcessKill(t, id) })
		t.Run("ProcessErrors", func(t *testing.T) { s.testProcessErrors(t, id) })
	}
	if p.SSH {
		t.Run("SSHKey", func(t *testing.T) { s.testSSHKey(t, id) })
	}
	if p.Stop {
		t.Run("StopStart", s.testStopStart)
		t.Run("StopUnknown", s.testStopUnknown)
	}
	if p.Stop && p.Processes {
		t.Run("ProcessLostAfterStop", s.testProcessLostAfterStop)
	}
}

// ctx returns a per-call context bounded by the boot timeout.
func (s *suite) ctx(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(t.Context(), s.opts.BootTimeout)
	t.Cleanup(cancel)
	return ctx
}

// --- Files ----------------------------------------------------------------------

// ReadFile collects one ReadFile stream, checking that info comes first.
func ReadFile(ctx context.Context, c nodev1.NodeClient, req *nodev1.ReadFileRequest) (*nodev1.FileInfo, []byte, error) {
	st, err := c.ReadFile(ctx, req)
	if err != nil {
		return nil, nil, err
	}
	var fi *nodev1.FileInfo
	var data bytes.Buffer
	for {
		msg, err := st.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, nil, err
		}
		switch part := msg.Part.(type) {
		case *nodev1.ReadFileResponse_Info:
			if fi != nil {
				return nil, nil, errors.New("second info message")
			}
			fi = part.Info
		case *nodev1.ReadFileResponse_Data:
			if fi == nil {
				return nil, nil, errors.New("data before info")
			}
			if len(part.Data) > 1<<20 {
				return nil, nil, errors.New("data chunk above 1 MiB")
			}
			data.Write(part.Data)
		}
	}
	if fi == nil {
		return nil, nil, errors.New("no info message")
	}
	return fi, data.Bytes(), nil
}

func (s *suite) testFiles(t *testing.T, id string) {
	ctx := s.ctx(t)
	payload := bytes.Repeat([]byte("0123456789abcdef"), (3<<20)/16) // 3 MiB: several chunks
	if _, err := PutFile(ctx, s.c, id, "p1files/a.txt", 0o640, payload, 1<<20); err != nil {
		t.Fatalf("PutFile: %v", err)
	}
	fi, data, err := ReadFile(ctx, s.c, &nodev1.ReadFileRequest{SandboxId: id, Path: "p1files/a.txt"})
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !bytes.Equal(data, payload) {
		t.Fatalf("ReadFile returned %d bytes, want the %d written", len(data), len(payload))
	}
	if fi.Path != "/home/user/p1files/a.txt" || fi.Name != "a.txt" || fi.Type != nodev1.FileType_FILE_TYPE_REGULAR ||
		fi.Size != int64(len(payload)) || fi.Mode != 0o640 || fi.IsSymlink || fi.ModifiedAtUnixMs == 0 {
		t.Fatalf("ReadFile info = %v", fi)
	}
	if _, _, err := ReadFile(ctx, s.c, &nodev1.ReadFileRequest{SandboxId: id, Path: "p1files/a.txt", MaxBytes: 1024}); err == nil {
		t.Fatal("ReadFile above max_bytes succeeded")
	} else {
		wantCode(t, err, codes.OutOfRange)
	}

	st, err := s.c.StatFile(ctx, &nodev1.StatFileRequest{SandboxId: id, Path: "/home/user/p1files"})
	if err != nil {
		t.Fatalf("StatFile: %v", err)
	}
	if st.Type != nodev1.FileType_FILE_TYPE_DIRECTORY || st.Path != "/home/user/p1files" || st.Name != "p1files" {
		t.Fatalf("StatFile = %v", st)
	}

	// MakeDir: parents, mode, idempotent (keeps the mode).
	md, err := s.c.MakeDir(ctx, &nodev1.MakeDirRequest{SandboxId: id, Path: "p1files/x/y/z", Mode: 0o700})
	if err != nil {
		t.Fatalf("MakeDir: %v", err)
	}
	if md.Type != nodev1.FileType_FILE_TYPE_DIRECTORY || md.Mode != 0o700 || md.Path != "/home/user/p1files/x/y/z" {
		t.Fatalf("MakeDir = %v", md)
	}
	if again, err := s.c.MakeDir(ctx, &nodev1.MakeDirRequest{SandboxId: id, Path: "p1files/x/y/z", Mode: 0o755}); err != nil || again.Mode != 0o700 {
		t.Fatalf("MakeDir again = %v, %v; want OK with the mode kept (0700)", again, err)
	}
	_, err = s.c.MakeDir(ctx, &nodev1.MakeDirRequest{SandboxId: id, Path: "p1files/a.txt"})
	wantCode(t, err, codes.AlreadyExists)

	// ListFiles: sorted, limit, truncated.
	for _, name := range []string{"c", "b"} {
		if _, err := PutFile(ctx, s.c, id, "p1files/"+name, 0, []byte(name), 0); err != nil {
			t.Fatalf("PutFile %s: %v", name, err)
		}
	}
	ls, err := s.c.ListFiles(ctx, &nodev1.ListFilesRequest{SandboxId: id, Path: "p1files"})
	if err != nil {
		t.Fatalf("ListFiles: %v", err)
	}
	var names []string
	for _, e := range ls.Entries {
		names = append(names, e.Name)
	}
	if want := []string{"a.txt", "b", "c", "x"}; !slices.Equal(names, want) || ls.Truncated || ls.Path != "/home/user/p1files" {
		t.Fatalf("ListFiles = %v (truncated %v, path %q), want %v", names, ls.Truncated, ls.Path, want)
	}
	if ls.Entries[3].Type != nodev1.FileType_FILE_TYPE_DIRECTORY || ls.Entries[1].Size != 1 {
		t.Fatalf("ListFiles entries = %v", ls.Entries)
	}
	ls, err = s.c.ListFiles(ctx, &nodev1.ListFilesRequest{SandboxId: id, Path: "p1files", Limit: 2})
	if err != nil || len(ls.Entries) != 2 || !ls.Truncated {
		t.Fatalf("ListFiles limit 2 = %v, %v; want 2 entries, truncated", ls, err)
	}

	// MoveFile: parents created, refuses an existing target unless overwrite.
	mv, err := s.c.MoveFile(ctx, &nodev1.MoveFileRequest{SandboxId: id, Source: "p1files/b", Destination: "/tmp/p1moved/b2"})
	if err != nil {
		t.Fatalf("MoveFile: %v", err)
	}
	if mv.Source != "/home/user/p1files/b" || mv.Destination != "/tmp/p1moved/b2" {
		t.Fatalf("MoveFile = %v", mv)
	}
	_, err = s.c.MoveFile(ctx, &nodev1.MoveFileRequest{SandboxId: id, Source: "p1files/c", Destination: "/tmp/p1moved/b2"})
	wantCode(t, err, codes.AlreadyExists)
	if _, err := s.c.MoveFile(ctx, &nodev1.MoveFileRequest{SandboxId: id, Source: "p1files/c", Destination: "/tmp/p1moved/b2", Overwrite: true}); err != nil {
		t.Fatalf("MoveFile overwrite: %v", err)
	}
	if _, data, err := ReadFile(ctx, s.c, &nodev1.ReadFileRequest{SandboxId: id, Path: "/tmp/p1moved/b2"}); err != nil || string(data) != "c" {
		t.Fatalf("moved file = %q, %v; want \"c\"", data, err)
	}

	// ChmodFile, including the special bits.
	ch, err := s.c.ChmodFile(ctx, &nodev1.ChmodFileRequest{SandboxId: id, Path: "p1files/a.txt", Mode: 0o1600})
	if err != nil || ch.Mode != 0o1600 {
		t.Fatalf("ChmodFile = %v, %v; want mode 1600", ch, err)
	}
	_, err = s.c.ChmodFile(ctx, &nodev1.ChmodFileRequest{SandboxId: id, Path: "p1files/a.txt", Mode: 0o10000})
	wantCode(t, err, codes.InvalidArgument)

	// DeleteFile: a non-empty directory needs recursive.
	_, err = s.c.DeleteFile(ctx, &nodev1.DeleteFileRequest{SandboxId: id, Path: "p1files"})
	wantCode(t, err, codes.InvalidArgument)
	if del, err := s.c.DeleteFile(ctx, &nodev1.DeleteFileRequest{SandboxId: id, Path: "p1files/a.txt"}); err != nil || del.Path != "/home/user/p1files/a.txt" {
		t.Fatalf("DeleteFile file = %v, %v", del, err)
	}
	if _, err := s.c.DeleteFile(ctx, &nodev1.DeleteFileRequest{SandboxId: id, Path: "p1files", Recursive: true}); err != nil {
		t.Fatalf("DeleteFile recursive: %v", err)
	}
	_, err = s.c.StatFile(ctx, &nodev1.StatFileRequest{SandboxId: id, Path: "p1files"})
	wantCode(t, err, codes.NotFound)
}

func (s *suite) testFilesErrors(t *testing.T, id string) {
	ctx := s.ctx(t)
	if _, err := PutFile(ctx, s.c, id, "p1err/f", 0, []byte("f"), 0); err != nil {
		t.Fatalf("PutFile: %v", err)
	}
	for _, tc := range []struct {
		name string
		call func() error
		want codes.Code
	}{
		{"read missing", func() error {
			_, _, err := ReadFile(ctx, s.c, &nodev1.ReadFileRequest{SandboxId: id, Path: "p1err/missing"})
			return err
		}, codes.NotFound},
		{"read directory", func() error {
			_, _, err := ReadFile(ctx, s.c, &nodev1.ReadFileRequest{SandboxId: id, Path: "p1err"})
			return err
		}, codes.InvalidArgument},
		{"read outside", func() error {
			_, _, err := ReadFile(ctx, s.c, &nodev1.ReadFileRequest{SandboxId: id, Path: "/etc/passwd"})
			return err
		}, codes.InvalidArgument},
		{"read dot-dot", func() error {
			_, _, err := ReadFile(ctx, s.c, &nodev1.ReadFileRequest{SandboxId: id, Path: "../../etc/passwd"})
			return err
		}, codes.InvalidArgument},
		{"stat missing", func() error {
			_, err := s.c.StatFile(ctx, &nodev1.StatFileRequest{SandboxId: id, Path: "p1err/missing"})
			return err
		}, codes.NotFound},
		{"list file", func() error {
			_, err := s.c.ListFiles(ctx, &nodev1.ListFilesRequest{SandboxId: id, Path: "p1err/f"})
			return err
		}, codes.InvalidArgument},
		{"list limit", func() error {
			_, err := s.c.ListFiles(ctx, &nodev1.ListFilesRequest{SandboxId: id, Path: "p1err", Limit: 10001})
			return err
		}, codes.InvalidArgument},
		{"delete home", func() error {
			_, err := s.c.DeleteFile(ctx, &nodev1.DeleteFileRequest{SandboxId: id, Path: "/home/user", Recursive: true})
			return err
		}, codes.InvalidArgument},
		{"delete tmp", func() error {
			_, err := s.c.DeleteFile(ctx, &nodev1.DeleteFileRequest{SandboxId: id, Path: "/tmp", Recursive: true})
			return err
		}, codes.InvalidArgument},
		{"delete missing", func() error {
			_, err := s.c.DeleteFile(ctx, &nodev1.DeleteFileRequest{SandboxId: id, Path: "p1err/missing"})
			return err
		}, codes.NotFound},
		{"move missing", func() error {
			_, err := s.c.MoveFile(ctx, &nodev1.MoveFileRequest{SandboxId: id, Source: "p1err/missing", Destination: "p1err/g"})
			return err
		}, codes.NotFound},
		{"move outside", func() error {
			_, err := s.c.MoveFile(ctx, &nodev1.MoveFileRequest{SandboxId: id, Source: "p1err/f", Destination: "/etc/f"})
			return err
		}, codes.InvalidArgument},
		{"move home", func() error {
			_, err := s.c.MoveFile(ctx, &nodev1.MoveFileRequest{SandboxId: id, Source: "/home/user", Destination: "/tmp/home"})
			return err
		}, codes.InvalidArgument},
		{"mkdir outside", func() error {
			_, err := s.c.MakeDir(ctx, &nodev1.MakeDirRequest{SandboxId: id, Path: "/var/p1"})
			return err
		}, codes.InvalidArgument},
		{"chmod missing", func() error {
			_, err := s.c.ChmodFile(ctx, &nodev1.ChmodFileRequest{SandboxId: id, Path: "p1err/missing", Mode: 0o644})
			return err
		}, codes.NotFound},
		{"unknown sandbox", func() error {
			_, err := s.c.StatFile(ctx, &nodev1.StatFileRequest{SandboxId: "bx_unknown0", Path: "f"})
			return err
		}, codes.NotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.call()
			if err == nil {
				t.Fatalf("succeeded, want %s", tc.want)
			}
			wantCode(t, err, tc.want)
		})
	}
}

// testFilesSymlinkEscape: a link pointing outside the roots is reported as
// a link of type OTHER and is never followed.
func (s *suite) testFilesSymlinkEscape(t *testing.T, id string) {
	ctx := s.ctx(t)
	if r := s.exec(t, id, "mkdir -p p1link && ln -sfn /etc/passwd p1link/out && ln -sfn ../p1link p1link/self"); r.Exit.GetExitCode() != 0 {
		t.Fatalf("ln: exit %v, stderr %q", r.Exit.ExitCode, r.Stderr)
	}
	fi, err := s.c.StatFile(ctx, &nodev1.StatFileRequest{SandboxId: id, Path: "p1link/out"})
	if err != nil {
		t.Fatalf("StatFile: %v", err)
	}
	if !fi.IsSymlink || fi.Type != nodev1.FileType_FILE_TYPE_OTHER {
		t.Fatalf("escaping link = %v, want is_symlink with type OTHER", fi)
	}
	_, _, err = ReadFile(ctx, s.c, &nodev1.ReadFileRequest{SandboxId: id, Path: "p1link/out"})
	if err == nil {
		t.Fatal("ReadFile followed a link outside the roots")
	}
	wantCode(t, err, codes.InvalidArgument)
	self, err := s.c.StatFile(ctx, &nodev1.StatFileRequest{SandboxId: id, Path: "p1link/self"})
	if err != nil || !self.IsSymlink || self.Type != nodev1.FileType_FILE_TYPE_DIRECTORY {
		t.Fatalf("inside link = %v, %v; want is_symlink with type DIRECTORY", self, err)
	}
	// Deleting a link removes the link, not the target.
	if _, err := s.c.DeleteFile(ctx, &nodev1.DeleteFileRequest{SandboxId: id, Path: "p1link/self"}); err != nil {
		t.Fatalf("DeleteFile link: %v", err)
	}
	if _, err := s.c.StatFile(ctx, &nodev1.StatFileRequest{SandboxId: id, Path: "p1link"}); err != nil {
		t.Fatalf("link target gone after deleting the link: %v", err)
	}
}

// --- Processes ------------------------------------------------------------------

// Attached is the collected outcome of one AttachProcess stream.
type Attached struct {
	First          *nodev1.ProcessEvent_Attached
	Stdout, Stderr string
	Exit           *nodev1.Process
}

// Attach collects an AttachProcess stream to its end, checking frame order
// and that chunk offsets are contiguous from the attached offsets.
func Attach(ctx context.Context, c nodev1.NodeClient, req *nodev1.AttachProcessRequest) (*Attached, error) {
	st, err := c.AttachProcess(ctx, req)
	if err != nil {
		return nil, err
	}
	res := &Attached{}
	var out, errb bytes.Buffer
	var outNext, errNext int64
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
		case *nodev1.ProcessEvent_Attached_:
			if res.First != nil {
				return nil, errors.New("second attached frame")
			}
			res.First = e.Attached
			outNext, errNext = e.Attached.StdoutOffset, e.Attached.StderrOffset
		case *nodev1.ProcessEvent_Stdout:
			if res.First == nil {
				return nil, errors.New("stdout before attached")
			}
			if e.Stdout.NextOffset != outNext+int64(len(e.Stdout.Data)) {
				return nil, errors.New("stdout chunk offsets are not contiguous")
			}
			outNext = e.Stdout.NextOffset
			out.Write(e.Stdout.Data)
		case *nodev1.ProcessEvent_Stderr:
			if res.First == nil {
				return nil, errors.New("stderr before attached")
			}
			if e.Stderr.NextOffset != errNext+int64(len(e.Stderr.Data)) {
				return nil, errors.New("stderr chunk offsets are not contiguous")
			}
			errNext = e.Stderr.NextOffset
			errb.Write(e.Stderr.Data)
		case *nodev1.ProcessEvent_Exit:
			if res.First == nil {
				return nil, errors.New("exit before attached")
			}
			res.Exit = e.Exit
		}
	}
	if res.Exit == nil {
		return nil, errors.New("stream ended without exit")
	}
	res.Stdout, res.Stderr = out.String(), errb.String()
	return res, nil
}

func (s *suite) testProcess(t *testing.T, id string) {
	ctx := s.ctx(t)
	req := &nodev1.StartProcessRequest{
		SandboxId: id, ProcessId: 101, Cwd: "/tmp",
		Command: `echo out-1; echo err-1 >&2; sleep 1; echo "out-2 $BBRAFT_VM_VAR $P1_VAR"; exit 3`,
		Env:     map[string]string{"P1_VAR": "from-start"},
	}
	p, err := s.c.StartProcess(ctx, req)
	if err != nil {
		t.Fatalf("StartProcess: %v", err)
	}
	if p.ProcessId != 101 || p.Pid <= 0 || p.Cwd != "/tmp" || p.Command != req.Command || p.StartedAtUnixMs == 0 ||
		p.LogPath == "" || p.ErrLogPath == "" || p.LogPath == p.ErrLogPath {
		t.Fatalf("StartProcess = %v", p)
	}
	if p.State != nodev1.ProcessState_PROCESS_STATE_RUNNING && p.State != nodev1.ProcessState_PROCESS_STATE_EXITED {
		t.Fatalf("StartProcess state = %s", p.State)
	}
	// Idempotent on process_id, whatever the other fields say.
	again, err := s.c.StartProcess(ctx, &nodev1.StartProcessRequest{SandboxId: id, ProcessId: 101, Command: "echo other"})
	if err != nil || again.Pid != p.Pid || again.Command != req.Command {
		t.Fatalf("StartProcess again = %v, %v; want the first process", again, err)
	}

	a, err := Attach(ctx, s.c, &nodev1.AttachProcessRequest{SandboxId: id, ProcessId: 101})
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	if want := "out-1\nout-2 from-vm from-start\n"; a.Stdout != want {
		t.Fatalf("attached stdout = %q, want %q", a.Stdout, want)
	}
	if a.Stderr != "err-1\n" {
		t.Fatalf("attached stderr = %q", a.Stderr)
	}
	e := a.Exit
	if e.State != nodev1.ProcessState_PROCESS_STATE_EXITED || e.Exit == nil || e.Exit.ExitCode == nil || *e.Exit.ExitCode != 3 ||
		e.FinishedAtUnixMs < e.StartedAtUnixMs || e.StdoutSize != int64(len(a.Stdout)) || e.StderrSize != int64(len(a.Stderr)) {
		t.Fatalf("exit = %v", e)
	}

	// Attaching to an ended process replays from the offsets and ends.
	a2, err := Attach(ctx, s.c, &nodev1.AttachProcessRequest{SandboxId: id, ProcessId: 101, StdoutOffset: 6, StderrOffset: 1 << 20})
	if err != nil {
		t.Fatalf("Attach again: %v", err)
	}
	if a2.First.StdoutOffset != 6 || a2.First.StderrOffset != int64(len("err-1\n")) || a2.Stdout != "out-2 from-vm from-start\n" || a2.Stderr != "" {
		t.Fatalf("re-attach = %+v", a2)
	}
}

func (s *suite) testProcessWindows(t *testing.T, id string) {
	ctx := s.ctx(t)
	if _, err := s.c.StartProcess(ctx, &nodev1.StartProcessRequest{SandboxId: id, ProcessId: 102, Command: "printf 0123456789; printf abc >&2"}); err != nil {
		t.Fatalf("StartProcess: %v", err)
	}
	if _, err := Attach(ctx, s.c, &nodev1.AttachProcessRequest{SandboxId: id, ProcessId: 102}); err != nil {
		t.Fatalf("Attach: %v", err)
	}
	get := func(req *nodev1.GetProcessRequest) *nodev1.ProcessOutput {
		t.Helper()
		req.SandboxId, req.ProcessId = id, 102
		out, err := s.c.GetProcess(ctx, req)
		if err != nil {
			t.Fatalf("GetProcess(%v): %v", req, err)
		}
		return out
	}
	// Tail mode.
	o := get(&nodev1.GetProcessRequest{LimitBytes: 4})
	if string(o.Stdout) != "6789" || o.StdoutOffset != 6 || !o.StdoutTruncated || string(o.Stderr) != "abc" || o.StderrTruncated || o.More {
		t.Fatalf("tail window = %+v", o)
	}
	o = get(&nodev1.GetProcessRequest{})
	if string(o.Stdout) != "0123456789" || o.StdoutTruncated || o.Process.State != nodev1.ProcessState_PROCESS_STATE_EXITED {
		t.Fatalf("full tail = %+v", o)
	}
	// Offset mode.
	o = get(&nodev1.GetProcessRequest{StdoutOffset: new(int64(2)), StderrOffset: new(int64(0)), LimitBytes: 5})
	if string(o.Stdout) != "23456" || o.StdoutOffset != 2 || !o.More || string(o.Stderr) != "abc" {
		t.Fatalf("offset window = %+v", o)
	}
	o = get(&nodev1.GetProcessRequest{StdoutOffset: new(int64(7)), StderrOffset: new(int64(3))})
	if string(o.Stdout) != "789" || o.More || len(o.Stderr) != 0 || o.StderrOffset != 3 {
		t.Fatalf("offset to end = %+v", o)
	}
	o = get(&nodev1.GetProcessRequest{StdoutOffset: new(int64(99)), StderrOffset: new(int64(99))})
	if len(o.Stdout) != 0 || o.StdoutOffset != 10 || o.More {
		t.Fatalf("offset past end = %+v", o)
	}
	_, err := s.c.GetProcess(ctx, &nodev1.GetProcessRequest{SandboxId: id, ProcessId: 102, LimitBytes: 8<<20 + 1})
	wantCode(t, err, codes.InvalidArgument)
}

func (s *suite) testProcessKill(t *testing.T, id string) {
	ctx := s.ctx(t)
	// The child keeps the group busy; TERM must end the whole group.
	if _, err := s.c.StartProcess(ctx, &nodev1.StartProcessRequest{SandboxId: id, ProcessId: 103, Command: "sleep 600 & sleep 600; wait"}); err != nil {
		t.Fatalf("StartProcess: %v", err)
	}
	k, err := s.c.KillProcess(ctx, &nodev1.KillProcessRequest{SandboxId: id, ProcessId: 103, GraceSeconds: 2})
	if err != nil {
		t.Fatalf("KillProcess: %v", err)
	}
	if !k.Signaled || k.Process.State != nodev1.ProcessState_PROCESS_STATE_EXITED || k.Process.Exit == nil ||
		k.Process.Exit.ExitCode != nil || k.Process.Exit.Signal == "" {
		t.Fatalf("KillProcess = %v; want signaled, EXITED by a signal", k)
	}
	k, err = s.c.KillProcess(ctx, &nodev1.KillProcessRequest{SandboxId: id, ProcessId: 103})
	if err != nil || k.Signaled {
		t.Fatalf("KillProcess again = %v, %v; want OK, not signaled", k, err)
	}
	if r := s.exec(t, id, fmt.Sprintf("pgrep -g %d || true", k.Process.Pid)); strings.TrimSpace(r.Stdout) != "" {
		t.Fatalf("processes of the killed group still run: %q", r.Stdout)
	}

	// A process that ignores TERM is killed after the grace period.
	if _, err := s.c.StartProcess(ctx, &nodev1.StartProcessRequest{SandboxId: id, ProcessId: 104, Command: "trap '' TERM; sleep 600"}); err != nil {
		t.Fatalf("StartProcess: %v", err)
	}
	time.Sleep(200 * time.Millisecond) // let bash install the trap
	start := time.Now()
	k, err = s.c.KillProcess(ctx, &nodev1.KillProcessRequest{SandboxId: id, ProcessId: 104, GraceSeconds: 1})
	if err != nil || !k.Signaled || k.Process.Exit.GetSignal() != "SIGKILL" {
		t.Fatalf("KillProcess TERM-ignoring = %v, %v; want SIGKILL", k, err)
	}
	if d := time.Since(start); d < 900*time.Millisecond {
		t.Fatalf("SIGKILL came after %v, before the 1 s grace", d)
	}
}

func (s *suite) testProcessErrors(t *testing.T, id string) {
	ctx := s.ctx(t)
	_, err := s.c.StartProcess(ctx, &nodev1.StartProcessRequest{SandboxId: id, ProcessId: 0, Command: "true"})
	wantCode(t, err, codes.InvalidArgument)
	_, err = s.c.StartProcess(ctx, &nodev1.StartProcessRequest{SandboxId: id, ProcessId: 199, Command: "true", Cwd: "/etc"})
	wantCode(t, err, codes.InvalidArgument)
	_, err = s.c.GetProcess(ctx, &nodev1.GetProcessRequest{SandboxId: id, ProcessId: 9999})
	wantCode(t, err, codes.NotFound)
	_, err = s.c.KillProcess(ctx, &nodev1.KillProcessRequest{SandboxId: id, ProcessId: 9999})
	wantCode(t, err, codes.NotFound)
	_, err = Attach(ctx, s.c, &nodev1.AttachProcessRequest{SandboxId: id, ProcessId: 9999})
	wantCode(t, err, codes.NotFound)
	_, err = s.c.StartProcess(ctx, &nodev1.StartProcessRequest{SandboxId: "bx_unknown0", ProcessId: 1, Command: "true"})
	wantCode(t, err, codes.NotFound)
}

// --- SSH ------------------------------------------------------------------------------

// TestPublicKey returns a fresh ed25519 public key in authorized_keys format.
func TestPublicKey(t *testing.T, comment string) string {
	t.Helper()
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	blob := binary.BigEndian.AppendUint32(nil, 11)
	blob = append(blob, "ssh-ed25519"...)
	blob = binary.BigEndian.AppendUint32(blob, uint32(len(pub)))
	blob = append(blob, pub...)
	return "ssh-ed25519 " + base64.StdEncoding.EncodeToString(blob) + " " + comment
}

func (s *suite) testSSHKey(t *testing.T, id string) {
	ctx := s.ctx(t)
	key := TestPublicKey(t, "nodetest")
	var acc *nodev1.SSHAccess
	var err error
	for range 2 { // idempotent: the second call adds nothing
		acc, err = s.c.AddSSHKey(ctx, &nodev1.AddSSHKeyRequest{SandboxId: id, PublicKey: key})
		if err != nil {
			t.Fatalf("AddSSHKey: %v", err)
		}
	}
	if acc.User != "user" || acc.Host == "" || acc.Port == 0 || acc.Port > 65535 ||
		len(strings.Fields(acc.HostKey)) < 2 || !strings.HasPrefix(acc.HostKey, "ssh-ed25519 ") {
		t.Fatalf("AddSSHKey = %v", acc)
	}
	r := s.exec(t, id, "cat .ssh/authorized_keys")
	if got := strings.Count(r.Stdout, strings.Fields(key)[1]); got != 1 {
		t.Fatalf("authorized_keys holds the key %d times, want 1:\n%s", got, r.Stdout)
	}
	for _, bad := range []string{
		"", "not a key", "ssh-ed25519", "ssh-ed25519 !!!notbase64",
		`command="rm -rf ~" ` + key,
		key + "\nssh-ed25519 AAAA second",
		"ssh-rsa " + strings.Fields(key)[1], // blob type does not match
	} {
		_, err := s.c.AddSSHKey(ctx, &nodev1.AddSSHKeyRequest{SandboxId: id, PublicKey: bad})
		if err == nil {
			t.Fatalf("AddSSHKey(%q) succeeded", bad)
		}
		wantCode(t, err, codes.InvalidArgument)
	}
}

// --- Stop and start ---------------------------------------------------------------------

func (s *suite) testStopStart(t *testing.T) {
	ctx := s.ctx(t)
	vm := s.create(t, &nodev1.CreateVMRequest{Env: map[string]string{"P1_ENV": "before"}})
	id := vm.SandboxId
	if _, err := PutFile(ctx, s.c, id, "kept.txt", 0, []byte("kept"), 0); err != nil {
		t.Fatalf("PutFile: %v", err)
	}
	stopped, err := s.c.StopVM(ctx, &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 10})
	if err != nil {
		t.Fatalf("StopVM: %v", err)
	}
	if stopped.State != nodev1.VMState_VM_STATE_STOPPED {
		t.Fatalf("StopVM state = %s", stopped.State)
	}
	if again, err := s.c.StopVM(ctx, &nodev1.StopVMRequest{SandboxId: id}); err != nil || again.State != nodev1.VMState_VM_STATE_STOPPED {
		t.Fatalf("StopVM again = %v, %v", again, err)
	}
	_, err = Exec(ctx, s.c, &nodev1.ExecRequest{SandboxId: id, Command: "true"})
	wantCode(t, err, codes.FailedPrecondition)
	_, err = s.c.StatFile(ctx, &nodev1.StatFileRequest{SandboxId: id, Path: "kept.txt"})
	if s.opts.Phase1.Files {
		wantCode(t, err, codes.FailedPrecondition)
	}
	list, err := s.c.ListVMs(ctx, &nodev1.ListVMsRequest{})
	if err != nil || !slices.ContainsFunc(list.Vms, func(v *nodev1.VM) bool {
		return v.SandboxId == id && v.State == nodev1.VMState_VM_STATE_STOPPED
	}) {
		t.Fatalf("ListVMs does not show %s STOPPED: %v, %v", id, list, err)
	}
	// CreateVM never boots an existing VM.
	if c, err := s.c.CreateVM(ctx, &nodev1.CreateVMRequest{SandboxId: id, Resources: s.opts.Resources}); err != nil || c.State != nodev1.VMState_VM_STATE_STOPPED {
		t.Fatalf("CreateVM on a stopped VM = %v, %v; want it returned STOPPED", c, err)
	}
	smaller := &nodev1.Resources{Vcpus: s.opts.Resources.Vcpus, MemoryMib: s.opts.Resources.MemoryMib, DiskGib: s.opts.Resources.DiskGib - 1}
	_, err = s.c.StartVM(ctx, &nodev1.StartVMRequest{SandboxId: id, Resources: smaller})
	wantCode(t, err, codes.InvalidArgument)

	started, err := s.c.StartVM(ctx, &nodev1.StartVMRequest{SandboxId: id, Env: map[string]string{"P1_ENV": "after"}})
	if err != nil {
		t.Fatalf("StartVM: %v", err)
	}
	if started.State != nodev1.VMState_VM_STATE_READY || started.StartedAtUnixMs < vm.StartedAtUnixMs || started.StartedAtUnixMs == 0 {
		t.Fatalf("StartVM = %v (first start %d)", started, vm.StartedAtUnixMs)
	}
	if again, err := s.c.StartVM(ctx, &nodev1.StartVMRequest{SandboxId: id, Env: map[string]string{"P1_ENV": "after"}}); err != nil || again.State != nodev1.VMState_VM_STATE_READY {
		t.Fatalf("StartVM again = %v, %v", again, err)
	}
	r := s.exec(t, id, `cat kept.txt; echo " $P1_ENV"`)
	if r.Stdout != "kept after\n" {
		t.Fatalf("after restart: stdout %q, want the kept file and the new env", r.Stdout)
	}
	// A stopped VM is deleted with its disk.
	if _, err := s.c.StopVM(ctx, &nodev1.StopVMRequest{SandboxId: id}); err != nil {
		t.Fatalf("StopVM: %v", err)
	}
	if _, err := s.c.DeleteVM(ctx, &nodev1.DeleteVMRequest{SandboxId: id}); err != nil {
		t.Fatalf("DeleteVM stopped: %v", err)
	}
	_, err = s.c.GetVM(ctx, &nodev1.GetVMRequest{SandboxId: id})
	wantCode(t, err, codes.NotFound)
}

func (s *suite) testStopUnknown(t *testing.T) {
	ctx := s.ctx(t)
	_, err := s.c.StopVM(ctx, &nodev1.StopVMRequest{SandboxId: "bx_unknown0"})
	wantCode(t, err, codes.NotFound)
	_, err = s.c.StartVM(ctx, &nodev1.StartVMRequest{SandboxId: "bx_unknown0"})
	wantCode(t, err, codes.NotFound)
}

func (s *suite) testProcessLostAfterStop(t *testing.T) {
	ctx := s.ctx(t)
	id := s.create(t, &nodev1.CreateVMRequest{}).SandboxId
	if _, err := s.c.StartProcess(ctx, &nodev1.StartProcessRequest{SandboxId: id, ProcessId: 1, Command: "echo before-stop; sleep 600"}); err != nil {
		t.Fatalf("StartProcess: %v", err)
	}
	waitOutput(t, ctx, s.c, id, 1, "before-stop\n")
	if _, err := s.c.StopVM(ctx, &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 5}); err != nil {
		t.Fatalf("StopVM: %v", err)
	}
	if _, err := s.c.StartVM(ctx, &nodev1.StartVMRequest{SandboxId: id}); err != nil {
		t.Fatalf("StartVM: %v", err)
	}
	o, err := s.c.GetProcess(ctx, &nodev1.GetProcessRequest{SandboxId: id, ProcessId: 1})
	if err != nil {
		t.Fatalf("GetProcess after restart: %v", err)
	}
	if o.Process.State != nodev1.ProcessState_PROCESS_STATE_LOST || string(o.Stdout) != "before-stop\n" {
		t.Fatalf("after restart = state %s, stdout %q; want LOST with the log kept", o.Process.State, o.Stdout)
	}
	a, err := Attach(ctx, s.c, &nodev1.AttachProcessRequest{SandboxId: id, ProcessId: 1})
	if err != nil || a.Exit.State != nodev1.ProcessState_PROCESS_STATE_LOST || a.Stdout != "before-stop\n" {
		t.Fatalf("attach after restart = %+v, %v", a, err)
	}
	k, err := s.c.KillProcess(ctx, &nodev1.KillProcessRequest{SandboxId: id, ProcessId: 1})
	if err != nil || k.Signaled {
		t.Fatalf("KillProcess of a lost process = %v, %v; want OK, not signaled", k, err)
	}
}

// waitOutput polls GetProcess until stdout equals want.
func waitOutput(t *testing.T, ctx context.Context, c nodev1.NodeClient, id string, pid uint32, want string) {
	t.Helper()
	for {
		o, err := c.GetProcess(ctx, &nodev1.GetProcessRequest{SandboxId: id, ProcessId: pid})
		if err != nil {
			t.Fatalf("GetProcess: %v", err)
		}
		if string(o.Stdout) == want {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("stdout stayed %q, want %q", o.Stdout, want)
		case <-time.After(50 * time.Millisecond):
		}
	}
}
