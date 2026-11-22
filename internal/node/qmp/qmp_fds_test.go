package qmp

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// fdsReadCommand reads one command frame from the raw unix connection (not
// through the buffered reader, which would drop ancillary data) and returns
// it with every descriptor received while reading it. The test owns and
// closes the received descriptors.
func fdsReadCommand(t *testing.T, sc *serverConn) (command, []int) {
	t.Helper()
	if n := sc.r.Buffered(); n != 0 {
		t.Fatalf("server reader holds %d buffered bytes; fd tests read the socket directly", n)
	}
	uc := sc.conn.(*net.UnixConn)
	if err := uc.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	defer uc.SetReadDeadline(time.Time{})
	var line []byte
	var fds []int
	buf := make([]byte, 64<<10)
	oob := make([]byte, unix.CmsgSpace(4*maxPassedFiles))
	for !bytes.HasSuffix(line, []byte("\n")) {
		n, oobn, _, _, err := uc.ReadMsgUnix(buf, oob)
		if err != nil {
			t.Fatalf("read command: %v", err)
		}
		line = append(line, buf[:n]...)
		if oobn > 0 {
			msgs, err := unix.ParseSocketControlMessage(oob[:oobn])
			if err != nil {
				t.Fatalf("parse control message: %v", err)
			}
			for _, m := range msgs {
				got, err := unix.ParseUnixRights(&m)
				if err != nil {
					t.Fatalf("parse unix rights: %v", err)
				}
				fds = append(fds, got...)
			}
		}
	}
	t.Cleanup(func() {
		for _, fd := range fds {
			unix.Close(fd)
		}
	})
	var raw map[string]jsontext.Value
	if err := json.Unmarshal(line, &raw); err != nil {
		t.Fatalf("decode command %q: %v", line, err)
	}
	c := command{raw: raw}
	if err := json.Unmarshal(raw["execute"], &c.name); err != nil {
		t.Fatalf("decode command name: %v", err)
	}
	if err := json.Unmarshal(raw["id"], &c.id); err != nil {
		t.Fatalf("decode command id: %v", err)
	}
	return c, fds
}

// fdsWaitReadable waits until the client started writing to the server's
// end of the connection.
func fdsWaitReadable(t *testing.T, sc *serverConn) {
	t.Helper()
	rc, err := sc.conn.(*net.UnixConn).SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var n int
	var perr error
	if err := rc.Control(func(fd uintptr) {
		n, perr = unix.Poll([]unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}, 5000)
	}); err != nil {
		t.Fatal(err)
	}
	if perr != nil || n != 1 {
		t.Fatalf("server end never became readable: n=%d err=%v", n, perr)
	}
}

// fdsTempFile creates a file holding content.
func fdsTempFile(t *testing.T, name, content string) *os.File {
	t.Helper()
	f, err := os.Create(filepath.Join(t.TempDir(), name))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	if _, err := f.WriteString(content); err != nil {
		t.Fatal(err)
	}
	return f
}

// fdsSameFile reports whether the received descriptor is the open file f.
func fdsSameFile(t *testing.T, fd int, f *os.File) bool {
	t.Helper()
	var got unix.Stat_t
	if err := unix.Fstat(fd, &got); err != nil {
		t.Fatalf("fstat received fd: %v", err)
	}
	info, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	want := info.Sys().(*syscall.Stat_t)
	return got.Dev == want.Dev && got.Ino == want.Ino
}

func TestExecuteWithFilesPassesDescriptors(t *testing.T) {
	tests := []struct {
		name  string
		files int
	}{
		{"one file", 1},
		{"three files in order", 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, accepted := startQPMServer(t)
			client, sc := dialForTest(t, srv, accepted)

			var files []*os.File
			for i := range tt.files {
				files = append(files, fdsTempFile(t, "f"+string(rune('a'+i)), "content-"+string(rune('a'+i))))
			}
			type addFdReply struct {
				FdsetID int `json:"fdset-id"`
				FD      int `json:"fd"`
			}
			done := make(chan error, 1)
			var got addFdReply
			go func() {
				done <- client.ExecuteWithFiles(t.Context(), "add-fd", map[string]string{"opaque": "bbraft-fleece"}, &got, files...)
			}()
			cmd, fds := fdsReadCommand(t, sc)
			if cmd.name != "add-fd" {
				t.Fatalf("command = %q, want add-fd", cmd.name)
			}
			if string(cmd.raw["arguments"]) != `{"opaque":"bbraft-fleece"}` {
				t.Fatalf("arguments = %s", cmd.raw["arguments"])
			}
			if len(fds) != len(files) {
				t.Fatalf("received %d descriptors, want %d", len(fds), len(files))
			}
			for i, fd := range fds {
				if !fdsSameFile(t, fd, files[i]) {
					t.Errorf("descriptor %d is not file %s", i, files[i].Name())
				}
				buf := make([]byte, 64)
				n, err := unix.Pread(fd, buf, 0)
				if err != nil {
					t.Fatalf("pread received fd: %v", err)
				}
				if want := "content-" + string(rune('a'+i)); string(buf[:n]) != want {
					t.Errorf("descriptor %d reads %q, want %q", i, buf[:n], want)
				}
			}
			sc.sendReturn(cmd.id, map[string]int{"fdset-id": 0, "fd": 54})
			if err := waitChannel(t, done); err != nil {
				t.Fatalf("ExecuteWithFiles: %v", err)
			}
			if got.FD != 54 {
				t.Fatalf("result = %+v", got)
			}
			// The client never closes the caller's files.
			for _, f := range files {
				if _, err := f.Stat(); err != nil {
					t.Fatalf("caller's file was closed: %v", err)
				}
			}
		})
	}
}

// TestExecuteWithFilesKeepsFrameOrder interleaves plain commands with one
// carrying a descriptor and proves every frame arrives whole and the
// descriptor arrives with exactly its command.
func TestExecuteWithFilesKeepsFrameOrder(t *testing.T) {
	srv, accepted := startQPMServer(t)
	client, sc := dialForTest(t, srv, accepted)
	f := fdsTempFile(t, "fleece", "x")

	for _, name := range []string{"query-status", "add-fd", "query-fdsets"} {
		done := make(chan error, 1)
		go func() {
			if name == "add-fd" {
				done <- client.ExecuteWithFiles(t.Context(), name, nil, nil, f)
				return
			}
			done <- client.Execute(t.Context(), name, nil, nil)
		}()
		cmd, fds := fdsReadCommand(t, sc)
		if cmd.name != name {
			t.Fatalf("command = %q, want %q", cmd.name, name)
		}
		wantFDs := 0
		if name == "add-fd" {
			wantFDs = 1
		}
		if len(fds) != wantFDs {
			t.Fatalf("%s carried %d descriptors, want %d", name, len(fds), wantFDs)
		}
		sc.sendReturn(cmd.id, nil)
		if err := waitChannel(t, done); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
}

// TestExecuteWithFilesLargeFrame passes a descriptor with a command bigger
// than the socket buffer: the descriptor travels with the first part and the
// rest of the frame follows as plain data.
func TestExecuteWithFilesLargeFrame(t *testing.T) {
	srv, accepted := startQPMServer(t)
	client, sc := dialForTest(t, srv, accepted)
	f := fdsTempFile(t, "fleece", "x")
	big := strings.Repeat("y", 1<<20)

	done := make(chan error, 1)
	go func() {
		done <- client.ExecuteWithFiles(t.Context(), "add-fd", map[string]string{"opaque": big}, nil, f)
	}()
	cmd, fds := fdsReadCommand(t, sc)
	if cmd.name != "add-fd" || len(fds) != 1 || !fdsSameFile(t, fds[0], f) {
		t.Fatalf("command %q with %d descriptors", cmd.name, len(fds))
	}
	var args map[string]string
	if err := json.Unmarshal(cmd.raw["arguments"], &args); err != nil || args["opaque"] != big {
		t.Fatalf("large arguments did not arrive whole: %v", err)
	}
	sc.sendReturn(cmd.id, nil)
	if err := waitChannel(t, done); err != nil {
		t.Fatalf("ExecuteWithFiles: %v", err)
	}
}

func TestExecuteWithFilesErrors(t *testing.T) {
	t.Run("not a unix socket", func(t *testing.T) {
		a, b := net.Pipe()
		defer a.Close()
		defer b.Close()
		c := &Client{conn: a}
		err := c.ExecuteWithFiles(t.Context(), "add-fd", nil, nil, fdsTempFile(t, "f", "x"))
		if err == nil || !strings.Contains(err.Error(), "unix socket") {
			t.Fatalf("ExecuteWithFiles over a pipe = %v, want a unix socket error", err)
		}
	})

	srv, accepted := startQPMServer(t)
	client, sc := dialForTest(t, srv, accepted)

	t.Run("no files", func(t *testing.T) {
		if err := client.ExecuteWithFiles(t.Context(), "add-fd", nil, nil); err == nil {
			t.Fatal("ExecuteWithFiles without files succeeded")
		}
	})
	t.Run("nil file", func(t *testing.T) {
		if err := client.ExecuteWithFiles(t.Context(), "add-fd", nil, nil, nil); err == nil {
			t.Fatal("ExecuteWithFiles with a nil file succeeded")
		}
	})
	t.Run("too many files", func(t *testing.T) {
		f := fdsTempFile(t, "f", "x")
		files := make([]*os.File, maxPassedFiles+1)
		for i := range files {
			files[i] = f
		}
		if err := client.ExecuteWithFiles(t.Context(), "add-fd", nil, nil, files...); err == nil {
			t.Fatal("ExecuteWithFiles with too many files succeeded")
		}
	})
	t.Run("closed file", func(t *testing.T) {
		f := fdsTempFile(t, "f", "x")
		f.Close()
		err := client.ExecuteWithFiles(t.Context(), "add-fd", nil, nil, f)
		if !errors.Is(err, os.ErrClosed) {
			t.Fatalf("ExecuteWithFiles with a closed file = %v, want os.ErrClosed", err)
		}
	})
	t.Run("already cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		err := client.ExecuteWithFiles(ctx, "add-fd", nil, nil, fdsTempFile(t, "f", "x"))
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("ExecuteWithFiles with a cancelled context = %v", err)
		}
	})
	t.Run("cancel while waiting for the reply", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
		defer cancel()
		f := fdsTempFile(t, "f", "x")
		done := make(chan error, 1)
		go func() {
			done <- client.ExecuteWithFiles(ctx, "add-fd", nil, nil, f)
		}()
		cmd, fds := fdsReadCommand(t, sc)
		if len(fds) != 1 {
			t.Fatalf("received %d descriptors", len(fds))
		}
		if err := waitChannel(t, done); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("ExecuteWithFiles = %v, want deadline exceeded", err)
		}
		if _, err := f.Stat(); err != nil {
			t.Fatalf("caller's file was closed: %v", err)
		}
		// The late reply is ignored and the client stays usable.
		sc.sendReturn(cmd.id, nil)
	})
	// Every failure above left the connection usable.
	done := make(chan error, 1)
	go func() {
		done <- client.ExecuteWithFiles(t.Context(), "add-fd", nil, nil, fdsTempFile(t, "g", "x"))
	}()
	cmd, fds := fdsReadCommand(t, sc)
	if cmd.name != "add-fd" || len(fds) != 1 {
		t.Fatalf("command %q with %d descriptors after the error cases", cmd.name, len(fds))
	}
	sc.sendReturn(cmd.id, nil)
	if err := waitChannel(t, done); err != nil {
		t.Fatalf("ExecuteWithFiles after the error cases: %v", err)
	}

	t.Run("closed client", func(t *testing.T) {
		client.Close()
		err := client.ExecuteWithFiles(t.Context(), "add-fd", nil, nil, fdsTempFile(t, "f", "x"))
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("ExecuteWithFiles on a closed client = %v, want ErrClosed", err)
		}
	})
}

// TestExecuteWithFilesFileClosedInQueue closes the caller's file while its
// request waits behind a blocked write: the writer reports the closed file
// for that request only, without sending a recycled descriptor or poisoning
// the connection.
func TestExecuteWithFilesFileClosedInQueue(t *testing.T) {
	srv, accepted := startQPMServer(t)
	client, sc := dialForTest(t, srv, accepted)

	// Occupy the writer: the server reads nothing until the blocker is in.
	big := strings.Repeat("x", 4<<20)
	blocker := make(chan error, 1)
	go func() {
		blocker <- client.Execute(t.Context(), "query-blocker", map[string]string{"data": big}, nil)
	}()
	fdsWaitReadable(t, sc)

	f := fdsTempFile(t, "fleece", "x")
	queued := make(chan error, 1)
	go func() {
		queued <- client.ExecuteWithFiles(t.Context(), "add-fd", nil, nil, f)
	}()
	deadline := time.Now().Add(5 * time.Second)
	for len(client.writeCh) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the add-fd request never reached the writer queue")
		}
		time.Sleep(time.Millisecond)
	}
	f.Close()

	// Drain the blocker; the add-fd frame must never arrive.
	cmd, fds := fdsReadCommand(t, sc)
	if cmd.name != "query-blocker" || len(fds) != 0 {
		t.Fatalf("first frame %q with %d descriptors", cmd.name, len(fds))
	}
	sc.sendReturn(cmd.id, nil)
	if err := waitChannel(t, blocker); err != nil {
		t.Fatalf("blocker: %v", err)
	}
	// "pass files" is the writer's error: the up-front check had passed.
	if err := waitChannel(t, queued); !errors.Is(err, os.ErrClosed) || !strings.Contains(err.Error(), "pass files") {
		t.Fatalf("queued ExecuteWithFiles = %v, want the writer's os.ErrClosed", err)
	}

	done := make(chan error, 1)
	go func() {
		done <- client.Execute(t.Context(), "query-next", nil, nil)
	}()
	next, fds := fdsReadCommand(t, sc)
	if next.name != "query-next" || len(fds) != 0 {
		t.Fatalf("next frame %q with %d descriptors; the closed file's frame was written", next.name, len(fds))
	}
	sc.sendReturn(next.id, nil)
	if err := waitChannel(t, done); err != nil {
		t.Fatalf("Execute after the closed file: %v", err)
	}
}
