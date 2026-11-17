//go:build linux

package cli

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// TestReadWithContextUnblocksBlockingFile uses a real blocking pipe wrapped by
// os.NewFile, the shape inherited stdio has, and proves cancellation closes a
// pollable replacement, joins the reader and then releases the original fd.
func TestReadWithContextUnblocksBlockingFile(t *testing.T) {
	var fds [2]int
	if err := unix.Pipe(fds[:]); err != nil {
		t.Fatalf("pipe: %v", err)
	}
	reader := os.NewFile(uintptr(fds[0]), "blocking-stdin")
	writer := os.NewFile(uintptr(fds[1]), "blocking-stdout")
	defer writer.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{})
	errCh := make(chan error, 1)
	go func() {
		errCh <- readWithContext(ctx, reader, func(r io.Reader) error {
			close(started)
			var one [1]byte
			_, err := r.Read(one[:])
			return err
		})
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("read did not start")
	}
	cancel()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("blocking read did not cancel")
	}

	closed := make(chan error, 1)
	go func() {
		_, err := writer.Write([]byte("x"))
		closed <- err
	}()
	select {
	case err := <-closed:
		if err == nil {
			t.Fatal("original reader was not released")
		}
	case <-time.After(time.Second):
		t.Fatal("writer blocked; original reader was not closed")
	}
}

// TestCLIStdinSignalHelper is the child half of the subprocess SIGINT test.
// It reads the real os.Stdin with readWithContext and exits 0 only when the
// signal cancels the blocked read.
func TestCLIStdinSignalHelper(t *testing.T) {
	if os.Getenv("BBRAFT_CLI_SIGNAL_HELPER") != "1" {
		return
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	err := readWithContext(ctx, os.Stdin, func(r io.Reader) error {
		fmt.Fprintln(os.Stderr, "READING")
		var one [1]byte
		_, readErr := r.Read(one[:])
		return readErr
	})
	if errors.Is(err, context.Canceled) {
		os.Exit(0)
	}
	os.Exit(1)
}

// TestReadWithContextSubprocessSIGINT proves SIGINT unblocks a real inherited
// stdin in a subprocess. The child announces READing, the parent signals it,
// and the child must exit promptly instead of waiting on the blocked Read.
func TestReadWithContextSubprocessSIGINT(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=TestCLIStdinSignalHelper")
	cmd.Env = append(os.Environ(), "BBRAFT_CLI_SIGNAL_HELPER=1")
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatalf("stdin pipe: %v", err)
	}
	defer stdin.Close()
	stderr, err := cmd.StderrPipe()
	if err != nil {
		t.Fatalf("stderr pipe: %v", err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start helper: %v", err)
	}
	waited := false
	defer func() {
		if !waited {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}()

	announce := make(chan string, 1)
	go func() {
		line, err := bufio.NewReader(stderr).ReadString('\n')
		if err != nil {
			announce <- ""
			return
		}
		announce <- line
	}()
	select {
	case line := <-announce:
		if line != "READING\n" {
			t.Fatalf("helper did not start reading: line=%q", line)
		}
	case <-ctx.Done():
		t.Fatal("helper did not start reading before the deadline")
	}
	if err := cmd.Process.Signal(os.Interrupt); err != nil {
		t.Fatalf("signal helper: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		waited = true
		if err != nil {
			t.Fatalf("helper exit: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("helper did not exit after SIGINT")
	}
}

// TestReopenPollableKeepsOriginalFlags proves the original nonblocking fd is
// not switched to blocking. File.Fd would do that for a Go pipe.
func TestReopenPollableKeepsOriginalFlags(t *testing.T) {
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	defer reader.Close()
	defer writer.Close()
	raw, err := reader.SyscallConn()
	if err != nil {
		t.Fatalf("syscall conn: %v", err)
	}
	var before int
	if err := raw.Control(func(fd uintptr) {
		before, err = unix.FcntlInt(fd, unix.F_GETFL, 0)
	}); err != nil {
		t.Fatalf("read flags: %v", err)
	}
	if before&unix.O_NONBLOCK == 0 {
		t.Fatalf("original flags %#x are not nonblocking", before)
	}
	reopened, ok := reopenPollable(reader)
	if !ok {
		t.Fatal("reopenPollable failed for a pipe")
	}
	defer reopened.Close()
	var after int
	if err := raw.Control(func(fd uintptr) {
		after, err = unix.FcntlInt(fd, unix.F_GETFL, 0)
	}); err != nil {
		t.Fatalf("read flags after reopen: %v", err)
	}
	if after != before {
		t.Fatalf("original flags changed from %#x to %#x", before, after)
	}
}

// TestReadWithContextRegularFileKeepsOffset proves a regular file is not
// reopened, so a partially consumed stdin reads the remaining bytes.
func TestReadWithContextRegularFileKeepsOffset(t *testing.T) {
	path := filepath.Join(t.TempDir(), "stdin")
	if err := os.WriteFile(path, []byte("abcdef"), 0o600); err != nil {
		t.Fatalf("write file: %v", err)
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open file: %v", err)
	}
	defer file.Close()
	var one [1]byte
	if _, err := file.Read(one[:]); err != nil {
		t.Fatalf("first read: %v", err)
	}
	var got []byte
	err = readWithContext(t.Context(), file, func(r io.Reader) error {
		var readErr error
		got, readErr = io.ReadAll(r)
		return readErr
	})
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != "bcdef" {
		t.Fatalf("got %q, want the remaining bytes", got)
	}
}

// TestReopenPollableFIFODoesNotBlock proves opening a FIFO with no writer uses
// O_NONBLOCK instead of waiting forever.
func TestReopenPollableFIFODoesNotBlock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fifo")
	if err := unix.Mkfifo(path, 0o600); err != nil {
		t.Fatalf("mkfifo: %v", err)
	}
	file, err := os.OpenFile(path, os.O_RDONLY|unix.O_NONBLOCK, 0)
	if err != nil {
		t.Fatalf("open fifo: %v", err)
	}
	defer file.Close()
	done := make(chan bool, 1)
	go func() {
		_, ok := reopenPollable(file)
		done <- ok
	}()
	select {
	case ok := <-done:
		if !ok {
			t.Fatal("reopenPollable failed for a fifo")
		}
	case <-time.After(time.Second):
		t.Fatal("reopenPollable blocked opening a fifo with no writer")
	}
}

// TestReadWithContextClosedWriterPipeEOF proves a pipe whose writer is closed
// reports EOF promptly instead of hanging.
func TestReadWithContextClosedWriterPipeEOF(t *testing.T) {
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	defer reader.Close()
	err = readWithContext(t.Context(), reader, func(r io.Reader) error {
		_, readErr := io.ReadAll(r)
		return readErr
	})
	if err != nil {
		t.Fatalf("read: %v", err)
	}
}
