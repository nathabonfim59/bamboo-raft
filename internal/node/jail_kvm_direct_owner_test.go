package node

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc"
)

func jailKVMSockID(t *testing.T, p string) jailKVMIdentity {
	t.Helper()
	var st syscall.Stat_t
	if err := syscall.Lstat(p, &st); err != nil {
		t.Fatalf("lstat %s: %v", p, err)
	}
	return jailKVMIdentity{Dev: st.Dev, Ino: st.Ino}
}

// TestJailKVMDirectListen: the Direct socket refuses an existing path, is
// 0600, and its cleanup removes only the inode it created. A Go unix
// listener unlinks its original pathname on Close by default, which would
// delete a replacement socket; the cleanup must never do that.
func TestJailKVMDirectListen(t *testing.T) {
	t.Run("own socket is 0600 and removed", func(t *testing.T) {
		p := filepath.Join(t.TempDir(), "direct.sock")
		_, cleanup, err := jailKVMDirectListen(p)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = cleanup() }) // idempotent: closes the listener on any failure
		if info, err := os.Lstat(p); err != nil || info.Mode()&os.ModeSocket == 0 || info.Mode().Perm() != 0o600 {
			t.Fatalf("socket %v, %v: want a 0600 socket", info, err)
		}
		if err := cleanup(); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Lstat(p); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("own socket left behind: %v", err)
		}
	})
	t.Run("existing path refused", func(t *testing.T) {
		p := filepath.Join(t.TempDir(), "direct.sock")
		if err := os.WriteFile(p, nil, 0o600); err != nil {
			t.Fatal(err)
		}
		if _, _, err := jailKVMDirectListen(p); err == nil {
			t.Fatal("an existing path was adopted")
		}
	})
	t.Run("a replacement socket survives the cleanup", func(t *testing.T) {
		dir := t.TempDir()
		p := filepath.Join(dir, "direct.sock")
		_, cleanup, err := jailKVMDirectListen(p)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = cleanup() }) // idempotent: closes the listener on any failure
		if err := os.Rename(p, p+".old"); err != nil {
			t.Fatal(err)
		}
		replacement, err := net.Listen("unix", p)
		if err != nil {
			t.Fatal(err)
		}
		replacement.(*net.UnixListener).SetUnlinkOnClose(false)
		defer replacement.Close()
		want := jailKVMSockID(t, p)
		if err := cleanup(); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Lstat(p); err != nil {
			t.Fatalf("the cleanup deleted the replacement socket: %v", err)
		}
		if got := jailKVMSockID(t, p); got != want {
			t.Fatalf("replacement %v became %v", want, got)
		}
	})
}

// jailKVMFakeServer scripts Serve, GracefulStop and Stop. Its "hang" cases
// block on a test-owned release gate, and each method closes its own
// predeclared done channel when it returns, so the test joins exactly the
// calls its scenario expects (run's Cleanup: release first, then the
// wrapper, then every expected call, all bounded).
type jailKVMFakeServer struct {
	mu           sync.Mutex
	calls        []string
	stopped      chan struct{} // closed by Stop (and by a cooperative GracefulStop)
	stopOnce     sync.Once
	release      chan struct{} // closed by the test's Cleanup
	releaseOnce  sync.Once
	serveDone    chan struct{} // closed when Serve returns
	gracefulDone chan struct{} // closed when GracefulStop returns
	gracefulOK   bool          // GracefulStop stops Serve by itself
	gracefulHang bool          // GracefulStop does not return until released
	serveEarly   error         // Serve returns this at once (early end)
	serveHang    bool          // Serve does not return until released
	serveRet     error
}

func newJailKVMFakeServer() *jailKVMFakeServer {
	return &jailKVMFakeServer{stopped: make(chan struct{}), release: make(chan struct{}),
		serveDone: make(chan struct{}), gracefulDone: make(chan struct{})}
}

// releaseAll opens every gate (idempotent).
func (f *jailKVMFakeServer) releaseAll() {
	f.releaseOnce.Do(func() { close(f.release) })
	f.stopOnce.Do(func() { close(f.stopped) })
}

func (f *jailKVMFakeServer) record(c string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, c)
}

func (f *jailKVMFakeServer) Serve(net.Listener) error {
	defer close(f.serveDone)
	f.record("serve")
	if f.serveEarly != nil {
		return f.serveEarly
	}
	<-f.stopped
	if f.serveHang {
		<-f.release
	}
	return f.serveRet
}

func (f *jailKVMFakeServer) GracefulStop() {
	defer close(f.gracefulDone)
	f.record("graceful")
	if f.gracefulOK {
		f.stopOnce.Do(func() { close(f.stopped) })
		return
	}
	<-f.stopped // a stuck client: only Stop ends it
	if f.gracefulHang {
		<-f.release
	}
}

func (f *jailKVMFakeServer) Stop() {
	f.record("stop")
	f.stopOnce.Do(func() { close(f.stopped) })
}

func (f *jailKVMFakeServer) has(c string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, x := range f.calls {
		if x == c {
			return true
		}
	}
	return false
}

// TestJailKVMDirectServe: the Direct helper owns its Serve and GracefulStop
// goroutines: a stop signal runs a bounded GracefulStop, falls back to Stop
// and then joins GracefulStop (bounded), then makes the sole bounded receive
// of Serve's result; an early Serve end is an error and still stops the
// server; every join is bounded.
func TestJailKVMDirectServe(t *testing.T) {
	b := jailKVMServeBudget{Graceful: 50 * time.Millisecond, GracefulJoin: 50 * time.Millisecond, ServeJoin: 50 * time.Millisecond}
	if d := jailKVMDirectServeDefaults(); d.Graceful+d.GracefulJoin+d.ServeJoin+jailKVMDirectShutdown >= 30*time.Second {
		t.Fatalf("stop budget %+v + %v does not fit TimeoutStopSec=30s", d, jailKVMDirectShutdown)
	}
	run := func(t *testing.T, f *jailKVMFakeServer, stop bool) (time.Duration, error) {
		ctx, cancel := context.WithCancel(t.Context())
		ready := make(chan struct{})
		out := make(chan error, 1)
		wrapperDone := make(chan struct{})
		// Every path, an early Fatal included: release the gates FIRST, then
		// join the wrapper, then every call this scenario makes (Serve
		// always; GracefulStop only after a stop signal), all bounded.
		t.Cleanup(func() {
			f.releaseAll()
			cancel()
			join := func(what string, done <-chan struct{}) {
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Errorf("%s did not finish", what)
				}
			}
			join("jailKVMDirectServe", wrapperDone)
			join("fake Serve", f.serveDone)
			if stop {
				join("fake GracefulStop", f.gracefulDone)
			}
		})
		go func() {
			defer close(wrapperDone)
			out <- jailKVMDirectServe(ctx, f, nil, func() { close(ready) }, b)
		}()
		select {
		case <-ready:
		case <-time.After(5 * time.Second):
			t.Fatal("jailKVMDirectServe never became ready")
		}
		start := time.Now()
		if stop {
			cancel()
		}
		select {
		case err := <-out:
			return time.Since(start), err
		case <-time.After(5 * time.Second):
			t.Fatal("jailKVMDirectServe did not return")
			return 0, nil
		}
	}
	t.Run("graceful stop", func(t *testing.T) {
		f := newJailKVMFakeServer()
		f.gracefulOK = true
		if _, err := run(t, f, true); err != nil || f.has("stop") {
			t.Fatalf("err %v, calls %q: want a clean graceful stop", err, f.calls)
		}
	})
	t.Run("graceful stop blocked until the fallback Stop", func(t *testing.T) {
		f := newJailKVMFakeServer()
		d, err := run(t, f, true)
		if err != nil || !f.has("stop") || d < b.Graceful {
			t.Fatalf("err %v after %v, calls %q: want Stop after the graceful bound and a clean join", err, d, f.calls)
		}
	})
	t.Run("graceful stop that never returns is a bounded error", func(t *testing.T) {
		f := newJailKVMFakeServer()
		f.gracefulHang = true
		if _, err := run(t, f, true); err == nil || !strings.Contains(err.Error(), "GracefulStop") {
			t.Fatalf("err %v: want the unjoined GracefulStop reported", err)
		}
	})
	t.Run("serve that never returns is a bounded error", func(t *testing.T) {
		f := newJailKVMFakeServer()
		f.gracefulOK, f.serveHang = true, true
		if _, err := run(t, f, true); err == nil || !strings.Contains(err.Error(), "serve") {
			t.Fatalf("err %v: want the unjoined Serve reported", err)
		}
	})
	t.Run("server stopped is clean", func(t *testing.T) {
		f := newJailKVMFakeServer()
		f.gracefulOK, f.serveRet = true, grpc.ErrServerStopped
		if _, err := run(t, f, true); err != nil {
			t.Fatalf("err %v", err)
		}
	})
	t.Run("an early serve end is an error and stops the server", func(t *testing.T) {
		f := newJailKVMFakeServer()
		f.serveEarly = errors.New("listener died")
		_, err := run(t, f, false)
		if err == nil || !strings.Contains(err.Error(), "listener died") || !f.has("stop") || f.has("graceful") {
			t.Fatalf("err %v, calls %q", err, f.calls)
		}
	})
}
