package nbd

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

const (
	nbdQEMUDiskSize = 64 << 20
	nbdQEMUChunk    = 4 << 20  // snapformat.ChunkSize, the bitmap granularity
	nbdQEMUCluster  = 64 << 10 // qcow2 default cluster size
)

// nbdQEMUWrite is a qemu-io pattern write.
type nbdQEMUWrite struct {
	off, n  int
	pattern byte
}

func (w nbdQEMUWrite) cmd() string { return fmt.Sprintf("write -P %#x %d %d", w.pattern, w.off, w.n) }

var (
	// nbdQEMUBaseWrites fill the base: chunks 0-3 and 10.
	nbdQEMUBaseWrites = []nbdQEMUWrite{{0, 16 << 20, 0x11}, {40 << 20, 4 << 20, 0x22}}
	// nbdQEMUOverlayWrites dirty chunk 1 (4 KiB inside one cluster), chunk 5
	// (one whole cluster) and chunks 8 and 9 (a write across their boundary).
	nbdQEMUOverlayWrites = []nbdQEMUWrite{
		{4<<20 + 100<<10, 4 << 10, 0xa1},
		{21 << 20, 64 << 10, 0xa5},
		{36<<20 - 32<<10, 64 << 10, 0xa8},
	}
	nbdQEMUDirtyChunks = []uint64{1, 5, 8, 9}
)

// TestQEMUNBD checks the client against qemu-nbd: dirty-bitmap and
// allocation-depth block status and reads of a read-only export of a qcow2
// overlay, then writes, zero writes (masking the backing file) and flush on
// a writable export.
//
// It skips unless BBRAFT_TEST_KVM=1 and needs qemu-img, qemu-io and qemu-nbd
// on PATH; with BBRAFT_TEST_REQUIRE_ALL=1 a missing tool fails instead:
//
//	BBRAFT_TEST_KVM=1 go test ./internal/node/nbd/ -run TestQEMUNBD -v
func TestQEMUNBD(t *testing.T) {
	if os.Getenv("BBRAFT_TEST_KVM") != "1" {
		t.Skip("set BBRAFT_TEST_KVM=1 to run the qemu-nbd integration test")
	}
	for _, tool := range []string{"qemu-img", "qemu-io", "qemu-nbd"} {
		if _, err := exec.LookPath(tool); err != nil {
			if os.Getenv("BBRAFT_TEST_REQUIRE_ALL") == "1" {
				t.Fatalf("%s not on PATH: %v", tool, err)
			}
			t.Skipf("%s not on PATH", tool)
		}
	}
	dir := nbdShortTempDir(t)
	base := filepath.Join(dir, "base.qcow2")
	overlay := filepath.Join(dir, "overlay.qcow2")

	nbdQEMURun(t, "qemu-img", "create", "-q", "-f", "qcow2", base, fmt.Sprint(nbdQEMUDiskSize))
	nbdQEMUio(t, base, nbdQEMUBaseWrites)
	nbdQEMURun(t, "qemu-img", "create", "-q", "-f", "qcow2", "-b", base, "-F", "qcow2", overlay)
	nbdQEMURun(t, "qemu-img", "bitmap", "--add", "-g", fmt.Sprint(nbdQEMUChunk), overlay, "bbraft-dirty")
	nbdQEMUio(t, overlay, nbdQEMUOverlayWrites)

	// What the disk should read as, built independently of QEMU, and what
	// qemu-img convert says it reads as (before qemu-nbd opens the image).
	want := make([]byte, nbdQEMUDiskSize)
	for _, w := range slices.Concat(nbdQEMUBaseWrites, nbdQEMUOverlayWrites) {
		for i := range w.n {
			want[w.off+i] = w.pattern
		}
	}
	converted := nbdQEMUConvert(t, overlay, filepath.Join(dir, "overlay.raw"))
	if !bytes.Equal(converted, want) {
		t.Fatal("qemu-img convert of the overlay differs from the data written")
	}

	t.Run("read-only export", func(t *testing.T) {
		const dirty = "qemu:dirty-bitmap:bbraft-dirty"
		const depth = "qemu:allocation-depth"
		srv := nbdStartQEMUNBD(t, filepath.Join(dir, "ro.sock"), "-r", "-B", "bbraft-dirty", "-A", "--image-opts", nbdImageOpts(overlay))
		c := srv.dial(t, Options{MetaContexts: []string{dirty, depth}})
		ctx := t.Context()
		if c.Size() != nbdQEMUDiskSize || !c.ReadOnly() {
			t.Fatalf("size %d read-only %v, want %d true", c.Size(), c.ReadOnly(), nbdQEMUDiskSize)
		}

		// Dirty extents are exactly the written 4 MiB chunks.
		var gotDirty []uint64
		var pos uint64
		for e, err := range c.Extents(ctx, dirty, 0, c.Size()) {
			if err != nil {
				t.Fatalf("Extents(%s): %v", dirty, err)
			}
			if e.Offset != pos {
				t.Fatalf("extent at %d, want %d", e.Offset, pos)
			}
			pos += e.Length
			if e.Flags&1 == 0 {
				continue
			}
			if e.Offset%nbdQEMUChunk != 0 || e.Length%nbdQEMUChunk != 0 {
				t.Errorf("dirty extent %+v not chunk-aligned", e)
			}
			for i := e.Offset / nbdQEMUChunk; i < (e.Offset+e.Length)/nbdQEMUChunk; i++ {
				gotDirty = append(gotDirty, i)
			}
		}
		if pos != c.Size() {
			t.Errorf("dirty extents cover %d of %d bytes", pos, c.Size())
		}
		if fmt.Sprint(gotDirty) != fmt.Sprint(nbdQEMUDirtyChunks) {
			t.Errorf("dirty chunks %v, want %v", gotDirty, nbdQEMUDirtyChunks)
		}

		// Allocation depth per cluster: 1 where the overlay was written, 2
		// where only the base was, 0 elsewhere.
		wantDepth := make([]uint32, nbdQEMUDiskSize/nbdQEMUCluster)
		for _, ws := range []struct {
			writes []nbdQEMUWrite
			depth  uint32
		}{{nbdQEMUBaseWrites, 2}, {nbdQEMUOverlayWrites, 1}} {
			for _, w := range ws.writes {
				for cl := w.off / nbdQEMUCluster; cl <= (w.off+w.n-1)/nbdQEMUCluster; cl++ {
					wantDepth[cl] = ws.depth
				}
			}
		}
		gotDepth := make([]uint32, len(wantDepth))
		for e, err := range c.Extents(ctx, depth, 0, c.Size()) {
			if err != nil {
				t.Fatalf("Extents(%s): %v", depth, err)
			}
			if e.Offset%nbdQEMUCluster != 0 || e.Length%nbdQEMUCluster != 0 {
				t.Fatalf("allocation-depth extent %+v not cluster-aligned", e)
			}
			for cl := e.Offset / nbdQEMUCluster; cl < (e.Offset+e.Length)/nbdQEMUCluster; cl++ {
				gotDepth[cl] = e.Flags
			}
		}
		for cl := range wantDepth {
			if gotDepth[cl] != wantDepth[cl] {
				t.Errorf("cluster %d (offset %d): depth %d, want %d", cl, cl*nbdQEMUCluster, gotDepth[cl], wantDepth[cl])
			}
		}

		// Reading the whole disk in 4 MiB chunks equals qemu-img convert.
		got := make([]byte, nbdQEMUDiskSize)
		for off := 0; off < len(got); off += nbdQEMUChunk {
			if err := c.Read(ctx, uint64(off), got[off:off+nbdQEMUChunk]); err != nil {
				t.Fatalf("Read %d: %v", off, err)
			}
		}
		if !bytes.Equal(got, converted) {
			t.Error("Read of the export differs from qemu-img convert")
		}
		if err := c.Write(ctx, 0, make([]byte, 4096)); !errors.Is(err, ErrUnsupported) {
			t.Errorf("Write to a read-only export: %v, want ErrUnsupported", err)
		}
		if err := c.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		srv.waitExit(t)
	})

	t.Run("writable export", func(t *testing.T) {
		restore := filepath.Join(dir, "restore.qcow2")
		nbdQEMURun(t, "qemu-img", "create", "-q", "-f", "qcow2", "-b", base, "-F", "qcow2", restore)
		srv := nbdStartQEMUNBD(t, filepath.Join(dir, "rw.sock"), "--image-opts", nbdImageOpts(restore))
		c := srv.dial(t, Options{})
		ctx := t.Context()
		if c.ReadOnly() {
			t.Fatal("writable export reported read-only")
		}
		data := bytes.Repeat([]byte{0x77}, nbdQEMUChunk)
		if err := c.Write(ctx, 6*nbdQEMUChunk, data); err != nil { // unallocated in the base
			t.Fatalf("Write: %v", err)
		}
		if err := c.WriteZeroes(ctx, 0, nbdQEMUChunk); err != nil { // base data: must be masked
			t.Fatalf("WriteZeroes: %v", err)
		}
		if err := c.Flush(ctx); err != nil {
			t.Fatalf("Flush: %v", err)
		}
		if err := c.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		srv.waitExit(t)

		want := make([]byte, nbdQEMUDiskSize)
		for _, w := range nbdQEMUBaseWrites {
			for i := range w.n {
				want[w.off+i] = w.pattern
			}
		}
		clear(want[:nbdQEMUChunk])
		copy(want[6*nbdQEMUChunk:], data)
		got := nbdQEMUConvert(t, restore, filepath.Join(dir, "restore.raw"))
		if !bytes.Equal(got[:nbdQEMUChunk], want[:nbdQEMUChunk]) {
			t.Error("WriteZeroes over base data: base still visible through the overlay")
		}
		if !bytes.Equal(got, want) {
			t.Error("qemu-img convert of the restored overlay differs from the data written")
		}
	})
}

// nbdImageOpts is the --image-opts string for a local qcow2 file.
func nbdImageOpts(path string) string {
	return "driver=qcow2,file.driver=file,file.filename=" + strings.ReplaceAll(path, ",", ",,")
}

func nbdQEMURun(t *testing.T, name string, args ...string) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	out, err := exec.CommandContext(ctx, name, args...).CombinedOutput()
	if err != nil {
		t.Fatalf("%s %s: %v\n%s", name, strings.Join(args, " "), err, out)
	}
	return out
}

func nbdQEMUio(t *testing.T, image string, writes []nbdQEMUWrite) {
	t.Helper()
	args := []string{"-f", "qcow2"}
	for _, w := range writes {
		args = append(args, "-c", w.cmd())
	}
	nbdQEMURun(t, "qemu-io", append(args, image)...)
}

func nbdQEMUConvert(t *testing.T, image, raw string) []byte {
	t.Helper()
	nbdQEMURun(t, "qemu-img", "convert", "-f", "qcow2", "-O", "raw", image, raw)
	b, err := os.ReadFile(raw)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// nbdQEMUServer is a qemu-nbd child process in its own process group.
type nbdQEMUServer struct {
	sock string
	cmd  *exec.Cmd
	done chan struct{} // closed when the process has been waited for
	err  error         // Wait's result, valid after done
	out  *nbdSyncBuffer
}

// nbdSyncBuffer is a bytes.Buffer safe for the exec copier and the test.
type nbdSyncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *nbdSyncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *nbdSyncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}

// nbdStartQEMUNBD runs qemu-nbd -k sock args... It exits after its single
// client disconnects; t.Cleanup kills its process group if it is still there.
func nbdStartQEMUNBD(t *testing.T, sock string, args ...string) *nbdQEMUServer {
	t.Helper()
	s := &nbdQEMUServer{sock: sock, done: make(chan struct{}), out: &nbdSyncBuffer{}}
	s.cmd = exec.Command("qemu-nbd", append([]string{"-k", sock}, args...)...)
	s.cmd.Stdout, s.cmd.Stderr = s.out, s.out
	s.cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := s.cmd.Start(); err != nil {
		t.Fatalf("start qemu-nbd: %v", err)
	}
	go func() {
		s.err = s.cmd.Wait()
		close(s.done)
	}()
	t.Cleanup(func() {
		_ = syscall.Kill(-s.cmd.Process.Pid, syscall.SIGKILL)
		<-s.done
	})
	return s
}

// dial connects once qemu-nbd listens, checking by SO_PEERCRED that the peer
// is the qemu-nbd process (the hook the node uses for QEMU's pid). Only
// refused connections are retried: qemu-nbd exits after its first client.
func (s *nbdQEMUServer) dial(t *testing.T, opts Options) *Client {
	t.Helper()
	opts.CheckPeer = func(uc *net.UnixConn) error {
		raw, err := uc.SyscallConn()
		if err != nil {
			return err
		}
		var cred *unix.Ucred
		var credErr error
		if err := raw.Control(func(fd uintptr) {
			cred, credErr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
		}); err != nil {
			return err
		}
		if credErr != nil {
			return credErr
		}
		if int(cred.Pid) != s.cmd.Process.Pid {
			return fmt.Errorf("peer pid %d, want qemu-nbd %d", cred.Pid, s.cmd.Process.Pid)
		}
		return nil
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		select {
		case <-s.done:
			t.Fatalf("qemu-nbd exited before serving: %v\n%s", s.err, s.out)
		default:
		}
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		c, err := Dial(ctx, s.sock, opts)
		cancel()
		if err == nil {
			t.Cleanup(func() { c.Close() })
			return c
		}
		if !errors.Is(err, syscall.ECONNREFUSED) && !errors.Is(err, syscall.ENOENT) {
			t.Fatalf("Dial qemu-nbd: %v\n%s", err, s.out)
		}
		if time.Now().After(deadline) {
			t.Fatalf("qemu-nbd did not listen on %s: %v\n%s", s.sock, err, s.out)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// waitExit waits for qemu-nbd to exit cleanly after its client disconnected,
// so the image is closed (metadata and bitmaps written).
func (s *nbdQEMUServer) waitExit(t *testing.T) {
	t.Helper()
	select {
	case <-s.done:
		if s.err != nil {
			t.Fatalf("qemu-nbd: %v\n%s", s.err, s.out)
		}
	case <-time.After(30 * time.Second):
		t.Fatalf("qemu-nbd still running after its client disconnected\n%s", s.out)
	}
}
