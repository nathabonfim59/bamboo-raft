// Process output reads (Phase 1): the log windows of GetProcess and the
// attach stream. Both read through pinned no-follow handles on the
// processes directory, so a symlink planted at a log's name cannot
// redirect the read, and both treat a missing log as empty.
package guest

import (
	"cmp"
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"slices"
	"time"

	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/proto/guestv1"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

const (
	// procDefaultWindow is GetProcess's per-stream window when
	// limit_bytes is 0, and the attach chunk size.
	procDefaultWindow = 1 << 20
	// procMaxWindow is the largest per-stream window GetProcess accepts.
	procMaxWindow = 8 << 20
	// procPollInterval is how often an attach stream re-reads the logs of
	// a still-running process.
	procPollInterval = 50 * time.Millisecond

	// procPassChunks bounds one attach pump pass per stream, so a stdout
	// that grows without end cannot starve stderr or the exit frame.
	procPassChunks = 32
)

// GetProcess returns the process and a window of each log: without an
// offset for a stream, the last limit_bytes of that log (truncated set
// when the log is longer); with an offset, the bytes from it. Offsets
// past the end return no data at the log's end.
func (s *Service) GetProcess(ctx context.Context, req *guestv1.GetProcessRequest) (*nodev1.ProcessOutput, error) {
	limit := int64(cmp.Or(req.GetLimitBytes(), procDefaultWindow))
	if limit > procMaxWindow {
		return nil, status.Errorf(codes.InvalidArgument, "limit_bytes %d is above %d", req.GetLimitBytes(), procMaxWindow)
	}
	p, err := s.resolveProc(ctx, req.GetProcessId())
	if err != nil {
		return nil, err
	}
	outName, errName := procLogNames(req.GetProcessId())
	res := &nodev1.ProcessOutput{Process: s.processSnapshot(p)}
	res.Stdout, res.StdoutOffset, res.StdoutTruncated, res.More, err = s.logWindow(outName, req.StdoutOffset, limit)
	if err != nil {
		return nil, err
	}
	var more bool
	res.Stderr, res.StderrOffset, res.StderrTruncated, more, err = s.logWindow(errName, req.StderrOffset, limit)
	if err != nil {
		return nil, err
	}
	res.More = res.More || more
	return res, nil
}

// logWindow reads one log window: the last limit bytes when offset is nil,
// else the bytes from offset, at most limit of them. A missing log reads
// as empty; a name that is not a regular file is an internal error, not
// data to serve.
func (s *Service) logWindow(name string, offset *int64, limit int64) (data []byte, start int64, truncated, more bool, err error) {
	f, size, err := s.openProcLogRead(name)
	if err != nil {
		return nil, 0, false, false, err
	}
	if f != nil {
		defer f.Close()
	}
	if offset == nil {
		start = max(size-limit, 0)
		truncated = start > 0
	} else {
		start = min(max(*offset, 0), size)
		more = start+limit < size
	}
	end := min(start+limit, size)
	if end <= start {
		return nil, start, truncated, more, nil
	}
	data = make([]byte, end-start)
	n, rerr := f.ReadAt(data, start)
	if rerr != nil && !errors.Is(rerr, io.EOF) {
		return nil, start, truncated, more, status.Errorf(codes.Internal, "read %s: %v", name, rerr)
	}
	// A log truncated underneath the service yields only the bytes that
	// exist, never zero padding for the bytes that were counted a moment
	// earlier.
	if int64(n) < int64(len(data)) {
		data = data[:n]
	}
	return data, start, truncated, more, nil
}

// AttachProcess streams a process's output from the given offsets: one
// attached frame, then stdout/stderr chunks in write order per stream (no
// gaps, no repeats: each chunk continues at the previous one's
// next_offset), and once the process has ended and all of its output was
// sent, one exit frame and the end of the stream. A process that already
// ended sends its remaining output and exit at once. Cancelling the RPC
// only stops the stream: nothing in the attach path ever signals the
// process. Offsets past the end of a log start at its end.
func (s *Service) AttachProcess(req *guestv1.AttachProcessRequest, stream grpc.ServerStreamingServer[nodev1.ProcessEvent]) error {
	ctx := stream.Context()
	p, err := s.resolveProc(ctx, req.GetProcessId())
	if err != nil {
		return err
	}
	outName, errName := procLogNames(req.GetProcessId())
	outF, outSize, err := s.openProcLogRead(outName)
	if err != nil {
		return err
	}
	if outF != nil {
		defer outF.Close()
	}
	errF, errSize, err := s.openProcLogRead(errName)
	if err != nil {
		return err
	}
	if errF != nil {
		defer errF.Close()
	}
	outOff := min(max(req.GetStdoutOffset(), 0), outSize)
	errOff := min(max(req.GetStderrOffset(), 0), errSize)
	if err := stream.Send(&nodev1.ProcessEvent{Event: &nodev1.ProcessEvent_Attached_{
		Attached: &nodev1.ProcessEvent_Attached{
			Process:      s.processSnapshot(p),
			StdoutOffset: outOff,
			StderrOffset: errOff,
		},
	}}); err != nil {
		return err
	}

	// pump sends bytes from *off, in procDefaultWindow chunks with
	// contiguous next_offset values, for at most procPassChunks chunks: a
	// stream that grows without end must not starve the other stream or
	// the terminal frame. more reports that the pass budget was spent
	// before EOF, so the caller can drain a finished stream to its end.
	// Every chunk's data is cloned, so no frame ever aliases the read buffer.
	pump := func(f *os.File, name string, off *int64, wrap func(*nodev1.ProcessEvent_Chunk) *nodev1.ProcessEvent) (more bool, err error) {
		if f == nil {
			return false, nil
		}
		buf := make([]byte, procDefaultWindow)
		for range procPassChunks {
			if ctx.Err() != nil {
				return false, status.FromContextError(ctx.Err()).Err()
			}
			n, rerr := f.ReadAt(buf, *off)
			if n > 0 {
				chunk := &nodev1.ProcessEvent_Chunk{Data: slices.Clone(buf[:n]), NextOffset: *off + int64(n)}
				if serr := stream.Send(wrap(chunk)); serr != nil {
					return false, serr
				}
				*off += int64(n)
			}
			if rerr != nil {
				if errors.Is(rerr, io.EOF) {
					return false, nil
				}
				return false, status.Errorf(codes.Internal, "read %s: %v", name, rerr)
			}
		}
		return true, nil // pass budget spent: the outer loop services the rest
	}
	stdoutFrame := func(c *nodev1.ProcessEvent_Chunk) *nodev1.ProcessEvent {
		return &nodev1.ProcessEvent{Event: &nodev1.ProcessEvent_Stdout{Stdout: c}}
	}
	stderrFrame := func(c *nodev1.ProcessEvent_Chunk) *nodev1.ProcessEvent {
		return &nodev1.ProcessEvent{Event: &nodev1.ProcessEvent_Stderr{Stderr: c}}
	}

	// drain reads one finished stream to EOF in pass-sized chunks. The
	// process's final record is durable and both logs are closed by the time
	// done fires, so the drain terminates; the pass budget stays in place
	// while the process runs, and cancellation is checked every chunk.
	drain := func(f *os.File, name string, off *int64, wrap func(*nodev1.ProcessEvent_Chunk) *nodev1.ProcessEvent) error {
		for {
			more, err := pump(f, name, off, wrap)
			if err != nil {
				return err
			}
			if !more {
				return nil
			}
		}
	}

	tick := time.NewTicker(procPollInterval)
	defer tick.Stop()
	for {
		if _, err := pump(outF, outName, &outOff, stdoutFrame); err != nil {
			return err
		}
		if _, err := pump(errF, errName, &errOff, stderrFrame); err != nil {
			return err
		}
		select {
		case <-p.done:
			// Ended: every byte of both logs precedes the terminal frame.
			if err := drain(outF, outName, &outOff, stdoutFrame); err != nil {
				return err
			}
			if err := drain(errF, errName, &errOff, stderrFrame); err != nil {
				return err
			}
			return stream.Send(&nodev1.ProcessEvent{Event: &nodev1.ProcessEvent_Exit{Exit: s.processSnapshot(p)}})
		case <-tick.C:
		case <-ctx.Done():
			return status.FromContextError(ctx.Err()).Err()
		}
	}
}

// openProcLogRead opens one process log for reading without following a
// symlink planted at its name and without blocking on a FIFO there: the
// open is nonblocking and only a regular file is accepted. A missing log
// returns a nil file and no error: it reads as empty.
func (s *Service) openProcLogRead(name string) (*os.File, int64, error) {
	base, err := s.procStateDir()
	if err != nil {
		return nil, 0, status.Errorf(codes.Internal, "process state: %v", err)
	}
	f, err := openAt(base, name, unix.O_RDONLY|unix.O_NONBLOCK, 0)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, 0, nil
		}
		return nil, 0, status.Errorf(codes.Internal, "open %s: %v", name, err)
	}
	fi, err := f.Stat()
	if err != nil || !fi.Mode().IsRegular() {
		f.Close()
		return nil, 0, status.Errorf(codes.Internal, "%s is not a regular log file", name)
	}
	return f, fi.Size(), nil
}
