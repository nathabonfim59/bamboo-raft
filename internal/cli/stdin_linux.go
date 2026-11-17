//go:build linux

package cli

import (
	"os"
	"strconv"
	"syscall"
)

// reopenPollable opens an independent, pollable reader for f. Inherited stdio
// files are created with os.NewFile and are not registered with Go's poller, so
// closing them can wait on a blocked Read. /proc/self/fd/N gives a fresh file
// description that os.OpenFile registers with the poller; closing it wakes the
// read. The descriptor comes from SyscallConn, not File.Fd, because Fd can put
// the original nonblocking descriptor back into blocking mode. O_NONBLOCK is
// passed to the open so opening a FIFO with no writer cannot block, and
// O_NOCTTY keeps a terminal from becoming the controlling terminal.
//
// Regular files are left alone: reopening them starts a fresh file
// description at offset zero, which would lose the caller's consumed offset.
// Their reads do not block indefinitely, so cancellation does not need a
// pollable replacement.
func reopenPollable(f *os.File) (*os.File, bool) {
	info, err := f.Stat()
	if err != nil || info.Mode().IsRegular() {
		return nil, false
	}
	raw, err := f.SyscallConn()
	if err != nil {
		return nil, false
	}
	var (
		reopened *os.File
		openErr  error
	)
	if err := raw.Control(func(fd uintptr) {
		path := "/proc/self/fd/" + strconv.Itoa(int(fd))
		reopened, openErr = os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK|syscall.O_NOCTTY, 0)
	}); err != nil || openErr != nil {
		return nil, false
	}
	return reopened, true
}
