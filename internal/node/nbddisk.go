// NBD-backed snapshot disk: the disksnap.Disk a SnapshotVM reads through
// QEMU's NBD export of a cut (the live fleece node, or a STOPPED VM's
// rootdisk).

package node

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"net"

	"github.com/nathabonfim59/bamboo-raft/internal/node/nbd"
	"github.com/nathabonfim59/bamboo-raft/internal/node/qemu"
	"github.com/nathabonfim59/bamboo-raft/internal/snapformat"
)

// Meta contexts of a cut's export (block-export-add with bitmaps
// [bbraft-cut] and allocation-depth).
const (
	cutDirtyContext      = "qemu:dirty-bitmap:" + qemu.CutBitmap
	cutAllocationContext = "qemu:allocation-depth"
	// cutExportName is the NBD export name of a cut.
	cutExportName = "cut"
)

// Allocation depths that belong to the sandbox (its own layer, not the
// base image). On a live cut the export is the fleece node (depth 1) over
// rootdisk (depth 2) over the base; on a STOPPED export rootdisk is depth 1.
const (
	liveSandboxDepth    = 2
	stoppedSandboxDepth = 1
)

// cutDiskOptions configures openNBDDisk.
type cutDiskOptions struct {
	// Socket is the host path of QEMU's NBD server socket.
	Socket string
	// Conns is the number of connections (one per snapshot worker).
	Conns int
	// MaxDepth is the deepest allocation depth counted as the sandbox's.
	MaxDepth uint32
	// PeerPID is the QEMU pid every connection's SO_PEERCRED must match.
	PeerPID int
}

// nbdDisk implements disksnap.Disk over a small pool of NBD connections to
// one export. ReadChunk is safe for concurrent use: each call takes a
// connection from the pool. Dirty and Allocated query the first connection,
// which serializes its own requests.
type nbdDisk struct {
	clients  []*nbd.Client
	pool     chan *nbd.Client
	size     uint64
	maxDepth uint32
}

// cutDisk is a snapshot disk the caller must close.
type cutDisk interface {
	Size() uint64
	Dirty(ctx context.Context) iter.Seq2[uint64, error]
	Allocated(ctx context.Context) iter.Seq2[uint64, error]
	ReadChunk(ctx context.Context, i uint64, buf []byte) ([]byte, error)
	Close() error
}

// openNBDDisk dials opts.Conns connections to the cut export. Every
// connection proves its peer is QEMU and negotiates both meta contexts.
func openNBDDisk(ctx context.Context, opts cutDiskOptions) (cutDisk, error) {
	if opts.Conns <= 0 {
		return nil, fmt.Errorf("nbd disk: %d connections", opts.Conns)
	}
	if opts.MaxDepth == 0 {
		return nil, errors.New("nbd disk: no sandbox allocation depth")
	}
	d := &nbdDisk{pool: make(chan *nbd.Client, opts.Conns), maxDepth: opts.MaxDepth}
	for range opts.Conns {
		c, err := nbd.Dial(ctx, opts.Socket, nbd.Options{
			Export:       cutExportName,
			MetaContexts: []string{cutDirtyContext, cutAllocationContext},
			CheckPeer:    func(uc *net.UnixConn) error { return checkPeerPID(uc, opts.PeerPID) },
		})
		if err != nil {
			d.Close()
			return nil, err
		}
		if len(d.clients) > 0 && c.Size() != d.size {
			c.Close()
			d.Close()
			return nil, fmt.Errorf("nbd disk: connections disagree on the export size (%d, %d)", d.size, c.Size())
		}
		d.size = c.Size()
		d.clients = append(d.clients, c)
		d.pool <- c
	}
	return d, nil
}

// checkPeerPID proves the socket's peer is the process pid (SO_PEERCRED).
func checkPeerPID(uc *net.UnixConn, pid int) error {
	if pid <= 0 {
		return errors.New("no QEMU pid to check the peer against")
	}
	peer, err := unixPeerPID(uc)
	if err != nil {
		return err
	}
	if peer != pid {
		return fmt.Errorf("peer pid %d is not QEMU's pid %d", peer, pid)
	}
	return nil
}

// Size implements disksnap.Disk.
func (d *nbdDisk) Size() uint64 { return d.size }

// Dirty implements disksnap.Disk: a chunk is dirty if any byte of it is
// set in bbraft-cut.
func (d *nbdDisk) Dirty(ctx context.Context) iter.Seq2[uint64, error] {
	return d.chunks(ctx, cutDirtyContext, func(flags uint32) bool { return flags&1 != 0 })
}

// Allocated implements disksnap.Disk: a chunk is the sandbox's if any
// extent in it has an allocation depth in 1..maxDepth.
func (d *nbdDisk) Allocated(ctx context.Context) iter.Seq2[uint64, error] {
	return d.chunks(ctx, cutAllocationContext, func(depth uint32) bool { return depth >= 1 && depth <= d.maxDepth })
}

// chunks yields, ascending and each once, the chunks any extent of
// metaContext marked by want touches.
func (d *nbdDisk) chunks(ctx context.Context, metaContext string, want func(uint32) bool) iter.Seq2[uint64, error] {
	return func(yield func(uint64, error) bool) {
		if len(d.clients) == 0 {
			yield(0, errors.New("nbd disk: closed"))
			return
		}
		var next uint64 // the lowest chunk not yielded yet
		for e, err := range d.clients[0].Extents(ctx, metaContext, 0, d.size) {
			if err != nil {
				yield(0, err)
				return
			}
			if !want(e.Flags) || e.Length == 0 {
				continue
			}
			first := max(e.Offset/snapformat.ChunkSize, next)
			last := (e.Offset + e.Length - 1) / snapformat.ChunkSize
			for i := first; i <= last; i++ {
				if !yield(i, nil) {
					return
				}
			}
			next = max(next, last+1)
		}
	}
}

// ReadChunk implements disksnap.Disk.
func (d *nbdDisk) ReadChunk(ctx context.Context, i uint64, buf []byte) ([]byte, error) {
	off := i * snapformat.ChunkSize
	if i >= (d.size+snapformat.ChunkSize-1)/snapformat.ChunkSize {
		return nil, fmt.Errorf("nbd disk: chunk %d past the %d byte export", i, d.size)
	}
	n := min(snapformat.ChunkSize, d.size-off)
	if uint64(cap(buf)) < n {
		buf = make([]byte, n)
	}
	buf = buf[:n]
	var c *nbd.Client
	select {
	case c = <-d.pool:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	defer func() { d.pool <- c }()
	if err := c.Read(ctx, off, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

// Close closes every connection.
func (d *nbdDisk) Close() error {
	var errs []error
	for _, c := range d.clients {
		errs = append(errs, c.Close())
	}
	d.clients = nil
	return errors.Join(errs...)
}
