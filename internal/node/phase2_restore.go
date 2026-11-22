// CreateVM from_manifest: eager restore (Phase 2a). The request is checked
// and its manifest fetched before any record exists (loadRestore), so a
// refused restore leaves no VM. The create's boot then writes the manifest
// into the fresh overlay through a maintenance session (D3) before the
// first launch: QEMU's writable NBD export of rootdisk, a pool of NBD
// clients, every listed chunk fetched through the chunk cache. A restore
// that fails is a failed boot (FAILED record kept for DeleteVM, as in
// nodefake), with its own CreateVM code (restoreError).

package node

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/node/nbd"
	"github.com/nathabonfim59/bamboo-raft/internal/node/qemu"
	"github.com/nathabonfim59/bamboo-raft/internal/node/storage"
	"github.com/nathabonfim59/bamboo-raft/internal/objstore"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
	"github.com/nathabonfim59/bamboo-raft/internal/snapformat"
)

const (
	// chunkCacheDir is the chunk cache's directory under the data dir.
	chunkCacheDir = "chunks"
	// restoreWorkers is the number of parallel chunk writers (and NBD
	// connections) of one restore.
	restoreWorkers = 4
	// restoreExportID and restoreExportName name the writable export of
	// rootdisk a restore writes through.
	restoreExportID   = "bbraft-restore"
	restoreExportName = "restore"
)

// restoreSource is a checked CreateVM from_manifest: the manifest, its
// object bytes (recorded as the VM's local last manifest) and its image.
type restoreSource struct {
	manifestKey string
	manifest    *snapformat.Manifest
	encoded     []byte
	image       storage.Image
}

// key is the manifest key, empty for a nil source.
func (r *restoreSource) key() string {
	if r == nil {
		return ""
	}
	return r.manifestKey
}

// chunks is the manifest's entry count, zero for a nil source.
func (r *restoreSource) chunks() uint64 {
	if r == nil {
		return 0
	}
	return uint64(len(r.manifest.Chunks))
}

// restoreError is a failed restore with the CreateVM code its callers get
// (waitBoot): a missing or corrupt chunk and NBD/QMP failures are INTERNAL,
// an object store outage UNAVAILABLE.
type restoreError struct {
	code codes.Code
	err  error
}

func (e *restoreError) Error() string { return "restore: " + e.err.Error() }
func (e *restoreError) Unwrap() error { return e.err }

// grpcStatus is the error's gRPC status.
func (e *restoreError) grpcStatus() error { return status.Errorf(e.code, "%v", e) }

func restoreFailed(code codes.Code, format string, args ...any) error {
	return &restoreError{code: code, err: fmt.Errorf(format, args...)}
}

// objectStoreStatus maps an object store error outside a VM record:
// context errors keep their codes, anything else is UNAVAILABLE.
func objectStoreStatus(op string, err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return status.FromContextError(err).Err()
	}
	return status.Errorf(codes.Unavailable, "object store: %s: %v", op, err)
}

// loadRestore fetches and checks a from_manifest request's manifest, in
// nodefake's order, after checkFromManifest's cheap checks and before any
// reservation or record exists: a missing manifest is NOT_FOUND, a store
// failure UNAVAILABLE, a manifest that does not decode (or names another
// key) INTERNAL, an explicit image other than the manifest's
// INVALID_ARGUMENT, the manifest's image missing here, or installed with
// another SHA-256 than the manifest records, FAILED_PRECONDITION. The disk
// size is checked by joinCreate with the other resources.
func (m *Manager) loadRestore(ctx context.Context, req *nodev1.CreateVMRequest) (*restoreSource, error) {
	key := req.GetFromManifest()
	chain, gen, err := snapformat.ParseManifestKey(key)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	encoded, err := readManifestObject(ctx, m.cfg.Store, key)
	if errors.Is(err, objstore.ErrNotFound) {
		return nil, status.Errorf(codes.NotFound, "manifest %s not found", key)
	}
	if err != nil {
		if errors.Is(err, errManifestTooLarge) {
			return nil, status.Errorf(codes.Internal, "manifest %s: %v", key, err)
		}
		return nil, objectStoreStatus("get manifest", err)
	}
	man, err := snapformat.DecodeManifest(encoded)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "manifest %s: %v", key, err)
	}
	if man.ChainID != chain || man.Generation != gen {
		return nil, status.Errorf(codes.Internal, "manifest %s describes chain %s generation %d", key, man.ChainID, man.Generation)
	}
	if img := req.GetImage(); img != "" && img != man.Image {
		return nil, status.Errorf(codes.InvalidArgument, "image %q differs from the manifest's %q", img, man.Image)
	}
	img, ok := m.images[man.Image]
	if !ok {
		return nil, status.Errorf(codes.FailedPrecondition, "image %q is not on this node", man.Image)
	}
	if man.ImageSHA256 != "" && man.ImageSHA256 != img.RootSHA256 {
		return nil, status.Errorf(codes.FailedPrecondition, "image %q on this node is not the build the manifest was taken on", man.Image)
	}
	return &restoreSource{manifestKey: key, manifest: man, encoded: encoded, image: img}, nil
}

var errManifestTooLarge = fmt.Errorf("object exceeds %d bytes", snapformat.MaxManifestBytes)

// readManifestObject reads a manifest object, at most
// snapformat.MaxManifestBytes.
func readManifestObject(ctx context.Context, store objstore.Store, key string) ([]byte, error) {
	rc, err := store.Get(ctx, key, 0, -1)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	b, err := io.ReadAll(io.LimitReader(rc, snapformat.MaxManifestBytes+1))
	if err != nil {
		return nil, err
	}
	if len(b) > snapformat.MaxManifestBytes {
		return nil, errManifestTooLarge
	}
	return b, nil
}

// restoreStats counts one restore's work for its log line.
type restoreStats struct {
	written    atomic.Uint64 // chunks written with data
	zeroes     atomic.Uint64 // Zero entries written as zeroes
	cacheHits  atomic.Uint64 // chunks the cache already held
	fetched    atomic.Uint64 // chunks fetched from the store
	bytesFetch atomic.Uint64 // uncompressed bytes of the fetched chunks
	bytesWrite atomic.Uint64 // bytes written with data
}

// restoreDisk runs the eager restore of a create in progress: a
// maintenance session on the BOOTING record writes v.restore's manifest
// into the new overlay, clears bbraft-dirty (the disk now equals the
// manifest) and records the manifest as the VM's last one, so its first
// SnapshotVM is incremental over it. Chunks the manifest does not list stay
// on the base image. ctx is the boot worker's (no BootTimeout): the last
// waiter leaving, DeleteVM and the node's shutdown end it.
func (m *Manager) restoreDisk(ctx context.Context, v *vm) error {
	src := v.restore
	start := m.now()
	var stats restoreStats
	err := m.maintenance(ctx, v, func(ctx context.Context, s *maintSession) error {
		return m.writeRestore(ctx, v, s, src, &stats)
	})
	if err != nil {
		return restoreFailure(ctx, err)
	}
	m.log.Info("node: disk restored from manifest", "sandbox", v.id, "manifest", src.manifestKey,
		"chunks", len(src.manifest.Chunks), "chunks_written", stats.written.Load(), "zero_chunks", stats.zeroes.Load(),
		"cache_hits", stats.cacheHits.Load(), "chunks_fetched", stats.fetched.Load(),
		"bytes_fetched", stats.bytesFetch.Load(), "bytes_written", stats.bytesWrite.Load(),
		"duration", m.now().Sub(start))
	return nil
}

// restoreFailure gives a failed restore session its CreateVM code. Context
// ends (cancellation, DeleteVM, shutdown) stay as they are, so the boot
// failure policy treats them as aborts; a status from the session (an
// unresolved record, a busy node) keeps its code.
func restoreFailure(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return fmt.Errorf("restore: %w", errors.Join(err, context.Cause(ctx)))
	}
	if _, ok := errors.AsType[*restoreError](err); ok {
		return err
	}
	if st, ok := status.FromError(err); ok {
		return &restoreError{code: st.Code(), err: err}
	}
	return &restoreError{code: codes.Internal, err: err}
}

// writeRestore is restoreDisk's work inside the session.
func (m *Manager) writeRestore(ctx context.Context, v *vm, s *maintSession, src *restoreSource, stats *restoreStats) error {
	id := v.id
	client := s.QMP()
	switch sup, known := m.learnPhase2(ctx, client); {
	case !known:
		return restoreFailed(codes.FailedPrecondition, "this node could not check its QEMU for Phase 2 support")
	case !sup.Available():
		return restoreFailed(codes.FailedPrecondition, "%s", sup.Reason())
	}
	v.mu.Lock()
	meta := v.meta
	v.mu.Unlock()
	size, ok := resourcesOf(meta.Resources).diskBytes()
	if !ok || size < src.manifest.DiskBytes {
		return restoreFailed(codes.Internal, "sandbox %s: disk of %d GiB cannot hold the manifest's %d bytes", id, meta.Resources.DiskGiB, src.manifest.DiskBytes)
	}
	host, err := s.StartNBD(ctx)
	if err != nil {
		return err
	}
	if err := client.Execute(ctx, "block-export-add", map[string]any{
		"type": "nbd", "id": restoreExportID, "node-name": qemu.RootDiskNode, "name": restoreExportName, "writable": true,
	}, nil); err != nil {
		return fmt.Errorf("block-export-add: %w", err)
	}
	open := m.openRestoreDisk
	if open == nil {
		open = openNBDRestoreDisk
	}
	disk, err := open(ctx, restoreDiskOptions{Socket: host, Conns: restoreWorkers, PeerPID: s.PID()})
	if err != nil {
		return fmt.Errorf("open restore export: %w", err)
	}
	if disk.Size() != size {
		disk.Close()
		return fmt.Errorf("restore export is %d bytes, the disk %d", disk.Size(), size)
	}
	err = m.writeChunks(ctx, disk, src.manifest, stats)
	if err == nil {
		err = disk.Flush(ctx)
	}
	if closeErr := disk.Close(); closeErr != nil {
		m.log.Debug("node: close restore export", "sandbox", id, "error", closeErr)
	}
	if err != nil {
		return err
	}
	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), maintTeardownTimeout)
	defer cancel()
	if err := s.StopNBD(cctx); err != nil {
		return err
	}
	// The disk equals the manifest now: nothing is dirty relative to it.
	if err := client.Execute(ctx, "block-dirty-bitmap-clear", map[string]string{"node": qemu.RootDiskNode, "name": qemu.DirtyBitmap}, nil); err != nil {
		return fmt.Errorf("block-dirty-bitmap-clear: %w", err)
	}
	if err := m.store.SetLastManifest(id, src.manifestKey, src.encoded); err != nil {
		return fmt.Errorf("record last manifest %s: %w", src.manifestKey, err)
	}
	// Only now, before the session quits: a QEMU that then has to be
	// killed marks the bitmap untrusted again (endMaintenance).
	if err := m.store.ClearFullScanNext(id); err != nil {
		return fmt.Errorf("clear full scan next: %w", err)
	}
	return nil
}

// writeChunks writes every manifest entry at its offset with restoreWorkers
// parallel workers: a Zero entry as zeroes, any other through the chunk
// cache. The first failure stops the others.
func (m *Manager) writeChunks(ctx context.Context, disk restoreDisk, man *snapformat.Manifest, stats *restoreStats) error {
	if m.chunks == nil {
		return restoreFailed(codes.FailedPrecondition, "this node has no object store")
	}
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	entries := make(chan snapformat.Entry)
	var wg sync.WaitGroup
	for range restoreWorkers {
		wg.Go(func() {
			var buf []byte
			for e := range entries {
				var err error
				buf, err = m.writeEntry(ctx, disk, man, e, buf, stats)
				if err != nil {
					cancel(err)
					return
				}
			}
		})
	}
feed:
	for _, e := range man.Chunks {
		select {
		case entries <- e:
		case <-ctx.Done():
			break feed
		}
	}
	close(entries)
	wg.Wait()
	if err := context.Cause(ctx); err != nil {
		return err
	}
	return nil
}

// writeEntry writes one manifest entry; buf is the worker's chunk buffer.
func (m *Manager) writeEntry(ctx context.Context, disk restoreDisk, man *snapformat.Manifest, e snapformat.Entry, buf []byte, stats *restoreStats) ([]byte, error) {
	off := e.Index * snapformat.ChunkSize
	n := man.ChunkLen(e.Index)
	if e.Zero {
		if err := disk.WriteZeroes(ctx, off, n); err != nil {
			return buf, fmt.Errorf("chunk %d: write zeroes: %w", e.Index, err)
		}
		stats.zeroes.Add(1)
		return buf, nil
	}
	hit := m.chunks.Has(e.Hash)
	data, err := m.chunks.Get(ctx, snapshotStore{m.cfg.Store}, e.Hash, buf)
	if err != nil {
		return buf, chunkError(e, err)
	}
	if uint64(len(data)) != n {
		return data, restoreFailed(codes.Internal, "chunk %d (%s) holds %d bytes, the manifest's chunk %d: %w", e.Index, e.Hash, len(data), n, snapformat.ErrCorruptChunk)
	}
	if hit {
		stats.cacheHits.Add(1)
	} else {
		stats.fetched.Add(1)
		stats.bytesFetch.Add(n)
	}
	if err := disk.Write(ctx, off, data); err != nil {
		return data, fmt.Errorf("chunk %d: write: %w", e.Index, err)
	}
	stats.written.Add(1)
	stats.bytesWrite.Add(n)
	return data, nil
}

// chunkError maps a chunk fetch failure: missing or corrupt is INTERNAL
// (the manifest cannot be restored), an object store outage UNAVAILABLE.
// Context ends stay as they are.
func chunkError(e snapformat.Entry, err error) error {
	switch {
	case errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded):
		return err
	case errors.Is(err, objstore.ErrNotFound):
		return restoreFailed(codes.Internal, "chunk %d (%s) is missing from the object store: %w", e.Index, e.Hash, err)
	case errors.Is(err, snapformat.ErrCorruptChunk):
		return restoreFailed(codes.Internal, "chunk %d: %w", e.Index, err)
	}
	if _, ok := errors.AsType[*objectStoreError](err); ok {
		return restoreFailed(codes.Unavailable, "chunk %d: %w", e.Index, err)
	}
	return restoreFailed(codes.Internal, "chunk %d: %w", e.Index, err)
}

// restoreDiskOptions configures openNBDRestoreDisk.
type restoreDiskOptions struct {
	// Socket is the host path of QEMU's NBD server socket.
	Socket string
	// Conns is the number of connections (one per restore worker).
	Conns int
	// PeerPID is the QEMU pid every connection's SO_PEERCRED must match.
	PeerPID int
}

// restoreDisk is a writable disk a restore writes through. Write and
// WriteZeroes are safe for concurrent use.
type restoreDisk interface {
	Size() uint64
	Write(ctx context.Context, off uint64, data []byte) error
	WriteZeroes(ctx context.Context, off, length uint64) error
	// Flush makes every completed write durable.
	Flush(ctx context.Context) error
	Close() error
}

// nbdRestoreDisk is a restoreDisk over a pool of NBD connections to one
// writable export.
type nbdRestoreDisk struct {
	clients []*nbd.Client
	pool    chan *nbd.Client
	size    uint64
}

// openNBDRestoreDisk dials opts.Conns connections to the restore export.
// Every connection proves its peer is QEMU and must be writable.
func openNBDRestoreDisk(ctx context.Context, opts restoreDiskOptions) (restoreDisk, error) {
	if opts.Conns <= 0 {
		return nil, fmt.Errorf("nbd restore disk: %d connections", opts.Conns)
	}
	d := &nbdRestoreDisk{pool: make(chan *nbd.Client, opts.Conns)}
	for range opts.Conns {
		c, err := nbd.Dial(ctx, opts.Socket, nbd.Options{
			Export:    restoreExportName,
			CheckPeer: func(uc *net.UnixConn) error { return checkPeerPID(uc, opts.PeerPID) },
		})
		if err != nil {
			d.Close()
			return nil, err
		}
		if c.ReadOnly() {
			c.Close()
			d.Close()
			return nil, errors.New("nbd restore disk: the export is read-only")
		}
		if len(d.clients) > 0 && c.Size() != d.size {
			c.Close()
			d.Close()
			return nil, fmt.Errorf("nbd restore disk: connections disagree on the export size (%d, %d)", d.size, c.Size())
		}
		d.size = c.Size()
		d.clients = append(d.clients, c)
		d.pool <- c
	}
	return d, nil
}

func (d *nbdRestoreDisk) Size() uint64 { return d.size }

// with runs fn on a pooled connection.
func (d *nbdRestoreDisk) with(ctx context.Context, fn func(*nbd.Client) error) error {
	var c *nbd.Client
	select {
	case c = <-d.pool:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { d.pool <- c }()
	return fn(c)
}

func (d *nbdRestoreDisk) Write(ctx context.Context, off uint64, data []byte) error {
	return d.with(ctx, func(c *nbd.Client) error { return c.Write(ctx, off, data) })
}

func (d *nbdRestoreDisk) WriteZeroes(ctx context.Context, off, length uint64) error {
	return d.with(ctx, func(c *nbd.Client) error { return c.WriteZeroes(ctx, off, length) })
}

// Flush flushes every connection: without NBD_FLAG_CAN_MULTI_CONN a flush
// covers only the writes completed on its own connection.
func (d *nbdRestoreDisk) Flush(ctx context.Context) error {
	for _, c := range d.clients {
		if err := c.Flush(ctx); err != nil {
			return fmt.Errorf("nbd restore disk: flush: %w", err)
		}
	}
	return nil
}

// Close closes every connection.
func (d *nbdRestoreDisk) Close() error {
	var errs []error
	for _, c := range d.clients {
		errs = append(errs, c.Close())
	}
	d.clients = nil
	return errors.Join(errs...)
}
