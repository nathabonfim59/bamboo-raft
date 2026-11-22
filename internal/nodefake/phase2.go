package nodefake

// Phase 2: snapshots in the object store and ResetVM.
//
// The fake's "disk" is the VM directory written as a deterministic tar
// stream (sorted entries, regular files, directories and symlinks with
// their modes and modification times), zero-padded to whole chunks. Its
// virtual disk size is resources.disk_gib (10 GiB when unset), so manifests
// list only the chunks the tar covers; everything past them "reads from the
// base image". SnapshotVM cuts the stream into snapformat chunks and writes a
// manifest; CreateVM from_manifest reassembles the stream and untars it.
// Unchanged files give unchanged chunks only up to the first change (tar is
// sequential), which is enough to exercise content-addressed skipping.

import (
	"archive/tar"
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/objstore"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
	"github.com/nathabonfim59/bamboo-raft/internal/snapformat"
)

// phase2 is the fake's Phase 2 state, embedded in Node.
type phase2 struct {
	// Store is the object store SnapshotVM writes to and CreateVM
	// from_manifest reads from. Set before use; nil makes both
	// FAILED_PRECONDITION.
	Store objstore.Store

	snapMu sync.Map // sandbox id → *sync.Mutex: SnapshotVM calls are serialized per VM
}

const defaultFakeDiskGiB = 10

func diskBytes(res *nodev1.Resources) uint64 {
	return cmp.Or(res.GetDiskGib(), defaultFakeDiskGiB) << 30
}

func (n *Node) snapLock(id string) func() {
	m, _ := n.snapMu.LoadOrStore(id, &sync.Mutex{})
	mu := m.(*sync.Mutex)
	mu.Lock()
	return mu.Unlock
}

// SnapshotVM implements nodev1.NodeServer.
func (n *Node) SnapshotVM(ctx context.Context, req *nodev1.SnapshotVMRequest) (*nodev1.Generation, error) {
	if n.Store == nil {
		return nil, status.Error(codes.FailedPrecondition, "this node has no object store")
	}
	if !snapformat.ValidChainID(req.GetChainId()) {
		return nil, status.Errorf(codes.InvalidArgument, "bad chain_id %q", req.GetChainId())
	}
	id := req.GetSandboxId()
	defer n.snapLock(id)()

	n.mu.Lock()
	vm, ok := n.vms[id]
	if !ok {
		n.mu.Unlock()
		return nil, status.Errorf(codes.NotFound, "sandbox %s not found", id)
	}
	state := vm.State
	res := vm.GetResources()
	n.mu.Unlock()
	if state != nodev1.VMState_VM_STATE_READY && state != nodev1.VMState_VM_STATE_STOPPED {
		return nil, status.Errorf(codes.FailedPrecondition, "sandbox %s is %s", id, state)
	}

	key := snapformat.ManifestKey(req.ChainId, req.GetGeneration())
	if _, err := n.Store.Head(ctx, key); err == nil {
		return nil, status.Errorf(codes.AlreadyExists, "manifest %s exists", key)
	} else if !errors.Is(err, objstore.ErrNotFound) {
		return nil, storeError("head manifest", err)
	}

	frozen := state == nodev1.VMState_VM_STATE_READY && (req.Freeze == nil || req.GetFreeze())
	cut := time.Now()
	stream, err := tarDir(n.vmDir(id))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "read disk: %v", err)
	}
	m := &snapformat.Manifest{
		Version:    snapformat.Version,
		ChainID:    req.ChainId,
		Generation: req.GetGeneration(),
		SandboxID:  id,
		Image:      Image,
		DiskBytes:  diskBytes(res),
		ChunkSize:  snapformat.ChunkSize,
		CreatedAt:  cut.UTC(),
	}
	if uint64(len(stream)) > m.DiskBytes {
		return nil, status.Errorf(codes.ResourceExhausted, "disk full: %d bytes of files on a %d byte disk", len(stream), m.DiskBytes)
	}
	gen := &nodev1.Generation{
		ManifestKey:     key,
		ChainId:         req.ChainId,
		Generation:      req.GetGeneration(),
		Image:           Image,
		DiskBytes:       m.DiskBytes,
		CreatedAtUnixMs: cut.UnixMilli(),
		Frozen:          frozen,
	}
	for i := 0; i < len(stream); i += snapformat.ChunkSize {
		chunk := stream[i:min(i+snapformat.ChunkSize, len(stream))]
		idx := uint64(i / snapformat.ChunkSize)
		if snapformat.IsZero(chunk) {
			m.Chunks = append(m.Chunks, snapformat.Entry{Index: idx, Zero: true})
			continue
		}
		h, obj, err := snapformat.EncodeChunk(chunk)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "encode chunk: %v", err)
		}
		m.Chunks = append(m.Chunks, snapformat.Entry{Index: idx, Hash: h})
		gen.ContentBytes += uint64(len(chunk))
		switch err := n.Store.PutIfAbsent(ctx, snapformat.ChunkKey(h), bytes.NewReader(obj), int64(len(obj))); {
		case err == nil:
			gen.ChunksUploaded++
			gen.BytesUploaded += uint64(len(obj))
		case errors.Is(err, objstore.ErrExists):
		default:
			return nil, storeError("put chunk", err)
		}
	}
	gen.Chunks = uint64(len(m.Chunks))
	b, err := snapformat.EncodeManifest(m)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "encode manifest: %v", err)
	}
	switch err := n.Store.PutIfAbsent(ctx, key, bytes.NewReader(b), int64(len(b))); {
	case errors.Is(err, objstore.ErrExists):
		return nil, status.Errorf(codes.AlreadyExists, "manifest %s exists", key)
	case err != nil:
		return nil, storeError("put manifest", err)
	}
	gen.BytesUploaded += uint64(len(b))
	return gen, nil
}

// ResetVM implements nodev1.NodeServer. The fake has no QEMU process, so a
// FAILED VM counts as one whose process still runs.
func (n *Node) ResetVM(ctx context.Context, req *nodev1.ResetVMRequest) (*nodev1.VM, error) {
	id := req.GetSandboxId()
	n.mu.Lock()
	vm, ok := n.vms[id]
	if !ok {
		n.mu.Unlock()
		return nil, status.Errorf(codes.NotFound, "sandbox %s not found", id)
	}
	if vm.State != nodev1.VMState_VM_STATE_READY && vm.State != nodev1.VMState_VM_STATE_FAILED {
		state := vm.State
		n.mu.Unlock()
		return nil, status.Errorf(codes.FailedPrecondition, "sandbox %s is %s", id, state)
	}
	vm.State = nodev1.VMState_VM_STATE_BOOTING
	vm.Error = ""
	bctx, cancel := context.WithCancel(context.Background())
	bt := &booting{cancel: cancel, done: make(chan struct{})}
	n.boots[id] = bt
	n.mu.Unlock()

	go func() {
		defer close(bt.done)
		defer cancel()
		n.killProcs(id, true)
		var err error
		if n.BootDelay > 0 {
			select {
			case <-time.After(n.BootDelay):
			case <-bctx.Done():
				err = bctx.Err()
			}
		}
		n.mu.Lock()
		defer n.mu.Unlock()
		delete(n.boots, id)
		if n.vms[id] != vm {
			return
		}
		if err != nil {
			vm.State = nodev1.VMState_VM_STATE_FAILED
			vm.Error = err.Error()
			return
		}
		n.markLostLocked(id)
		vm.State = nodev1.VMState_VM_STATE_READY
		vm.StartedAtUnixMs = time.Now().UnixMilli()
	}()
	return n.waitReady(ctx, vm)
}

// loadManifest fetches and checks a CreateVM from_manifest before the VM
// record exists, so its errors are the call's.
func (n *Node) loadManifest(ctx context.Context, req *nodev1.CreateVMRequest) (*snapformat.Manifest, error) {
	key := req.GetFromManifest()
	if req.GetFromSnapshot() != "" {
		return nil, status.Error(codes.InvalidArgument, "from_snapshot and from_manifest are exclusive")
	}
	if n.Store == nil {
		return nil, status.Error(codes.FailedPrecondition, "this node has no object store")
	}
	if _, _, err := snapformat.ParseManifestKey(key); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	b, err := n.readObject(ctx, key)
	if errors.Is(err, objstore.ErrNotFound) {
		return nil, status.Errorf(codes.NotFound, "manifest %s not found", key)
	}
	if err != nil {
		return nil, storeError("get manifest", err)
	}
	m, err := snapformat.DecodeManifest(b)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "manifest %s: %v", key, err)
	}
	if img := req.GetImage(); img != "" && img != m.Image {
		return nil, status.Errorf(codes.InvalidArgument, "image %q differs from the manifest's %q", img, m.Image)
	}
	if m.Image != Image {
		return nil, status.Errorf(codes.FailedPrecondition, "image %q is not on this node", m.Image)
	}
	if diskBytes(req.GetResources()) < m.DiskBytes {
		return nil, status.Errorf(codes.InvalidArgument, "disk of %d GiB is smaller than the manifest's %d bytes", req.GetResources().GetDiskGib(), m.DiskBytes)
	}
	return m, nil
}

// restoreManifest writes the manifest's disk into dir (eager restore).
func (n *Node) restoreManifest(ctx context.Context, m *snapformat.Manifest, dir string) error {
	var stream bytes.Buffer
	var buf []byte
	next := uint64(0)
	for _, e := range m.Chunks {
		for ; next < e.Index; next++ { // unlisted chunks inside the tar would be base image data: the fake has none
			stream.Write(make([]byte, m.ChunkLen(next)))
		}
		next = e.Index + 1
		if e.Zero {
			stream.Write(make([]byte, m.ChunkLen(e.Index)))
			continue
		}
		obj, err := n.readObject(ctx, snapformat.ChunkKey(e.Hash))
		if err != nil {
			return fmt.Errorf("chunk %d: %w", e.Index, err)
		}
		buf, err = snapformat.DecodeChunk(e.Hash, obj, buf)
		if err != nil {
			return err
		}
		stream.Write(buf)
	}
	return untarDir(&stream, dir)
}

func (n *Node) readObject(ctx context.Context, key string) ([]byte, error) {
	rc, err := n.Store.Get(ctx, key, 0, -1)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return io.ReadAll(rc)
}

func storeError(op string, err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return status.FromContextError(err).Err()
	}
	return status.Errorf(codes.Unavailable, "object store: %s: %v", op, err)
}

// fakeEpoch replaces zero modification times so tar headers stay valid.
var fakeEpoch = time.Unix(0, 0)

// tarDir writes dir as a deterministic tar stream padded to whole chunks.
func tarDir(dir string) ([]byte, error) {
	var out bytes.Buffer
	tw := tar.NewWriter(&out)
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(dir, p)
		if err != nil || rel == "." {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		link := ""
		if info.Mode()&fs.ModeSymlink != 0 {
			if link, err = os.Readlink(p); err != nil {
				return err
			}
		} else if !info.Mode().IsRegular() && !info.IsDir() {
			return nil // sockets, fifos: not part of the fake disk
		}
		hdr, err := tar.FileInfoHeader(info, link)
		if err != nil {
			return err
		}
		hdr.Name = filepath.ToSlash(rel)
		if info.IsDir() {
			hdr.Name += "/"
		}
		hdr.Uid, hdr.Gid, hdr.Uname, hdr.Gname = 0, 0, "", ""
		hdr.AccessTime, hdr.ChangeTime = time.Time{}, time.Time{}
		hdr.Format = tar.FormatPAX
		if hdr.ModTime.Before(fakeEpoch) {
			hdr.ModTime = fakeEpoch
		}
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		if info.Mode().IsRegular() {
			f, err := os.Open(p)
			if err != nil {
				return err
			}
			defer f.Close()
			if _, err := io.CopyN(tw, f, hdr.Size); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if err := tw.Close(); err != nil {
		return nil, err
	}
	if pad := out.Len() % snapformat.ChunkSize; pad != 0 {
		out.Write(make([]byte, snapformat.ChunkSize-pad))
	}
	return out.Bytes(), nil
}

// untarDir unpacks a tarDir stream into dir (created), confined by os.Root.
func untarDir(r io.Reader, dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return err
	}
	defer root.Close()
	type dirTime struct {
		name string
		t    time.Time
	}
	var dirs []dirTime
	tr := tar.NewReader(r)
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("untar: %w", err)
		}
		name := strings.TrimSuffix(hdr.Name, "/")
		mode := fs.FileMode(hdr.Mode) & fs.ModePerm
		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := root.MkdirAll(name, 0o755); err != nil {
				return err
			}
			if err := root.Chmod(name, mode|fs.FileMode(hdr.Mode)&(fs.ModeSetuid|fs.ModeSetgid|fs.ModeSticky)); err != nil {
				return err
			}
			dirs = append(dirs, dirTime{name, hdr.ModTime})
		case tar.TypeReg:
			f, err := root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, mode)
			if err != nil {
				return err
			}
			_, cerr := io.Copy(f, tr)
			if err := errors.Join(cerr, f.Close()); err != nil {
				return err
			}
			if err := root.Chmod(name, mode); err != nil {
				return err
			}
			if err := root.Chtimes(name, hdr.ModTime, hdr.ModTime); err != nil {
				return err
			}
		case tar.TypeSymlink:
			if err := root.Symlink(hdr.Linkname, name); err != nil {
				return err
			}
		default:
			return fmt.Errorf("untar: unexpected entry type %q for %s", hdr.Typeflag, hdr.Name)
		}
	}
	// Directory times last, deepest first, as writing files changed them.
	slices.Reverse(dirs)
	for _, d := range dirs {
		if err := root.Chtimes(d.name, d.t, d.t); err != nil {
			return err
		}
	}
	for _, d := range []string{"home/user", "tmp"} {
		if err := root.MkdirAll(d, 0o755); err != nil {
			return err
		}
	}
	return nil
}
