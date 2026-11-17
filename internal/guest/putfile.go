package guest

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/proto/guestv1"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// tempPrefix marks PutFile staging files. They live beside the target as
// hidden siblings and are removed on every failure path.
const tempPrefix = ".bbraft-put."

// uploadAbortTimeout bounds cleanup of a failed or canceled upload. The RPC
// context is already canceled on that path, so cleanup gets its own deadline
// to let the helper remove the staging file as the sandbox user.
const uploadAbortTimeout = 5 * time.Second

// PutFile writes one file as the sandbox user. The first message must be
// the header; the rest are data chunks of at most 1 MiB. The target is never
// touched as root: the upload runs in the file operations implementation
// (the credential-dropped helper in the real guest), which stages a hidden
// temporary sibling and only renames it over the target after this handler
// has seen a clean stream end and explicitly commits. A canceled or
// malformed stream aborts it, leaving the previous file intact and removing
// the staged file as the sandbox user.
func (s *Service) PutFile(stream grpc.ClientStreamingServer[guestv1.PutFileRequest, nodev1.PutFileResponse]) error {
	first, err := stream.Recv()
	if err != nil {
		return status.Error(codes.InvalidArgument, "first PutFile message must be the header")
	}
	hdr := first.GetHeader()
	if hdr == nil {
		return status.Error(codes.InvalidArgument, "first PutFile message must be the header")
	}
	up, err := s.files.newUpload(stream.Context(), hdr.GetPath(), hdr.GetMode())
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if committed {
			return
		}
		ctx, cancel := context.WithTimeout(context.WithoutCancel(stream.Context()), uploadAbortTimeout)
		defer cancel()
		if err := up.abort(ctx); err != nil {
			s.log.Warn("PutFile abort cleanup failed", "err", err)
		}
	}()

	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			// A canceled RPC may surface as a bare context error from
			// Recv; report the gRPC code as every other path does.
			if ctxErr := stream.Context().Err(); ctxErr != nil {
				return status.FromContextError(ctxErr).Err()
			}
			return err
		}
		switch part := msg.GetPart().(type) {
		case *guestv1.PutFileRequest_Header_:
			return status.Error(codes.InvalidArgument, "the PutFile header must be the first message")
		case *guestv1.PutFileRequest_Data:
			if len(part.Data) > maxUploadChunk {
				return status.Errorf(codes.InvalidArgument, "PutFile chunk of %d bytes exceeds %d bytes", len(part.Data), maxUploadChunk)
			}
			if err := up.write(part.Data); err != nil {
				return err
			}
		default:
			// An empty or unknown oneof is malformed, not an empty chunk.
			return status.Error(codes.InvalidArgument, "malformed PutFile message")
		}
	}
	// The stream may have been canceled after the last chunk: never commit
	// a file the client has given up on.
	if err := stream.Context().Err(); err != nil {
		return status.FromContextError(err).Err()
	}
	resp, err := up.commit(stream.Context())
	if err != nil {
		return err
	}
	committed = true
	return stream.SendAndClose(resp)
}

// fileUpload stages one PutFile. Write appends data, Commit publishes the
// staged file atomically and Abort discards it. Both paths run as the
// sandbox user; the target is only renamed in Commit.
type fileUpload interface {
	write([]byte) error
	commit(context.Context) (*nodev1.PutFileResponse, error)
	abort(context.Context) error
}

// newUpload starts a staged upload under fileServer's pinned roots.
func (f *fileServer) newUpload(_ context.Context, p string, mode uint32) (fileUpload, error) {
	rt, rel, canonical, err := f.resolve(p)
	if err != nil {
		return nil, err
	}
	if canonical == homeSpec || canonical == tmpSpec {
		return nil, status.Errorf(codes.InvalidArgument, "path %q is a directory", canonical)
	}
	if mode != 0 && mode&^0o777 != 0 {
		return nil, status.Errorf(codes.InvalidArgument, "mode %#o has bits beyond the permission bits", mode)
	}
	perm := fs.FileMode(mode) & fs.ModePerm
	if perm == 0 {
		perm = 0o644
	}
	// Borrow an independent handle from the already-pinned confinement
	// root. OpenRoot(".") keeps the original inode even if the configured
	// path is renamed or replaced with a symlink; re-opening that path with
	// os.OpenRoot would follow the new target and defeat the confinement.
	// The borrow keeps cleanup valid when a racing successful Shutdown
	// closes the service's shared roots while this upload is staged.
	uploadRoot, err := rt.r.OpenRoot(".")
	if err != nil {
		return nil, fileErr("put", canonical, err)
	}
	up := &fileUploadImpl{fs: f, root: uploadRoot, canonical: canonical, rel: rel, perm: perm}
	if err := up.mkdirParents(); err != nil {
		uploadRoot.Close()
		return nil, err
	}
	tmp := path.Join(path.Dir(rel), tempPrefix+randSuffix())
	file, err := uploadRoot.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		uploadRoot.Close()
		return nil, fileErr("put", canonical, err)
	}
	up.tmp, up.file = tmp, file
	return up, nil
}

// fileUploadImpl is the single-user staged upload used both in-process and
// inside the helper child.
type fileUploadImpl struct {
	fs        *fileServer
	root      *os.Root
	rel       string
	canonical string
	tmp       string
	file      *os.File
	perm      fs.FileMode
	size      int64
	done      bool
}

func (u *fileUploadImpl) write(b []byte) error {
	if u.done || u.file == nil {
		return status.Error(codes.Internal, "upload is already finished")
	}
	if len(b) > maxUploadChunk {
		return status.Errorf(codes.InvalidArgument, "PutFile chunk of %d bytes exceeds %d bytes", len(b), maxUploadChunk)
	}
	n, err := u.file.Write(b)
	u.size += int64(n)
	if err != nil {
		return status.Errorf(codes.Internal, "write: %v", err)
	}
	return nil
}

func (u *fileUploadImpl) commit(ctx context.Context) (*nodev1.PutFileResponse, error) {
	if u.done || u.file == nil {
		return nil, status.Error(codes.Internal, "upload is already finished")
	}
	fail := func(err error) (*nodev1.PutFileResponse, error) {
		u.discard()
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return fail(status.FromContextError(err).Err())
	}
	if err := u.file.Chmod(u.perm); err != nil { // OpenFile's mode is filtered by umask
		return fail(status.Errorf(codes.Internal, "chmod: %v", err))
	}
	if err := u.file.Sync(); err != nil {
		return fail(status.Errorf(codes.Internal, "sync: %v", err))
	}
	// Recheck at the commit boundary: a cancel that arrived while chmod or
	// fsync ran must not publish a file the client has given up on.
	if err := ctx.Err(); err != nil {
		return fail(status.FromContextError(err).Err())
	}
	if err := u.file.Close(); err != nil {
		u.file = nil
		u.root.Remove(u.tmp)
		return nil, status.Errorf(codes.Internal, "close: %v", err)
	}
	u.file = nil
	if err := u.root.Rename(u.tmp, u.rel); err != nil {
		u.root.Remove(u.tmp)
		u.finish()
		if errors.Is(err, fs.ErrExist) {
			return nil, status.Errorf(codes.AlreadyExists, "put %s: exists", u.canonical)
		}
		return nil, fileErr("put", u.canonical, err)
	}
	// Make the rename itself durable; the file contents already are. This
	// includes root-level files, whose parent is the confinement root
	// itself ("."), and a failure here is reported: the file is visible but
	// not durably committed.
	d, err := u.root.Open(path.Dir(u.rel))
	if err != nil {
		u.finish()
		return nil, status.Errorf(codes.Internal, "open parent directory: %v", err)
	}
	syncErr := u.fs.syncDir(d)
	closeErr := d.Close()
	u.finish()
	if syncErr != nil {
		return nil, status.Errorf(codes.Internal, "sync parent directory: %v", syncErr)
	}
	if closeErr != nil {
		return nil, status.Errorf(codes.Internal, "close parent directory: %v", closeErr)
	}
	u.fs.log.Info("file uploaded", "path", u.canonical, "size", u.size)
	return &nodev1.PutFileResponse{Path: u.canonical, Size: u.size}, nil
}

func (u *fileUploadImpl) abort(context.Context) error {
	if u.done {
		return nil
	}
	u.discard()
	return nil
}

// discard closes and removes the staged file. It is idempotent.
func (u *fileUploadImpl) discard() {
	if u.file != nil {
		u.file.Close()
		u.file = nil
	}
	if u.tmp != "" {
		if err := u.root.Remove(u.tmp); err != nil && !errors.Is(err, fs.ErrNotExist) {
			u.fs.log.Warn("remove staged upload", "path", u.tmp, "err", err)
		}
	}
	u.finish()
}

// finish releases the borrowed confinement root. The staged file is already
// gone by then.
func (u *fileUploadImpl) finish() {
	if u.done {
		return
	}
	u.done = true
	if u.root != nil {
		u.root.Close()
		u.root = nil
	}
}

// mkdirParents creates the missing parent directories of rel (a clean
// relative path) under u.root. They belong to the uploading user, which is
// the process's own user here, so no chown is needed; the kernel decides
// whether each mkdir is allowed.
func (u *fileUploadImpl) mkdirParents() error {
	dir := path.Dir(u.rel)
	if dir == "." {
		return nil
	}
	cur := "."
	for _, part := range strings.Split(dir, "/") {
		cur = path.Join(cur, part)
		if _, err := u.root.Stat(cur); err == nil {
			continue
		} else if !errors.Is(err, fs.ErrNotExist) {
			return fileErr("put", u.canonical, err)
		}
		if err := u.root.Mkdir(cur, 0o755); err != nil {
			if errors.Is(err, fs.ErrExist) {
				continue // created by a racing upload
			}
			return fileErr("put", u.canonical, err)
		}
		if err := u.root.Chmod(cur, 0o755); err != nil { // Mkdir's mode is filtered by umask
			return fileErr("put", u.canonical, err)
		}
	}
	return nil
}

// randSuffix names the temporary sibling uniquely.
func randSuffix() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand never fails on Linux; if it somehow does, the
		// O_EXCL create still refuses collisions.
		return "0"
	}
	return fmt.Sprintf("%x", b[:])
}
