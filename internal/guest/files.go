package guest

import (
	"context"

	"google.golang.org/grpc"

	"github.com/nathabonfim59/bamboo-raft/internal/proto/guestv1"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// fileOps implements the guest file operations as the sandbox user. It is
// either the in-process [fileServer] (the agent already runs as that user)
// or the credential-dropped helper's client. Nothing here runs filesystem
// operations as root.
type fileOps interface {
	readFile(ctx context.Context, p string, maxBytes int64, send func(*nodev1.ReadFileResponse) error) error
	statFile(ctx context.Context, p string) (*nodev1.FileInfo, error)
	listFiles(ctx context.Context, p string, limit uint32) (*nodev1.ListFilesResponse, error)
	deleteFile(ctx context.Context, p string, recursive bool) (*nodev1.DeleteFileResponse, error)
	makeDir(ctx context.Context, p string, mode uint32) (*nodev1.FileInfo, error)
	moveFile(ctx context.Context, source, destination string, overwrite bool) (*nodev1.MoveFileResponse, error)
	chmodFile(ctx context.Context, p string, mode uint32) (*nodev1.FileInfo, error)
	newUpload(ctx context.Context, p string, mode uint32) (fileUpload, error)
}

// ReadFile streams one regular file as the sandbox user: one info frame,
// then data chunks of at most 1 MiB. A file over max_bytes is refused before
// the info frame.
func (s *Service) ReadFile(req *guestv1.ReadFileRequest, stream grpc.ServerStreamingServer[nodev1.ReadFileResponse]) error {
	return s.files.readFile(stream.Context(), req.GetPath(), req.GetMaxBytes(), stream.Send)
}

// StatFile describes one path.
func (s *Service) StatFile(ctx context.Context, req *guestv1.StatFileRequest) (*nodev1.FileInfo, error) {
	return s.files.statFile(ctx, req.GetPath())
}

// ListFiles lists a directory's direct entries, sorted by name and limited.
func (s *Service) ListFiles(ctx context.Context, req *guestv1.ListFilesRequest) (*nodev1.ListFilesResponse, error) {
	return s.files.listFiles(ctx, req.GetPath(), req.GetLimit())
}

// DeleteFile removes a file, a symlink (not its target) or a directory.
func (s *Service) DeleteFile(ctx context.Context, req *guestv1.DeleteFileRequest) (*nodev1.DeleteFileResponse, error) {
	return s.files.deleteFile(ctx, req.GetPath(), req.GetRecursive())
}

// MakeDir creates a directory and its missing parents.
func (s *Service) MakeDir(ctx context.Context, req *guestv1.MakeDirRequest) (*nodev1.FileInfo, error) {
	return s.files.makeDir(ctx, req.GetPath(), req.GetMode())
}

// MoveFile renames source to destination.
func (s *Service) MoveFile(ctx context.Context, req *guestv1.MoveFileRequest) (*nodev1.MoveFileResponse, error) {
	return s.files.moveFile(ctx, req.GetSource(), req.GetDestination(), req.GetOverwrite())
}

// ChmodFile sets permission bits on the resolved target.
func (s *Service) ChmodFile(ctx context.Context, req *guestv1.ChmodFileRequest) (*nodev1.FileInfo, error) {
	return s.files.chmodFile(ctx, req.GetPath(), req.GetMode())
}
