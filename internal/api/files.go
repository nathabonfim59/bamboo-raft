package api

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"path"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// Route patterns of the pinned file operations. They carry no HTTP method;
// [RegisterFiles] pairs each with its method. The paths match api/bbraft-v1.yaml
// exactly.
const (
	// FilesPath is readFile (GET), writeFile (PUT) and deleteFile (DELETE).
	FilesPath = "/api/v1/sandboxes/{sandboxId}/files"
	// FilesListPath is listFiles (GET).
	FilesListPath = "/api/v1/sandboxes/{sandboxId}/files/list"
	// FilesInfoPath is getFileInfo (GET).
	FilesInfoPath = "/api/v1/sandboxes/{sandboxId}/files/info"
	// FilesFolderPath is createFolder (POST).
	FilesFolderPath = "/api/v1/sandboxes/{sandboxId}/files/folder"
	// FilesMovePath is moveFile (POST).
	FilesMovePath = "/api/v1/sandboxes/{sandboxId}/files/move"
	// FilesPermissionsPath is setFilePermissions (POST).
	FilesPermissionsPath = "/api/v1/sandboxes/{sandboxId}/files/permissions"
)

// Success envelope discriminators of the file operations. They are pinned;
// extensions must never reuse them.
const (
	TypeFileRead           = "file.read"
	TypeFileWritten        = "file.written"
	TypeFileDeleted        = "file.deleted"
	TypeFileList           = "file.list"
	TypeFileInfo           = "file.info"
	TypeFolderCreated      = "folder.created"
	TypeFileMoved          = "file.moved"
	TypeFilePermissionsSet = "file.permissions_set"
)

// FileInlineLimitBytes is the pinned inline file limit: reads stop at 5 MiB
// and writes accept at most 5 MiB after decoding. Larger transfers use the
// signed URLs instead.
const FileInlineLimitBytes int64 = 5 << 20

// FileListEntries is the ListFiles limit the API sends to the node. The pin
// reports truncation when a directory holds more than 10,000 entries.
const FileListEntries uint32 = 10000

// FileChunkBytes bounds one nodev1 data message in either direction. The
// frozen contract allows at most 1 MiB per message.
const FileChunkBytes = 1 << 20

// File readiness defaults. The pin says a file call on a starting sandbox
// waits until it is ready, up to 60 seconds, like a command.
const (
	// DefaultFileReadyDeadline bounds the wait for a starting sandbox.
	DefaultFileReadyDeadline = 60 * time.Second
	// DefaultFileReadyPoll is the interval between durable target lookups
	// while a sandbox is starting.
	DefaultFileReadyPoll = time.Second
)

const (
	// maxFileWriteBodyBytes bounds the raw JSON body of writeFile. The
	// decoded file is at most FileInlineLimitBytes, but JSON escaping can
	// expand a valid text payload up to six times (every byte as \u00XX);
	// the extra mebibyte covers paths, keys and whitespace.
	maxFileWriteBodyBytes = 6*FileInlineLimitBytes + (1 << 20)
	// maxFileJSONBodyBytes bounds the small JSON bodies that carry only
	// paths and modes.
	maxFileJSONBodyBytes = MaxJSONBodyBytes
	// maxFilePathBytes bounds one canonical guest path.
	maxFilePathBytes = 4096
)

// FileEncoding is the pinned file content encoding.
type FileEncoding string

// The pinned file encodings.
const (
	FileEncodingUTF8   FileEncoding = "utf8"
	FileEncodingBase64 FileEncoding = "base64"
)

// valid reports whether e is one of the pinned encodings.
func (e FileEncoding) valid() bool {
	return e == FileEncodingUTF8 || e == FileEncodingBase64
}

// FileReadResponse is the readFile success body. The envelope adds ok and
// file.read.
type FileReadResponse struct {
	SuccessBase
	Success  bool         `json:"success"`
	Path     string       `json:"path"`
	Encoding FileEncoding `json:"encoding"`
	Size     int64        `json:"size"`
	Content  string       `json:"content"`
}

// NewFileReadResponse builds a readFile success body.
func NewFileReadResponse(path string, encoding FileEncoding, size int64, content string) FileReadResponse {
	return FileReadResponse{
		SuccessBase: success(TypeFileRead),
		Success:     true,
		Path:        path,
		Encoding:    encoding,
		Size:        size,
		Content:     content,
	}
}

// FileWriteRequest is the writeFile request body.
type FileWriteRequest struct {
	Path     string       `json:"path"`
	Content  string       `json:"content"`
	Encoding FileEncoding `json:"encoding,omitzero"`
}

// FileWriteResponse is the writeFile success body. The envelope adds ok and
// file.written.
type FileWriteResponse struct {
	SuccessBase
	Success  bool         `json:"success"`
	Path     string       `json:"path"`
	Encoding FileEncoding `json:"encoding"`
	Size     int64        `json:"size"`
}

// NewFileWriteResponse builds a writeFile success body.
func NewFileWriteResponse(path string, encoding FileEncoding, size int64) FileWriteResponse {
	return FileWriteResponse{
		SuccessBase: success(TypeFileWritten),
		Success:     true,
		Path:        path,
		Encoding:    encoding,
		Size:        size,
	}
}

// FileEntry is one file or directory as the pinned API describes it. Mode is
// four octal permission digits, for example "0644".
type FileEntry struct {
	Name       string    `json:"name"`
	Path       string    `json:"path"`
	IsDir      bool      `json:"isDir"`
	IsSymlink  bool      `json:"isSymlink"`
	Size       int64     `json:"size"`
	Mode       string    `json:"mode"`
	ModifiedAt time.Time `json:"modifiedAt"`
}

// ErrFileInfoInvalid reports node file metadata that violates the frozen
// contract. It carries a fixed reason and never native text.
var ErrFileInfoInvalid = errors.New("api: node file metadata is invalid")

// FileEntryFromInfo validates one nodev1 FileInfo and converts it to the
// pinned wire shape. The node must report a known type, a non-negative size,
// permission bits up to 0o7777, a canonical confined path and that path's last
// element as the name. The modification time may be zero or negative; it is
// formatted as time.UnixMilli in UTC without inventing a positive timestamp.
//
// It is exported for reuse by the signed-download receiver; that package does
// not use it yet.
func FileEntryFromInfo(info *nodev1.FileInfo) (FileEntry, error) {
	if info == nil {
		return FileEntry{}, ErrFileInfoInvalid
	}
	switch info.GetType() {
	case nodev1.FileType_FILE_TYPE_REGULAR,
		nodev1.FileType_FILE_TYPE_DIRECTORY,
		nodev1.FileType_FILE_TYPE_OTHER:
	default:
		return FileEntry{}, ErrFileInfoInvalid
	}
	if info.GetSize() < 0 || info.GetMode() > 0o7777 {
		return FileEntry{}, ErrFileInfoInvalid
	}
	canonical, err := fileCanonicalPath(info.GetPath())
	if err != nil || canonical != info.GetPath() {
		return FileEntry{}, ErrFileInfoInvalid
	}
	if info.GetName() == "" || info.GetName() != path.Base(info.GetPath()) {
		return FileEntry{}, ErrFileInfoInvalid
	}
	return NewFileEntry(info), nil
}

// NewFileEntry converts a nodev1 FileInfo to the pinned wire shape. Callers
// that handle node responses should use [FileEntryFromInfo], which validates
// the metadata first. The node reports the entry itself (lstat) and, for an
// in-root symlink, its target's type, size and mode; both are carried through
// unchanged.
func NewFileEntry(info *nodev1.FileInfo) FileEntry {
	if info == nil {
		return FileEntry{Mode: fileModeString(0)}
	}
	return FileEntry{
		Name:       info.GetName(),
		Path:       info.GetPath(),
		IsDir:      info.GetType() == nodev1.FileType_FILE_TYPE_DIRECTORY,
		IsSymlink:  info.GetIsSymlink(),
		Size:       info.GetSize(),
		Mode:       fileModeString(info.GetMode()),
		ModifiedAt: time.UnixMilli(info.GetModifiedAtUnixMs()).UTC(),
	}
}

// fileModeString formats permission bits as four octal digits.
func fileModeString(mode uint32) string {
	return fmt.Sprintf("%04o", mode&0o7777)
}

// FileListResponse is the listFiles success body. The envelope adds ok and
// file.list.
type FileListResponse struct {
	SuccessBase
	Success   bool        `json:"success"`
	Path      string      `json:"path"`
	Files     []FileEntry `json:"files"`
	Truncated bool        `json:"truncated"`
}

// NewFileListResponse builds a listFiles success body. A nil slice becomes an
// empty array because the pinned schema requires an array, never null.
func NewFileListResponse(path string, files []FileEntry, truncated bool) FileListResponse {
	if files == nil {
		files = []FileEntry{}
	}
	return FileListResponse{
		SuccessBase: success(TypeFileList),
		Success:     true,
		Path:        path,
		Files:       files,
		Truncated:   truncated,
	}
}

// FileInfoResponse is the success body of getFileInfo, createFolder and
// setFilePermissions. The envelope adds ok and the operation's discriminator.
type FileInfoResponse struct {
	SuccessBase
	FileEntry
	Success bool `json:"success"`
}

// NewFileInfoResponse builds one of the three FileInfoResponse operations.
// typ must be one of TypeFileInfo, TypeFolderCreated or
// TypeFilePermissionsSet.
func NewFileInfoResponse(typ string, entry FileEntry) FileInfoResponse {
	return FileInfoResponse{
		SuccessBase: success(typ),
		FileEntry:   entry,
		Success:     true,
	}
}

// FileDeleteResponse is the deleteFile success body. The envelope adds ok and
// file.deleted.
type FileDeleteResponse struct {
	SuccessBase
	Success bool   `json:"success"`
	Path    string `json:"path"`
}

// NewFileDeleteResponse builds a deleteFile success body.
func NewFileDeleteResponse(path string) FileDeleteResponse {
	return FileDeleteResponse{
		SuccessBase: success(TypeFileDeleted),
		Success:     true,
		Path:        path,
	}
}

// FileFolderRequest is the createFolder request body. Mode is optional octal
// permission bits; empty means the node's 0755 default.
type FileFolderRequest struct {
	Path string `json:"path"`
	Mode string `json:"mode,omitzero"`
}

// FileMoveRequest is the moveFile request body.
type FileMoveRequest struct {
	Source      string `json:"source"`
	Destination string `json:"destination"`
	Overwrite   bool   `json:"overwrite,omitzero"`
}

// FileMoveResponse is the moveFile success body. The envelope adds ok and
// file.moved.
type FileMoveResponse struct {
	SuccessBase
	Success     bool   `json:"success"`
	Path        string `json:"path"`
	Destination string `json:"destination"`
}

// NewFileMoveResponse builds a moveFile success body.
func NewFileMoveResponse(source, destination string) FileMoveResponse {
	return FileMoveResponse{
		SuccessBase: success(TypeFileMoved),
		Success:     true,
		Path:        source,
		Destination: destination,
	}
}

// FilePermissionsRequest is the setFilePermissions request body. Mode is
// required octal permission bits, for example "0644".
type FilePermissionsRequest struct {
	Path string `json:"path"`
	Mode string `json:"mode"`
}

// FilesOptions are the dependencies and bounded readiness knobs of the file
// handlers. The production file, listing and chunk limits are fixed by the
// pinned contract and are not configurable.
type FilesOptions struct {
	// Lookup resolves the owner-scoped sandbox target. Required. It is the
	// same accepted interface the command and upload handlers consume.
	Lookup CommandTargetLookup
	// Clients returns a client for the target's node. Required. It is the
	// only seam into the node gateway.
	Clients NodeClientFactory
	// Logger receives operational failures. Nil discards them.
	Logger *slog.Logger
	// ReadyDeadline bounds how long a file call waits for a starting
	// sandbox. Zero or negative uses DefaultFileReadyDeadline.
	ReadyDeadline time.Duration
	// ReadyPoll is the interval between durable target lookups while a
	// sandbox is starting. Zero or negative uses DefaultFileReadyPoll.
	ReadyPoll time.Duration
}

// FilesHandlers are the eight pinned file routes. Nothing here registers
// them: server wiring chooses the mux and the middleware, exactly as it does
// for the core Handlers struct.
type FilesHandlers struct {
	// ReadFile serves GET FilesPath.
	ReadFile http.HandlerFunc
	// WriteFile serves PUT FilesPath.
	WriteFile http.HandlerFunc
	// DeleteFile serves DELETE FilesPath.
	DeleteFile http.HandlerFunc
	// ListFiles serves GET FilesListPath.
	ListFiles http.HandlerFunc
	// GetFileInfo serves GET FilesInfoPath.
	GetFileInfo http.HandlerFunc
	// CreateFolder serves POST FilesFolderPath.
	CreateFolder http.HandlerFunc
	// MoveFile serves POST FilesMovePath.
	MoveFile http.HandlerFunc
	// SetFilePermissions serves POST FilesPermissionsPath.
	SetFilePermissions http.HandlerFunc
}

// filesService is the shared state behind the eight handlers. It holds no
// per-request or process-global state.
type filesService struct {
	lookup        CommandTargetLookup
	clients       NodeClientFactory
	logger        *slog.Logger
	readyDeadline time.Duration
	readyPoll     time.Duration
}

// NewFilesHandlers validates opts and returns the eight file handlers. The
// caller registers them with [RegisterFiles] or by pairing each handler with
// its pinned method and path.
func NewFilesHandlers(opts FilesOptions) (FilesHandlers, error) {
	if opts.Lookup == nil {
		return FilesHandlers{}, errors.New("api: files handler needs a target lookup")
	}
	if opts.Clients == nil {
		return FilesHandlers{}, errors.New("api: files handler needs a node-client factory")
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	deadline := opts.ReadyDeadline
	if deadline <= 0 {
		deadline = DefaultFileReadyDeadline
	}
	poll := opts.ReadyPoll
	if poll <= 0 {
		poll = DefaultFileReadyPoll
	}
	service := &filesService{
		lookup:        opts.Lookup,
		clients:       opts.Clients,
		logger:        logger,
		readyDeadline: deadline,
		readyPoll:     poll,
	}
	return FilesHandlers{
		ReadFile:           service.readFile,
		WriteFile:          service.writeFile,
		DeleteFile:         service.deleteFile,
		ListFiles:          service.listFiles,
		GetFileInfo:        service.getFileInfo,
		CreateFolder:       service.createFolder,
		MoveFile:           service.moveFile,
		SetFilePermissions: service.setFilePermissions,
	}, nil
}

// RegisterFiles adds the pinned file routes to mux. The mux must be the
// authenticated /api/v1 API mux, not a public or unauthenticated mux: the
// handlers rely on the server's identity middleware. A nil handler leaves its
// route unregistered, so a partial server can start while a feature is
// disabled. The patterns carry the /api/v1 prefix.
func RegisterFiles(mux *http.ServeMux, h FilesHandlers) {
	handle := func(pattern string, handler http.HandlerFunc) {
		if handler != nil {
			mux.HandleFunc(pattern, handler)
		}
	}
	handle("GET "+FilesPath, h.ReadFile)
	handle("PUT "+FilesPath, h.WriteFile)
	handle("DELETE "+FilesPath, h.DeleteFile)
	handle("GET "+FilesListPath, h.ListFiles)
	handle("GET "+FilesInfoPath, h.GetFileInfo)
	handle("POST "+FilesFolderPath, h.CreateFolder)
	handle("POST "+FilesMovePath, h.MoveFile)
	handle("POST "+FilesPermissionsPath, h.SetFilePermissions)
}
