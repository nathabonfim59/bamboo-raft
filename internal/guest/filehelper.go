package guest

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// The file helper is a one-shot child process per file RPC. The parent
// re-executes this binary with these environment variables and two inherited
// directory descriptors (ExtraFiles 3 and 4) that pin the service's home and
// tmp roots; exec drops to the sandbox uid/gid before the child runs, so
// every filesystem syscall — including the kernel's mode, ACL and sticky
// checks — is performed as the sandbox user. The child reopens the inherited
// inodes as that user and closes the descriptors, so a configured path
// renamed or replaced after New cannot move a file RPC to another directory.
// A root the sandbox user cannot open (for example a search-only 0111
// directory) is reported as a typed INVALID_ARGUMENT naming the logical root;
// it never becomes an INTERNAL error, and Exec is unaffected. The child
// protocol is framed; a malformed frame is always an INTERNAL protocol error,
// never mistaken for an operation error.
const (
	fileHelperEnv       = "BBRAFT_GUEST_FILE_HELPER"
	fileHelperEnvHomeFD = "BBRAFT_GUEST_FILE_HELPER_HOME_FD"
	fileHelperEnvTmpFD  = "BBRAFT_GUEST_FILE_HELPER_TMP_FD"
	fileHelperEnvUID    = "BBRAFT_GUEST_FILE_HELPER_UID"
	fileHelperEnvGID    = "BBRAFT_GUEST_FILE_HELPER_GID"

	// maxHelperFrame bounds every frame before allocation: a 1 MiB data
	// chunk plus framing, with headroom for JSON results.
	maxHelperFrame = 2 << 20
	// helperStderrLimit caps retained child diagnostics as the child writes.
	helperStderrLimit = 2 << 10
	// helperWaitTimeout bounds reaping a helper that already reported.
	helperWaitTimeout = 10 * time.Second
	// helperTrailerTimeout bounds waiting for the child to close its stdout
	// after a terminal frame, so trailing frames are detected.
	helperTrailerTimeout = 2 * time.Second
	// helperReadyTimeout bounds a PutFile header handshake: the child must
	// accept (or reject) the target before the parent sends any data.
	helperReadyTimeout = 5 * time.Second
	// helperReadyAbortTimeout bounds reaping a child while a canceled upload
	// waits for readiness; a stuck child is killed after it.
	helperReadyAbortTimeout = 2 * time.Second

	// listBatchBytes bounds one internal ListFiles batch frame, well under
	// maxHelperFrame; the parent rejects larger batches.
	listBatchBytes = 256 << 10
	// maxListBatchEntries bounds one internal ListFiles batch frame.
	maxListBatchEntries = 512
	// maxPathLen and maxNameLen are the Linux filesystem limits for a
	// decoded helper path and directory-entry name. They keep a malformed
	// child from growing parent memory with oversized fields.
	maxPathLen = 4096
	maxNameLen = 255
)

// Frame kinds. Requests travel parent → child; the rest travel child →
// parent.
const (
	frameRequest    byte = 1
	frameData       byte = 2
	frameCommit     byte = 3
	frameAbort      byte = 4
	frameInfo       byte = 5
	frameResult     byte = 6
	frameStreamData byte = 7
	frameEnd        byte = 8
	frameError      byte = 9
	frameReady      byte = 10
	frameListHeader byte = 11
	frameListBatch  byte = 12
)

// Helper operation names.
const (
	helperOpRead    = "read"
	helperOpStat    = "stat"
	helperOpList    = "list"
	helperOpDelete  = "delete"
	helperOpMkdir   = "mkdir"
	helperOpMove    = "move"
	helperOpChmod   = "chmod"
	helperOpPutFile = "putfile"
)

// errHelperProtocol marks a malformed helper exchange. Callers report it
// distinctly from the child's operation errors.
var errHelperProtocol = errors.New("file helper protocol")

// helperFiles is the [fileOps] implementation that runs every operation in a
// fresh credential-dropped child process.
type helperFiles struct {
	exe        string
	homeRoot   *os.Root // the service's pinned roots, borrowed per spawn
	tmpRoot    *os.Root
	uid, gid   int
	credential func() (*syscall.Credential, error)
	extraEnv   []string // test-only fixture selectors, appended to the child env
}

// newHelperFiles prepares the helper launcher. The executable is captured
// now: the child is this same binary in helper mode. It holds the service's
// pinned confinement roots and borrows them per spawn, so renaming or
// replacing the configured home/tmp paths after New never moves file RPCs to
// another inode. credential yields the sandbox identity from the shared
// builder (A5); errors are reported, never silently replaced with the agent's
// identity.
func newHelperFiles(homeRoot, tmpRoot *os.Root, uid, gid int, credential func() (*syscall.Credential, error)) (*helperFiles, error) {
	exe, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("guest: locate file helper executable: %w", err)
	}
	return &helperFiles{exe: exe, homeRoot: homeRoot, tmpRoot: tmpRoot, uid: uid, gid: gid, credential: credential}, nil
}

// helperRequest is the JSON request frame.
type helperRequest struct {
	Op          string `json:"op"`
	Path        string `json:"path,omitempty"`
	Source      string `json:"source,omitempty"`
	Destination string `json:"destination,omitempty"`
	MaxBytes    int64  `json:"maxBytes,omitempty"`
	Limit       uint32 `json:"limit,omitempty"`
	Mode        uint32 `json:"mode,omitempty"`
	Recursive   bool   `json:"recursive,omitempty"`
	Overwrite   bool   `json:"overwrite,omitempty"`
}

// helperError is the child's operation error frame.
type helperError struct {
	Code    int32  `json:"code"`
	Message string `json:"message"`
}

// fileInfoDTO mirrors nodev1.FileInfo over JSON; proto messages are not
// JSON-encoded directly.
type fileInfoDTO struct {
	Path             string `json:"path"`
	Name             string `json:"name"`
	Type             int32  `json:"type"`
	IsSymlink        bool   `json:"isSymlink,omitempty"`
	Size             int64  `json:"size,omitempty"`
	Mode             uint32 `json:"mode,omitempty"`
	ModifiedAtUnixMs int64  `json:"modifiedAtUnixMs,omitempty"`
}

// listHeaderDTO starts a streamed ListFiles result: the canonical directory,
// whether more entries were omitted, and the exact number of entries that
// follow in batches.
type listHeaderDTO struct {
	Path      string `json:"path"`
	Truncated bool   `json:"truncated,omitempty"`
	Count     uint32 `json:"count"`
}

// listEndDTO terminates a streamed ListFiles result and repeats the count for
// an independent consistency check.
type listEndDTO struct {
	Count uint32 `json:"count"`
}

type deleteFileDTO struct {
	Path string `json:"path"`
}

type moveFileDTO struct {
	Source      string `json:"source"`
	Destination string `json:"destination"`
}

type putFileDTO struct {
	Path string `json:"path"`
	Size int64  `json:"size"`
}

// validateFileInfoDTO bounds every decoded field and checks the invariants
// the child must uphold, so a malformed helper cannot inflate parent memory or
// smuggle an invalid enum.
func validateFileInfoDTO(d fileInfoDTO) error {
	if d.Type < int32(nodev1.FileType_FILE_TYPE_UNSPECIFIED) || d.Type > int32(nodev1.FileType_FILE_TYPE_OTHER) {
		return fmt.Errorf("%w: invalid file type %d", errHelperProtocol, d.Type)
	}
	if d.Mode > 0o7777 {
		return fmt.Errorf("%w: mode %#o is above 07777", errHelperProtocol, d.Mode)
	}
	if d.Path == "" || len(d.Path) > maxPathLen || !path.IsAbs(d.Path) || path.Clean(d.Path) != d.Path {
		return fmt.Errorf("%w: invalid file path", errHelperProtocol)
	}
	if d.Name == "" || len(d.Name) > maxNameLen || d.Name == "." || d.Name == ".." || strings.Contains(d.Name, "/") {
		return fmt.Errorf("%w: invalid file name", errHelperProtocol)
	}
	if d.Name != path.Base(d.Path) {
		return fmt.Errorf("%w: file name does not match its path", errHelperProtocol)
	}
	return nil
}

func fileInfoToDTO(fi *nodev1.FileInfo) fileInfoDTO {
	return fileInfoDTO{
		Path:             fi.GetPath(),
		Name:             fi.GetName(),
		Type:             int32(fi.GetType()),
		IsSymlink:        fi.GetIsSymlink(),
		Size:             fi.GetSize(),
		Mode:             fi.GetMode(),
		ModifiedAtUnixMs: fi.GetModifiedAtUnixMs(),
	}
}

func fileInfoFromDTO(d fileInfoDTO) (*nodev1.FileInfo, error) {
	if err := validateFileInfoDTO(d); err != nil {
		return nil, err
	}
	return &nodev1.FileInfo{
		Path:             d.Path,
		Name:             d.Name,
		Type:             nodev1.FileType(d.Type),
		IsSymlink:        d.IsSymlink,
		Size:             d.Size,
		Mode:             d.Mode,
		ModifiedAtUnixMs: d.ModifiedAtUnixMs,
	}, nil
}

// --- Parent side -----------------------------------------------------------

func (h *helperFiles) statFile(ctx context.Context, p string) (*nodev1.FileInfo, error) {
	var out fileInfoDTO
	if err := h.simple(ctx, helperRequest{Op: helperOpStat, Path: p}, &out); err != nil {
		return nil, err
	}
	return fileInfoResult(out)
}

// listFiles streams a ListFiles result as a header, bounded batches and a
// final count, then returns the single unary response the RPC contract
// requires. The effective limit is validated before the child starts, and
// every frame's count, size, order and path relation is checked so a
// malformed helper cannot grow memory through maximal frames.
// fileInfoResult validates a FileInfo that arrived after the child already
// reported success. The child is reaped; an invalid result is a helper
// protocol failure (INTERNAL), never a raw error that maps to UNKNOWN.
func fileInfoResult(d fileInfoDTO) (*nodev1.FileInfo, error) {
	fi, err := fileInfoFromDTO(d)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return fi, nil
}

func (h *helperFiles) listFiles(ctx context.Context, p string, limit uint32) (*nodev1.ListFilesResponse, error) {
	effective := int(limit)
	if effective == 0 {
		effective = defaultListLimit
	}
	if effective > maxListLimit {
		return nil, status.Errorf(codes.InvalidArgument, "limit %d is above %d", effective, maxListLimit)
	}
	conn, err := h.start(ctx, false)
	if err != nil {
		return nil, err
	}
	if err := conn.writeRequest(helperRequest{Op: helperOpList, Path: p, Limit: limit}); err != nil {
		return nil, conn.fail(ctx, err)
	}
	kind, payload, err := conn.read()
	if err != nil {
		return nil, conn.fail(ctx, err)
	}
	if kind == frameError {
		return nil, conn.finishOpError(payload)
	}
	if kind != frameListHeader {
		return nil, conn.protocolf("unexpected frame %d at the start of a list result", kind)
	}
	var hdr listHeaderDTO
	if err := json.Unmarshal(payload, &hdr, json.RejectUnknownMembers(true)); err != nil {
		return nil, conn.protocolf("list header: %v", err)
	}
	if hdr.Path == "" || len(hdr.Path) > maxPathLen || !path.IsAbs(hdr.Path) || path.Clean(hdr.Path) != hdr.Path {
		return nil, conn.protocolf("list header has an invalid directory path")
	}
	if int(hdr.Count) > effective {
		return nil, conn.protocolf("list header count %d exceeds the %d entry limit", hdr.Count, effective)
	}
	if hdr.Truncated && int(hdr.Count) != effective {
		return nil, conn.protocolf("truncated list has %d entries, want the %d entry limit", hdr.Count, effective)
	}
	resp := &nodev1.ListFilesResponse{Path: hdr.Path, Truncated: hdr.Truncated}
	total := 0
	lastName := ""
	for {
		kind, payload, err := conn.read()
		if err != nil {
			return nil, conn.fail(ctx, err)
		}
		switch kind {
		case frameListBatch:
			batch, err := decodeListBatch(payload)
			if err != nil {
				return nil, conn.abortProtocolError(err)
			}
			if total+len(batch) > int(hdr.Count) {
				return nil, conn.protocolf("list batches exceed the declared count %d", hdr.Count)
			}
			for _, d := range batch {
				fi, err := fileInfoFromDTO(d)
				if err != nil {
					return nil, conn.protocolf("list entry: %v", err)
				}
				if fi.GetPath() != path.Join(hdr.Path, fi.GetName()) {
					return nil, conn.protocolf("list entry %q is not under %q", fi.GetPath(), hdr.Path)
				}
				if lastName != "" && fi.GetName() <= lastName {
					return nil, conn.protocolf("list entries are not strictly ordered at %q", fi.GetName())
				}
				lastName = fi.GetName()
				resp.Entries = append(resp.Entries, fi)
			}
			total += len(batch)
		case frameEnd:
			var end listEndDTO
			if err := json.Unmarshal(payload, &end, json.RejectUnknownMembers(true)); err != nil {
				return nil, conn.protocolf("list end: %v", err)
			}
			if int(end.Count) != total || total != int(hdr.Count) {
				return nil, conn.protocolf("list count mismatch: header %d, frames %d, end %d", hdr.Count, total, end.Count)
			}
			if err := conn.finishOK(); err != nil {
				return nil, err
			}
			return resp, nil
		case frameError:
			return nil, conn.finishOpError(payload)
		default:
			return nil, conn.protocolf("unexpected frame %d in a list result", kind)
		}
	}
}

// decodeListBatch validates one streamed list batch before decoding it.
func decodeListBatch(payload []byte) ([]fileInfoDTO, error) {
	if len(payload) > listBatchBytes {
		return nil, fmt.Errorf("%w: list batch of %d bytes exceeds %d", errHelperProtocol, len(payload), listBatchBytes)
	}
	var batch []fileInfoDTO
	if err := json.Unmarshal(payload, &batch, json.RejectUnknownMembers(true)); err != nil {
		return nil, fmt.Errorf("%w: list batch: %v", errHelperProtocol, err)
	}
	if len(batch) == 0 || len(batch) > maxListBatchEntries {
		return nil, fmt.Errorf("%w: list batch has %d entries", errHelperProtocol, len(batch))
	}
	return batch, nil
}

func (h *helperFiles) deleteFile(ctx context.Context, p string, recursive bool) (*nodev1.DeleteFileResponse, error) {
	var out deleteFileDTO
	if err := h.simple(ctx, helperRequest{Op: helperOpDelete, Path: p, Recursive: recursive}, &out); err != nil {
		return nil, err
	}
	return &nodev1.DeleteFileResponse{Path: out.Path}, nil
}

func (h *helperFiles) makeDir(ctx context.Context, p string, mode uint32) (*nodev1.FileInfo, error) {
	var out fileInfoDTO
	if err := h.simple(ctx, helperRequest{Op: helperOpMkdir, Path: p, Mode: mode}, &out); err != nil {
		return nil, err
	}
	return fileInfoResult(out)
}

func (h *helperFiles) moveFile(ctx context.Context, source, destination string, overwrite bool) (*nodev1.MoveFileResponse, error) {
	var out moveFileDTO
	if err := h.simple(ctx, helperRequest{Op: helperOpMove, Source: source, Destination: destination, Overwrite: overwrite}, &out); err != nil {
		return nil, err
	}
	return &nodev1.MoveFileResponse{Source: out.Source, Destination: out.Destination}, nil
}

func (h *helperFiles) chmodFile(ctx context.Context, p string, mode uint32) (*nodev1.FileInfo, error) {
	var out fileInfoDTO
	if err := h.simple(ctx, helperRequest{Op: helperOpChmod, Path: p, Mode: mode}, &out); err != nil {
		return nil, err
	}
	return fileInfoResult(out)
}

// simple runs one non-streaming operation and decodes its single result.
func (h *helperFiles) simple(ctx context.Context, req helperRequest, out any) error {
	conn, err := h.start(ctx, false)
	if err != nil {
		return err
	}
	if err := conn.writeRequest(req); err != nil {
		return conn.fail(ctx, err)
	}
	kind, payload, err := conn.read()
	if err != nil {
		return conn.fail(ctx, err)
	}
	switch kind {
	case frameError:
		return conn.finishOpError(payload)
	case frameResult:
		if err := json.Unmarshal(payload, out); err != nil {
			return conn.protocolf("result: %v", err)
		}
		return conn.finishOK()
	default:
		return conn.protocolf("unexpected frame %d", kind)
	}
}

func (h *helperFiles) readFile(ctx context.Context, p string, maxBytes int64, send func(*nodev1.ReadFileResponse) error) error {
	conn, err := h.start(ctx, false)
	if err != nil {
		return err
	}
	if err := conn.writeRequest(helperRequest{Op: helperOpRead, Path: p, MaxBytes: maxBytes}); err != nil {
		return conn.fail(ctx, err)
	}
	sawInfo := false
	for {
		kind, payload, err := conn.read()
		if err != nil {
			return conn.fail(ctx, err)
		}
		switch kind {
		case frameInfo:
			if sawInfo {
				return conn.protocolf("duplicate info frame")
			}
			var dto fileInfoDTO
			if err := json.Unmarshal(payload, &dto); err != nil {
				return conn.protocolf("info: %v", err)
			}
			info, err := fileInfoFromDTO(dto)
			if err != nil {
				return conn.protocolf("info: %v", err)
			}
			sawInfo = true
			if err := send(&nodev1.ReadFileResponse{Part: &nodev1.ReadFileResponse_Info{Info: info}}); err != nil {
				return conn.abortTransport(err)
			}
		case frameStreamData:
			if !sawInfo {
				return conn.protocolf("data frame before info")
			}
			if len(payload) > maxReadChunk {
				return conn.protocolf("data frame of %d bytes exceeds %d", len(payload), maxReadChunk)
			}
			if err := send(&nodev1.ReadFileResponse{Part: &nodev1.ReadFileResponse_Data{Data: payload}}); err != nil {
				return conn.abortTransport(err)
			}
		case frameEnd:
			if !sawInfo {
				return conn.protocolf("end frame before info")
			}
			return conn.finishOK()
		case frameError:
			return conn.finishOpError(payload)
		default:
			return conn.protocolf("unexpected frame %d", kind)
		}
	}
}

func (h *helperFiles) newUpload(ctx context.Context, p string, mode uint32) (fileUpload, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	conn, err := h.start(ctx, true)
	if err != nil {
		return nil, err
	}
	if err := conn.writeRequest(helperRequest{Op: helperOpPutFile, Path: p, Mode: mode}); err != nil {
		conn.abort()
		return nil, conn.fail(ctx, err)
	}
	if err := conn.awaitReady(ctx); err != nil {
		return nil, err
	}
	return &helperUpload{conn: conn}, nil
}

// helperUpload forwards chunks to the child and only then signals the
// explicit commit frame. The child never renames before that frame.
type helperUpload struct {
	conn *helperConn
	done bool
}

func (u *helperUpload) write(b []byte) error {
	if u.done {
		return status.Error(codes.Internal, "upload is already finished")
	}
	if len(b) > maxUploadChunk {
		return status.Errorf(codes.InvalidArgument, "PutFile chunk of %d bytes exceeds %d bytes", len(b), maxUploadChunk)
	}
	if err := u.conn.writeFrame(frameData, b); err != nil {
		u.conn.abort()
		u.done = true
		return status.Errorf(codes.Internal, "file helper write: %v", err)
	}
	return nil
}

func (u *helperUpload) commit(ctx context.Context) (*nodev1.PutFileResponse, error) {
	if u.done {
		return nil, status.Error(codes.Internal, "upload is already finished")
	}
	u.done = true
	if err := u.conn.writeFrame(frameCommit, nil); err != nil {
		u.conn.abort()
		return nil, status.Errorf(codes.Internal, "file helper commit: %v", err)
	}
	kind, payload, err := u.conn.read()
	if err != nil {
		return nil, u.conn.fail(ctx, err)
	}
	switch kind {
	case frameError:
		return nil, u.conn.finishOpError(payload)
	case frameResult:
		var dto putFileDTO
		if err := json.Unmarshal(payload, &dto); err != nil {
			return nil, u.conn.protocolf("put result: %v", err)
		}
		if err := u.conn.finishOK(); err != nil {
			return nil, err
		}
		return &nodev1.PutFileResponse{Path: dto.Path, Size: dto.Size}, nil
	default:
		return nil, u.conn.protocolf("unexpected frame %d", kind)
	}
}

func (u *helperUpload) abort(ctx context.Context) error {
	if u.done {
		return nil
	}
	u.done = true
	return u.conn.abortWait(ctx)
}

// --- Child process ---------------------------------------------------------

// FileHelperRequested reports whether this process was started as the
// credential-dropped file helper. The guest command and the test binary
// check it before doing any normal startup work.
func FileHelperRequested() bool {
	return os.Getenv(fileHelperEnv) == "1"
}

// FileHelperMain runs the helper protocol on stdin/stdout as this process's
// (already dropped) user and returns a process exit code.
func FileHelperMain() int {
	in := bufio.NewReader(os.Stdin)
	out := bufio.NewWriter(os.Stdout)
	// Consume the bounded request before opening roots or reporting any
	// startup failure. The parent writes exactly one request; reading it
	// first keeps that write from racing this process's exit into EPIPE, so
	// a startup failure arrives as a typed error frame instead of INTERNAL.
	req, err := readHelperRequest(in)
	if err != nil {
		return helperExit(err.Error())
	}
	startupError := func(code codes.Code, msg string) int {
		if err := sendHelperError(out, status.Error(code, msg)); err != nil {
			fmt.Fprintln(os.Stderr, "bbraft-guest file helper:", err)
			return 2
		}
		return 0
	}

	uid, uidErr := strconv.Atoi(os.Getenv(fileHelperEnvUID))
	gid, gidErr := strconv.Atoi(os.Getenv(fileHelperEnvGID))
	homeFD, homeErr := strconv.Atoi(os.Getenv(fileHelperEnvHomeFD))
	tmpFD, tmpErr := strconv.Atoi(os.Getenv(fileHelperEnvTmpFD))
	if uidErr != nil || gidErr != nil || homeErr != nil || tmpErr != nil {
		return startupError(codes.Internal, "file helper: invalid startup environment")
	}
	// The parent asked the kernel to drop credentials before exec. Never run
	// a file operation as a more privileged user than requested.
	if os.Geteuid() != uid || os.Getegid() != gid {
		return startupError(codes.Internal, fmt.Sprintf(
			"file helper: effective uid/gid %d:%d do not match the requested %d:%d",
			os.Geteuid(), os.Getegid(), uid, gid))
	}
	// The fds are the parent's pinned roots. Reopen those exact inodes as the
	// dropped user (so the kernel still enforces access), then close the
	// inherited descriptors: from here on only the reopened roots are used.
	fs, err := openFileServerFDs(homeFD, tmpFD)
	_ = syscall.Close(homeFD)
	_ = syscall.Close(tmpFD)
	if err != nil {
		// A root the sandbox user cannot open (for example a search-only
		// 0111 directory) is a clean file-API argument/permission error, not
		// an internal helper failure, and Exec is unaffected. Any other
		// startup failure (bad descriptor, EMFILE, I/O) stays INTERNAL.
		return startupError(helperStartupCode(err), "file helper: "+err.Error())
	}
	if err := serveFileHelper(fs, req, in, out); err != nil {
		fmt.Fprintln(os.Stderr, "bbraft-guest file helper:", err)
		return 2
	}
	return 0
}

func helperExit(msg string) int {
	fmt.Fprintln(os.Stderr, "bbraft-guest file helper:", msg)
	return 2
}

// readHelperRequest consumes the single bounded request frame.
func readHelperRequest(in *bufio.Reader) (helperRequest, error) {
	kind, payload, err := readHelperFrame(in)
	if err != nil {
		return helperRequest{}, fmt.Errorf("read request: %w", err)
	}
	if kind != frameRequest {
		return helperRequest{}, fmt.Errorf("first frame is %d, want a request", kind)
	}
	var req helperRequest
	if err := json.Unmarshal(payload, &req, json.RejectUnknownMembers(true)); err != nil {
		return helperRequest{}, fmt.Errorf("decode request: %w", err)
	}
	return req, nil
}

// serveFileHelper handles one already-decoded request and returns when it is
// done.
func serveFileHelper(fs *fileServer, req helperRequest, in *bufio.Reader, out *bufio.Writer) error {
	ctx := context.Background()
	switch req.Op {
	case helperOpRead:
		err := fs.readFile(ctx, req.Path, req.MaxBytes, func(resp *nodev1.ReadFileResponse) error {
			switch part := resp.GetPart().(type) {
			case *nodev1.ReadFileResponse_Info:
				return writeHelperJSON(out, frameInfo, fileInfoToDTO(part.Info))
			case *nodev1.ReadFileResponse_Data:
				return writeHelperFrame(out, frameStreamData, part.Data)
			default:
				return fmt.Errorf("%w: unknown read frame", errHelperProtocol)
			}
		})
		if err != nil {
			return sendHelperError(out, err)
		}
		return writeHelperFrame(out, frameEnd, nil)
	case helperOpStat:
		info, err := fs.statFile(ctx, req.Path)
		return sendHelperInfo(out, info, err)
	case helperOpList:
		list, err := fs.listFiles(ctx, req.Path, req.Limit)
		if err != nil {
			return sendHelperError(out, err)
		}
		dtos := make([]fileInfoDTO, len(list.GetEntries()))
		for i, e := range list.GetEntries() {
			dtos[i] = fileInfoToDTO(e)
		}
		if err := writeHelperJSON(out, frameListHeader, listHeaderDTO{
			Path:      list.GetPath(),
			Truncated: list.GetTruncated(),
			Count:     uint32(len(dtos)),
		}); err != nil {
			return err
		}
		if err := writeListBatches(out, dtos); err != nil {
			return err
		}
		return writeHelperJSON(out, frameEnd, listEndDTO{Count: uint32(len(dtos))})
	case helperOpDelete:
		del, err := fs.deleteFile(ctx, req.Path, req.Recursive)
		if err != nil {
			return sendHelperError(out, err)
		}
		return writeHelperJSON(out, frameResult, deleteFileDTO{Path: del.GetPath()})
	case helperOpMkdir:
		info, err := fs.makeDir(ctx, req.Path, req.Mode)
		return sendHelperInfo(out, info, err)
	case helperOpMove:
		mv, err := fs.moveFile(ctx, req.Source, req.Destination, req.Overwrite)
		if err != nil {
			return sendHelperError(out, err)
		}
		return writeHelperJSON(out, frameResult, moveFileDTO{Source: mv.GetSource(), Destination: mv.GetDestination()})
	case helperOpChmod:
		info, err := fs.chmodFile(ctx, req.Path, req.Mode)
		return sendHelperInfo(out, info, err)
	case helperOpPutFile:
		return serveHelperPutFile(fs, req, in, out)
	default:
		return fmt.Errorf("%w: unknown op %q", errHelperProtocol, req.Op)
	}
}

func serveHelperPutFile(fs *fileServer, req helperRequest, in *bufio.Reader, out *bufio.Writer) error {
	up, err := fs.newUpload(context.Background(), req.Path, req.Mode)
	if err != nil {
		return sendHelperError(out, err)
	}
	// The target is resolved and the staging file exists: the parent may now
	// stream data. A rejected header never gets this far, so the parent
	// reports the child's status instead of an EPIPE from writing data.
	if err := writeHelperFrame(out, frameReady, nil); err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			up.abort(context.Background())
		}
	}()
	for {
		kind, payload, err := readHelperFrame(in)
		if errors.Is(err, io.EOF) {
			return nil // no commit: the defer discards the staged file
		}
		if err != nil {
			return fmt.Errorf("read upload frame: %w", err)
		}
		switch kind {
		case frameData:
			if err := up.write(payload); err != nil {
				return sendHelperError(out, err)
			}
		case frameCommit:
			resp, err := up.commit(context.Background())
			if err != nil {
				return sendHelperError(out, err)
			}
			committed = true
			return writeHelperJSON(out, frameResult, putFileDTO{Path: resp.GetPath(), Size: resp.GetSize()})
		case frameAbort:
			return nil
		default:
			return fmt.Errorf("%w: unexpected upload frame %d", errHelperProtocol, kind)
		}
	}
}

func sendHelperInfo(out *bufio.Writer, info *nodev1.FileInfo, err error) error {
	if err != nil {
		return sendHelperError(out, err)
	}
	return writeHelperJSON(out, frameResult, fileInfoToDTO(info))
}

func sendHelperError(out *bufio.Writer, err error) error {
	st := status.Convert(err)
	if st.Code() == codes.OK {
		st = status.New(codes.Internal, st.Message())
	}
	return writeHelperJSON(out, frameError, helperError{Code: int32(st.Code()), Message: st.Message()})
}

// writeListBatches emits entries in bounded batches: each frame is at most
// maxListBatchEntries and listBatchBytes of JSON, with allocation bounded
// before every write. A batch is halved until it fits; a single entry is
// always small because validateFileInfoDTO bounds its fields.
func writeListBatches(out *bufio.Writer, entries []fileInfoDTO) error {
	for len(entries) > 0 {
		n := min(len(entries), maxListBatchEntries)
		payload, err := json.Marshal(entries[:n])
		for err == nil && len(payload) > listBatchBytes && n > 1 {
			n /= 2
			payload, err = json.Marshal(entries[:n])
		}
		if err != nil {
			return err
		}
		if len(payload) > maxHelperFrame {
			return fmt.Errorf("%w: list batch of %d bytes exceeds %d", errHelperProtocol, len(payload), maxHelperFrame)
		}
		if err := writeHelperFrame(out, frameListBatch, payload); err != nil {
			return err
		}
		entries = entries[n:]
	}
	return nil
}

func writeHelperJSON(out *bufio.Writer, kind byte, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return writeHelperFrame(out, kind, b)
}

func writeHelperFrame(out *bufio.Writer, kind byte, payload []byte) error {
	if len(payload)+1 > maxHelperFrame {
		return fmt.Errorf("%w: frame of %d bytes is too large", errHelperProtocol, len(payload)+1)
	}
	var hdr [5]byte
	binary.BigEndian.PutUint32(hdr[:4], uint32(len(payload)+1))
	hdr[4] = kind
	if _, err := out.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := out.Write(payload); err != nil {
		return err
	}
	return out.Flush()
}

func readHelperFrame(r io.Reader) (byte, []byte, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return 0, nil, err
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if n < 1 || n > maxHelperFrame {
		return 0, nil, fmt.Errorf("%w: frame length %d is out of range", errHelperProtocol, n)
	}
	var kind [1]byte
	if _, err := io.ReadFull(r, kind[:]); err != nil {
		return 0, nil, err
	}
	payload := make([]byte, int(n)-1)
	if _, err := io.ReadFull(r, payload); err != nil {
		return 0, nil, err
	}
	return kind[0], payload, nil
}

// awaitReady waits for the child's PutFile header decision: a ready frame, or
// the operation error. Waiting is bounded and cancellable; on cancellation the
// child is closed down and reaped before the caller sees Canceled.
func (c *helperConn) awaitReady(ctx context.Context) error {
	file, ok := c.stdout.(*os.File)
	if !ok {
		return c.abortProtocol("helper stdout is not a bounded pipe")
	}
	if err := file.SetReadDeadline(time.Now().Add(helperReadyTimeout)); err != nil {
		return c.abortProtocol("cannot bound the upload readiness check: " + err.Error())
	}
	stop := context.AfterFunc(ctx, func() { file.SetReadDeadline(time.Now()) })
	defer stop()
	kind, payload, err := c.read()
	if err != nil {
		if ctx.Err() != nil {
			c.stopWatcher()
			c.stdin.Close()
			_ = c.reap(helperReadyAbortTimeout)
			return status.FromContextError(ctx.Err()).Err()
		}
		if errors.Is(err, os.ErrDeadlineExceeded) {
			c.stopWatcher()
			c.kill()
			_ = c.reap(helperWaitTimeout)
			return c.protocolError(fmt.Errorf("%w: helper did not become ready", errHelperProtocol))
		}
		return c.fail(ctx, err)
	}
	switch kind {
	case frameReady:
		// Keep later commit reads on their own schedule.
		if err := file.SetReadDeadline(time.Time{}); err != nil {
			return c.abortProtocol("cannot clear the upload readiness deadline: " + err.Error())
		}
		return nil
	case frameError:
		return c.finishOpError(payload)
	default:
		return c.protocolf("unexpected frame %d before upload readiness", kind)
	}
}

// abortProtocolError reaps the child after a malformed result located after
// the terminal frame would have been consumed, then reports the protocol
// error. Unlike protocolError it owns cleanup, so no child can leak.
func (c *helperConn) abortProtocolError(err error) error {
	c.stopWatcher()
	c.kill()
	_ = c.reap(helperWaitTimeout)
	return c.protocolError(err)
}

// abortProtocol reports a malformed exchange after making sure the child is
// killed and reaped.
func (c *helperConn) abortProtocol(msg string) error {
	c.stopWatcher()
	c.kill()
	_ = c.reap(helperWaitTimeout)
	return c.protocolError(fmt.Errorf("%w: %s", errHelperProtocol, msg))
}

// abortTransport reaps a child after the gRPC send side failed and returns
// the send error unchanged: the operation outcome is the client's, not the
// helper's, but the child must not survive the handler.
func (c *helperConn) abortTransport(sendErr error) error {
	c.stopWatcher()
	c.kill()
	_ = c.reap(helperWaitTimeout)
	return sendErr
}

// --- Helper process management --------------------------------------------

// helperConn is one spawned helper and its stdio pipes.
type helperConn struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout io.ReadCloser
	out    *bufio.Reader
	stderr *boundedBuffer

	stop func() bool

	mu      sync.Mutex
	exited  bool
	exitErr error
}

// start launches a helper with a cleared environment. It inherits stdio plus
// exactly two extra descriptors: the service's pinned home and tmp directory
// inodes, so the child can never follow a replaced configured path. The child
// reopens those inodes as the sandbox user and closes the inherited
// descriptors; the parent closes its borrowed copies right after Start.
// Credentials are dropped by exec in the forked child, exactly like exec's own
// commands. graceful makes cancellation close the child's stdin (letting an
// upload discard its staged file) instead of killing the process group.
func (h *helperFiles) start(ctx context.Context, graceful bool) (*helperConn, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	cmd := exec.Command(h.exe)
	cmd.Env = []string{
		fileHelperEnv + "=1",
		fileHelperEnvHomeFD + "=3",
		fileHelperEnvTmpFD + "=4",
		fileHelperEnvUID + "=" + strconv.Itoa(h.uid),
		fileHelperEnvGID + "=" + strconv.Itoa(h.gid),
	}
	cred, err := h.credential()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "file helper credential: %v", err)
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Credential: cred,
		Setpgid:    true,
		Pdeathsig:  syscall.SIGKILL,
	}
	// Borrow the pinned roots from the service and let the child reopen those
	// exact inodes as the sandbox user. O_PATH carries traversal identity
	// only, so even a search-only (0111) root the user may legitimately
	// traverse can be borrowed; an O_RDONLY borrow would fail as that user.
	// The descriptors are ExtraFiles 3 and 4; both parent copies close right
	// after Start.
	homeFD, err := h.homeRoot.OpenFile(".", unix.O_PATH|unix.O_DIRECTORY, 0)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "borrow pinned home root: %v", err)
	}
	tmpFD, err := h.tmpRoot.OpenFile(".", unix.O_PATH|unix.O_DIRECTORY, 0)
	if err != nil {
		homeFD.Close()
		return nil, status.Errorf(codes.Internal, "borrow pinned tmp root: %v", err)
	}
	cmd.ExtraFiles = []*os.File{homeFD, tmpFD}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		homeFD.Close()
		tmpFD.Close()
		return nil, status.Errorf(codes.Internal, "file helper stdin: %v", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		homeFD.Close()
		tmpFD.Close()
		stdin.Close()
		return nil, status.Errorf(codes.Internal, "file helper stdout: %v", err)
	}
	stderr := &boundedBuffer{limit: helperStderrLimit}
	cmd.Stderr = stderr
	cmd.Env = append(cmd.Env, h.extraEnv...)
	if err := cmd.Start(); err != nil {
		homeFD.Close()
		tmpFD.Close()
		stdin.Close()
		return nil, status.Errorf(codes.Internal, "start file helper: %v", err)
	}
	homeFD.Close()
	tmpFD.Close()
	c := &helperConn{
		cmd:    cmd,
		stdin:  stdin,
		stdout: stdout,
		out:    bufio.NewReader(stdout),
		stderr: stderr,
	}
	if graceful {
		c.stop = context.AfterFunc(ctx, func() { stdin.Close() })
	} else {
		c.stop = context.AfterFunc(ctx, c.kill)
	}
	return c, nil
}

func (c *helperConn) writeRequest(req helperRequest) error {
	b, err := json.Marshal(req)
	if err != nil {
		return err
	}
	return c.writeFrame(frameRequest, b)
}

func (c *helperConn) writeFrame(kind byte, payload []byte) error {
	if len(payload)+1 > maxHelperFrame {
		return fmt.Errorf("%w: frame of %d bytes is too large", errHelperProtocol, len(payload)+1)
	}
	var hdr [5]byte
	binary.BigEndian.PutUint32(hdr[:4], uint32(len(payload)+1))
	hdr[4] = kind
	if _, err := c.stdin.Write(hdr[:]); err != nil {
		return err
	}
	if len(payload) > 0 {
		if _, err := c.stdin.Write(payload); err != nil {
			return err
		}
	}
	return nil
}

func (c *helperConn) read() (byte, []byte, error) {
	return readHelperFrame(c.out)
}

// finishOK closes the child's stdin and verifies it reported everything:
// exactly one terminal frame, then EOF, and a successful exit.
func (c *helperConn) finishOK() error {
	c.stopWatcher()
	c.stdin.Close()
	if err := c.expectEOF(); err != nil {
		return err
	}
	if err := c.reap(helperWaitTimeout); err != nil {
		return c.childFailure(fmt.Errorf("file helper exited: %w", err))
	}
	return nil
}

// finishOpError decodes the child's operation error and reaps it. A trailing
// frame or a failed exit is a protocol failure, not the operation error.
func (c *helperConn) finishOpError(payload []byte) error {
	var e helperError
	if err := json.Unmarshal(payload, &e); err != nil {
		return c.protocolf("error frame: %v", err)
	}
	if e.Code <= int32(codes.OK) || e.Code > int32(codes.Unauthenticated) {
		return c.protocolf("error frame has invalid code %d", e.Code)
	}
	c.stopWatcher()
	c.stdin.Close()
	if err := c.expectEOF(); err != nil {
		return err
	}
	if err := c.reap(helperWaitTimeout); err != nil {
		// A child that signals an operation error and then crashes is not a
		// trustworthy result: report the helper failure, not the error it
		// happened to emit.
		return c.childFailure(fmt.Errorf("file helper failed after reporting an operation error: %w", err))
	}
	return status.Error(codes.Code(e.Code), e.Message)
}

// expectEOF reads one more frame: a well-behaved child closes its stdout
// right after the terminal frame. Any extra frame is a protocol error.
func (c *helperConn) expectEOF() error {
	if err := validateHelperEOF(c.stdout, c.out); err != nil {
		c.stopWatcher()
		c.kill()
		_ = c.reap(helperWaitTimeout)
		return c.protocolError(err)
	}
	return nil
}

// validateHelperEOF requires the child to close its output after the terminal
// frame. The check is bounded by a read deadline and mandatory: a stream the
// guest cannot bound is a protocol error, never silently accepted.
func validateHelperEOF(stdout io.ReadCloser, out *bufio.Reader) error {
	file, ok := stdout.(*os.File)
	if !ok {
		return fmt.Errorf("%w: helper stdout is not a bounded pipe", errHelperProtocol)
	}
	if err := file.SetReadDeadline(time.Now().Add(helperTrailerTimeout)); err != nil {
		return fmt.Errorf("%w: cannot bound the trailing-frame check: %v", errHelperProtocol, err)
	}
	kind, _, err := readHelperFrame(out)
	switch {
	case errors.Is(err, io.EOF):
		return nil
	case err == nil:
		return fmt.Errorf("%w: trailing frame %d after the result", errHelperProtocol, kind)
	case errors.Is(err, os.ErrDeadlineExceeded):
		return fmt.Errorf("%w: helper did not close its output", errHelperProtocol)
	default:
		return fmt.Errorf("%w: trailing read: %v", errHelperProtocol, err)
	}
}

// fail maps a transport failure on an operation. Cancellation is reported as
// Canceled; anything else is a helper failure with a bounded stderr excerpt.
func (c *helperConn) fail(ctx context.Context, err error) error {
	c.stopWatcher()
	if ctxErr := ctx.Err(); ctxErr != nil {
		c.kill()
		_ = c.reap(helperWaitTimeout)
		return status.FromContextError(ctxErr).Err()
	}
	if errors.Is(err, errHelperProtocol) {
		c.stopWatcher()
		c.kill()
		_ = c.reap(helperWaitTimeout)
		return c.protocolError(err)
	}
	c.kill()
	waitErr := c.reap(helperWaitTimeout)
	return c.childFailure(errors.Join(err, waitErr))
}

// protocolf kills the child and reports a malformed exchange.
func (c *helperConn) protocolf(format string, args ...any) error {
	c.stopWatcher()
	c.kill()
	_ = c.reap(helperWaitTimeout)
	return c.protocolError(fmt.Errorf(format, args...))
}

func (c *helperConn) protocolError(err error) error {
	msg := fmt.Sprintf("%v", err)
	if !errors.Is(err, errHelperProtocol) {
		msg = errHelperProtocol.Error() + ": " + msg
	}
	return status.Error(codes.Internal, msg)
}

func (c *helperConn) childFailure(err error) error {
	msg := err.Error()
	if excerpt := strings.TrimSpace(c.stderr.String()); excerpt != "" {
		msg += ": " + excerpt
	}
	return status.Error(codes.Internal, "file helper: "+msg)
}

func (c *helperConn) stopWatcher() {
	if c.stop != nil {
		c.stop()
		c.stop = nil
	}
}

func (c *helperConn) kill() {
	c.mu.Lock()
	if c.exited || c.cmd.Process == nil {
		c.mu.Unlock()
		return
	}
	pid := c.cmd.Process.Pid
	c.mu.Unlock()
	_ = syscall.Kill(-pid, syscall.SIGKILL)
}

// reap waits for the child, killing its whole group if it overstays.
func (c *helperConn) reap(timeout time.Duration) error {
	c.mu.Lock()
	if c.exited {
		err := c.exitErr
		c.mu.Unlock()
		return err
	}
	c.mu.Unlock()
	// Wait may only run after every read from StdoutPipe has finished, so it
	// starts here rather than at spawn time; a timeout kills the group and
	// then waits for the same Wait call.
	done := make(chan error, 1)
	go func() { done <- c.cmd.Wait() }()
	select {
	case err := <-done:
		c.mu.Lock()
		c.exited, c.exitErr = true, err
		c.mu.Unlock()
		return err
	case <-time.After(timeout):
		c.kill()
		err := <-done
		c.mu.Lock()
		c.exited, c.exitErr = true, err
		c.mu.Unlock()
		return err
	}
}

// abort closes stdin (the child discards its staged file) and reaps it,
// killing the group if it does not exit within the caller's deadline.
func (c *helperConn) abort() {
	c.stopWatcher()
	c.stdin.Close()
	_ = c.reap(helperWaitTimeout)
}

func (c *helperConn) abortWait(ctx context.Context) error {
	c.stopWatcher()
	c.stdin.Close()
	timeout := helperWaitTimeout
	if deadline, ok := ctx.Deadline(); ok {
		timeout = min(timeout, time.Until(deadline))
	}
	if timeout < time.Second {
		timeout = time.Second
	}
	if err := c.reap(timeout); err != nil {
		return fmt.Errorf("file helper cleanup: %w", err)
	}
	return nil
}

// boundedBuffer keeps at most limit bytes and discards the rest as it is
// written, so a chatty child cannot grow memory.
type boundedBuffer struct {
	mu    sync.Mutex
	buf   []byte
	limit int
}

func (b *boundedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if room := b.limit - len(b.buf); room > 0 {
		b.buf = append(b.buf, p[:min(room, len(p))]...)
	}
	return len(p), nil
}

func (b *boundedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return string(b.buf)
}
