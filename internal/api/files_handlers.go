package api

import (
	"context"
	"encoding/base64"
	"net/http"
	"path"
	"unicode/utf8"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/apierr"
	"github.com/nathabonfim59/bamboo-raft/internal/auth"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// fileRequest is the authenticated owner and sandbox of one call.
type fileRequest struct {
	owner     string
	sandboxID string
}

// openRequest authenticates the caller and reads the sandbox path parameter.
// Missing identity is 401; a missing sandbox parameter is the owner-scoped
// 404, exactly like an unknown sandbox.
func (s *filesService) openRequest(w http.ResponseWriter, r *http.Request) (fileRequest, bool) {
	if r.Context().Err() != nil {
		return fileRequest{}, false
	}
	identity, ok := auth.IdentityFrom(r.Context())
	if !ok || identity.Owner == "" {
		fileWriteError(w, r, fileUnauthorized())
		return fileRequest{}, false
	}
	sandboxID := r.PathValue(ParamSandboxID)
	if sandboxID == "" {
		fileWriteError(w, r, fileNotFound())
		return fileRequest{}, false
	}
	return fileRequest{owner: identity.Owner, sandboxID: sandboxID}, true
}

// readFile serves GET FilesPath.
func (s *filesService) readFile(w http.ResponseWriter, r *http.Request) {
	request, ok := s.openRequest(w, r)
	if !ok {
		return
	}
	query, apiErr := fileParseQuery(r, true, true, false)
	if apiErr != nil {
		fileWriteError(w, r, apiErr)
		return
	}
	canonical, apiErr := fileRequiredQueryPath(query)
	if apiErr != nil {
		fileWriteError(w, r, apiErr)
		return
	}
	target, apiErr, proceed := s.resolveTarget(r.Context(), request.owner, request.sandboxID)
	if !fileRespondTarget(w, r, apiErr, proceed) {
		return
	}
	client, closeClient, ok := s.dialForRequest(w, r, target.NodeID)
	if !ok {
		return
	}
	defer s.closeClient(closeClient)

	result, apiErr, proceed := s.readFileStream(r.Context(), client, request.sandboxID, canonical)
	if !proceed {
		return
	}
	if apiErr != nil {
		fileWriteError(w, r, apiErr)
		return
	}
	content, apiErr := fileRenderContent(query.encoding, result.data)
	if apiErr != nil {
		fileWriteError(w, r, apiErr)
		return
	}
	fileWriteSuccess(w, r, http.StatusOK, TypeFileRead,
		NewFileReadResponse(result.path, query.encoding, int64(len(result.data)), content))
}

// fileRenderContent converts the assembled bytes to the requested encoding.
// Invalid UTF-8 is refused rather than replaced; base64 is the lossless
// escape.
func fileRenderContent(encoding FileEncoding, data []byte) (string, *apierr.Error) {
	switch encoding {
	case FileEncodingBase64:
		return base64.StdEncoding.EncodeToString(data), nil
	case FileEncodingUTF8:
		if !utf8.Valid(data) {
			return "", fileInvalidQuery("encoding", "file is not valid UTF-8; use encoding=base64")
		}
		return string(data), nil
	default:
		return "", fileInvalidQuery("encoding", "must be utf8 or base64")
	}
}

// writeFile serves PUT FilesPath.
func (s *filesService) writeFile(w http.ResponseWriter, r *http.Request) {
	request, ok := s.openRequest(w, r)
	if !ok {
		return
	}
	if _, apiErr := fileParseQuery(r, false, false, false); apiErr != nil {
		fileWriteError(w, r, apiErr)
		return
	}
	body, apiErr := fileDecodeWrite(r)
	if apiErr != nil {
		fileWriteError(w, r, apiErr)
		return
	}
	canonical, apiErr := fileCheckPath(body.Path)
	if apiErr != nil {
		fileWriteError(w, r, apiErr)
		return
	}
	data, apiErr := fileDecodeContent(body)
	if apiErr != nil {
		fileWriteError(w, r, apiErr)
		return
	}
	target, apiErr, proceed := s.resolveTarget(r.Context(), request.owner, request.sandboxID)
	if !fileRespondTarget(w, r, apiErr, proceed) {
		return
	}
	client, closeClient, ok := s.dialForRequest(w, r, target.NodeID)
	if !ok {
		return
	}
	defer s.closeClient(closeClient)

	response, apiErr, proceed := s.putFileStream(r.Context(), client, request.sandboxID, canonical, data)
	if !proceed {
		return
	}
	if apiErr != nil {
		fileWriteError(w, r, apiErr)
		return
	}
	fileWriteSuccess(w, r, http.StatusOK, TypeFileWritten,
		NewFileWriteResponse(response.GetPath(), body.Encoding, response.GetSize()))
}

// deleteFile serves DELETE FilesPath.
func (s *filesService) deleteFile(w http.ResponseWriter, r *http.Request) {
	request, ok := s.openRequest(w, r)
	if !ok {
		return
	}
	query, apiErr := fileParseQuery(r, true, false, true)
	if apiErr != nil {
		fileWriteError(w, r, apiErr)
		return
	}
	canonical, apiErr := fileRequiredQueryPath(query)
	if apiErr != nil {
		fileWriteError(w, r, apiErr)
		return
	}
	if apiErr := fileRejectRoot(canonical); apiErr != nil {
		fileWriteError(w, r, apiErr)
		return
	}
	target, apiErr, proceed := s.resolveTarget(r.Context(), request.owner, request.sandboxID)
	if !fileRespondTarget(w, r, apiErr, proceed) {
		return
	}
	client, closeClient, ok := s.dialForRequest(w, r, target.NodeID)
	if !ok {
		return
	}
	defer s.closeClient(closeClient)

	response, err := client.DeleteFile(r.Context(), &nodev1.DeleteFileRequest{
		SandboxId: request.sandboxID,
		Path:      canonical,
		Recursive: query.recursive,
	})
	if err != nil {
		fileWriteError(w, r, s.mapNodeError(r.Context(), fileOpDelete, request.sandboxID, err))
		return
	}
	if response == nil || response.GetPath() != canonical {
		fileWriteError(w, r, s.unaryContractViolation(r.Context(), fileOpDelete, request.sandboxID, "delete_response"))
		return
	}
	fileWriteSuccess(w, r, http.StatusOK, TypeFileDeleted, NewFileDeleteResponse(response.GetPath()))
}

// listFiles serves GET FilesListPath.
func (s *filesService) listFiles(w http.ResponseWriter, r *http.Request) {
	request, ok := s.openRequest(w, r)
	if !ok {
		return
	}
	query, apiErr := fileParseQuery(r, true, false, false)
	if apiErr != nil {
		fileWriteError(w, r, apiErr)
		return
	}
	canonical, apiErr := fileOptionalQueryPath(query, "/home/user")
	if apiErr != nil {
		fileWriteError(w, r, apiErr)
		return
	}
	target, apiErr, proceed := s.resolveTarget(r.Context(), request.owner, request.sandboxID)
	if !fileRespondTarget(w, r, apiErr, proceed) {
		return
	}
	client, closeClient, ok := s.dialForRequest(w, r, target.NodeID)
	if !ok {
		return
	}
	defer s.closeClient(closeClient)

	response, err := client.ListFiles(r.Context(), &nodev1.ListFilesRequest{
		SandboxId: request.sandboxID,
		Path:      canonical,
		Limit:     FileListEntries,
	})
	if err != nil {
		fileWriteError(w, r, s.mapNodeError(r.Context(), fileOpList, request.sandboxID, err))
		return
	}
	if response == nil || response.GetPath() != canonical {
		fileWriteError(w, r, s.unaryContractViolation(r.Context(), fileOpList, request.sandboxID, "list_response"))
		return
	}
	rawEntries := response.GetEntries()
	if len(rawEntries) > int(FileListEntries) {
		fileWriteError(w, r, s.unaryContractViolation(r.Context(), fileOpList, request.sandboxID, "list_bounds"))
		return
	}
	entries := make([]FileEntry, 0, len(rawEntries))
	previous := ""
	for index, info := range rawEntries {
		entry, apiErr := s.fileEntry(r.Context(), fileOpList, request.sandboxID, info)
		if apiErr != nil {
			fileWriteError(w, r, apiErr)
			return
		}
		if path.Dir(entry.Path) != canonical {
			fileWriteError(w, r, s.unaryContractViolation(r.Context(), fileOpList, request.sandboxID, "list_child"))
			return
		}
		if index > 0 && entry.Name <= previous {
			fileWriteError(w, r, s.unaryContractViolation(r.Context(), fileOpList, request.sandboxID, "list_order"))
			return
		}
		previous = entry.Name
		entries = append(entries, entry)
	}
	fileWriteSuccess(w, r, http.StatusOK, TypeFileList,
		NewFileListResponse(response.GetPath(), entries, response.GetTruncated()))
}

// getFileInfo serves GET FilesInfoPath.
func (s *filesService) getFileInfo(w http.ResponseWriter, r *http.Request) {
	request, ok := s.openRequest(w, r)
	if !ok {
		return
	}
	query, apiErr := fileParseQuery(r, true, false, false)
	if apiErr != nil {
		fileWriteError(w, r, apiErr)
		return
	}
	canonical, apiErr := fileRequiredQueryPath(query)
	if apiErr != nil {
		fileWriteError(w, r, apiErr)
		return
	}
	target, apiErr, proceed := s.resolveTarget(r.Context(), request.owner, request.sandboxID)
	if !fileRespondTarget(w, r, apiErr, proceed) {
		return
	}
	client, closeClient, ok := s.dialForRequest(w, r, target.NodeID)
	if !ok {
		return
	}
	defer s.closeClient(closeClient)

	info, err := client.StatFile(r.Context(), &nodev1.StatFileRequest{
		SandboxId: request.sandboxID,
		Path:      canonical,
	})
	if err != nil {
		fileWriteError(w, r, s.mapNodeError(r.Context(), fileOpStat, request.sandboxID, err))
		return
	}
	if info == nil || info.GetPath() != canonical {
		fileWriteError(w, r, s.unaryContractViolation(r.Context(), fileOpStat, request.sandboxID, "stat_response"))
		return
	}
	entry, apiErr := s.fileEntry(r.Context(), fileOpStat, request.sandboxID, info)
	if apiErr != nil {
		fileWriteError(w, r, apiErr)
		return
	}
	fileWriteSuccess(w, r, http.StatusOK, TypeFileInfo,
		NewFileInfoResponse(TypeFileInfo, entry))
}

// createFolder serves POST FilesFolderPath.
func (s *filesService) createFolder(w http.ResponseWriter, r *http.Request) {
	request, ok := s.openRequest(w, r)
	if !ok {
		return
	}
	if _, apiErr := fileParseQuery(r, false, false, false); apiErr != nil {
		fileWriteError(w, r, apiErr)
		return
	}
	body, apiErr := fileDecodeFolder(r)
	if apiErr != nil {
		fileWriteError(w, r, apiErr)
		return
	}
	canonical, apiErr := fileCheckPath(body.Path)
	if apiErr != nil {
		fileWriteError(w, r, apiErr)
		return
	}
	var mode uint32
	modeSet := body.Mode != ""
	if modeSet {
		parsed, ok := fileParseMode(body.Mode)
		if !ok {
			fileWriteError(w, r, fileInvalidJSON("mode must be octal permission bits up to 7777"))
			return
		}
		mode = parsed
	}
	target, apiErr, proceed := s.resolveTarget(r.Context(), request.owner, request.sandboxID)
	if !fileRespondTarget(w, r, apiErr, proceed) {
		return
	}
	client, closeClient, ok := s.dialForRequest(w, r, target.NodeID)
	if !ok {
		return
	}
	defer s.closeClient(closeClient)

	var info *nodev1.FileInfo
	var err error
	if modeSet && mode == 0 {
		info, apiErr, proceed = s.makeZeroModeFolder(r.Context(), client, request.sandboxID, canonical)
		if !proceed {
			return
		}
		if apiErr != nil {
			fileWriteError(w, r, apiErr)
			return
		}
	} else {
		info, err = client.MakeDir(r.Context(), &nodev1.MakeDirRequest{
			SandboxId: request.sandboxID,
			Path:      canonical,
			Mode:      mode,
		})
		if err != nil {
			fileWriteError(w, r, s.mapNodeError(r.Context(), fileOpMakeDir, request.sandboxID, err))
			return
		}
		if info == nil || info.GetPath() != canonical || info.GetType() != nodev1.FileType_FILE_TYPE_DIRECTORY {
			fileWriteError(w, r, s.unaryContractViolation(r.Context(), fileOpMakeDir, request.sandboxID, "mkdir_response"))
			return
		}
	}
	entry, apiErr := s.fileEntry(r.Context(), fileOpMakeDir, request.sandboxID, info)
	if apiErr != nil {
		fileWriteError(w, r, apiErr)
		return
	}
	fileWriteSuccess(w, r, http.StatusOK, TypeFolderCreated,
		NewFileInfoResponse(TypeFolderCreated, entry))
}

// makeZeroModeFolder implements the approved explicit-0000 createFolder
// protocol. An existing path gets a plain MakeDir and keeps its mode; a
// missing path gets MakeDir (the node's 0755 default) followed by Chmod 0.
// The returned metadata comes from the last RPC that produced it. A Chmod
// failure is a request error and is never cleaned up, because a concurrent
// creator may also own the path. A concurrent creator between the missing
// Stat and MakeDir can therefore end at mode 0000; that is the approved race.
func (s *filesService) makeZeroModeFolder(ctx context.Context, client nodev1.NodeClient, sandboxID, canonical string) (*nodev1.FileInfo, *apierr.Error, bool) {
	if ctx.Err() != nil {
		return nil, nil, false
	}
	existing, err := client.StatFile(ctx, &nodev1.StatFileRequest{SandboxId: sandboxID, Path: canonical})
	if err != nil {
		if ctx.Err() != nil {
			return nil, nil, false
		}
		if status.Code(err) != codes.NotFound {
			return nil, s.mapNodeError(ctx, fileOpStat, sandboxID, err), true
		}
	} else {
		if existing == nil || existing.GetPath() != canonical {
			return nil, s.unaryContractViolation(ctx, fileOpStat, sandboxID, "stat_response"), true
		}
		if _, err := FileEntryFromInfo(existing); err != nil {
			return nil, s.unaryContractViolation(ctx, fileOpStat, sandboxID, "stat_metadata"), true
		}
		if ctx.Err() != nil {
			return nil, nil, false
		}
		info, err := client.MakeDir(ctx, &nodev1.MakeDirRequest{SandboxId: sandboxID, Path: canonical})
		if err != nil {
			if ctx.Err() != nil {
				return nil, nil, false
			}
			return nil, s.mapNodeError(ctx, fileOpMakeDir, sandboxID, err), true
		}
		if info == nil || info.GetPath() != canonical || info.GetType() != nodev1.FileType_FILE_TYPE_DIRECTORY {
			return nil, s.unaryContractViolation(ctx, fileOpMakeDir, sandboxID, "mkdir_response"), true
		}
		if _, err := FileEntryFromInfo(info); err != nil {
			return nil, s.unaryContractViolation(ctx, fileOpMakeDir, sandboxID, "mkdir_metadata"), true
		}
		return info, nil, true
	}

	info, err := client.MakeDir(ctx, &nodev1.MakeDirRequest{SandboxId: sandboxID, Path: canonical})
	if err != nil {
		if ctx.Err() != nil {
			return nil, nil, false
		}
		return nil, s.mapNodeError(ctx, fileOpMakeDir, sandboxID, err), true
	}
	if ctx.Err() != nil {
		return nil, nil, false
	}
	if info == nil || info.GetPath() != canonical || info.GetType() != nodev1.FileType_FILE_TYPE_DIRECTORY {
		return nil, s.unaryContractViolation(ctx, fileOpMakeDir, sandboxID, "mkdir_response"), true
	}
	if _, err := FileEntryFromInfo(info); err != nil {
		return nil, s.unaryContractViolation(ctx, fileOpMakeDir, sandboxID, "mkdir_metadata"), true
	}
	info, err = client.ChmodFile(ctx, &nodev1.ChmodFileRequest{SandboxId: sandboxID, Path: canonical, Mode: 0})
	if err != nil {
		if ctx.Err() != nil {
			return nil, nil, false
		}
		return nil, s.mapNodeError(ctx, fileOpChmod, sandboxID, err), true
	}
	if ctx.Err() != nil {
		return nil, nil, false
	}
	if info == nil || info.GetPath() != canonical || info.GetType() != nodev1.FileType_FILE_TYPE_DIRECTORY {
		return nil, s.unaryContractViolation(ctx, fileOpChmod, sandboxID, "chmod_response"), true
	}
	if _, err := FileEntryFromInfo(info); err != nil {
		return nil, s.unaryContractViolation(ctx, fileOpChmod, sandboxID, "chmod_metadata"), true
	}
	return info, nil, true
}

// moveFile serves POST FilesMovePath.
func (s *filesService) moveFile(w http.ResponseWriter, r *http.Request) {
	request, ok := s.openRequest(w, r)
	if !ok {
		return
	}
	if _, apiErr := fileParseQuery(r, false, false, false); apiErr != nil {
		fileWriteError(w, r, apiErr)
		return
	}
	body, apiErr := fileDecodeMove(r)
	if apiErr != nil {
		fileWriteError(w, r, apiErr)
		return
	}
	source, apiErr := fileCheckPath(body.Source)
	if apiErr != nil {
		fileWriteError(w, r, apiErr)
		return
	}
	destination, apiErr := fileCheckPath(body.Destination)
	if apiErr != nil {
		fileWriteError(w, r, apiErr)
		return
	}
	if apiErr := fileRejectRoot(source); apiErr != nil {
		fileWriteError(w, r, apiErr)
		return
	}
	if apiErr := fileRejectRoot(destination); apiErr != nil {
		fileWriteError(w, r, apiErr)
		return
	}
	target, apiErr, proceed := s.resolveTarget(r.Context(), request.owner, request.sandboxID)
	if !fileRespondTarget(w, r, apiErr, proceed) {
		return
	}
	client, closeClient, ok := s.dialForRequest(w, r, target.NodeID)
	if !ok {
		return
	}
	defer s.closeClient(closeClient)

	response, err := client.MoveFile(r.Context(), &nodev1.MoveFileRequest{
		SandboxId:   request.sandboxID,
		Source:      source,
		Destination: destination,
		Overwrite:   body.Overwrite,
	})
	if err != nil {
		fileWriteError(w, r, s.mapNodeError(r.Context(), fileOpMove, request.sandboxID, err))
		return
	}
	if response == nil || response.GetSource() != source || response.GetDestination() != destination {
		fileWriteError(w, r, s.unaryContractViolation(r.Context(), fileOpMove, request.sandboxID, "move_response"))
		return
	}
	fileWriteSuccess(w, r, http.StatusOK, TypeFileMoved,
		NewFileMoveResponse(response.GetSource(), response.GetDestination()))
}

// setFilePermissions serves POST FilesPermissionsPath.
func (s *filesService) setFilePermissions(w http.ResponseWriter, r *http.Request) {
	request, ok := s.openRequest(w, r)
	if !ok {
		return
	}
	if _, apiErr := fileParseQuery(r, false, false, false); apiErr != nil {
		fileWriteError(w, r, apiErr)
		return
	}
	body, apiErr := fileDecodePermissions(r)
	if apiErr != nil {
		fileWriteError(w, r, apiErr)
		return
	}
	canonical, apiErr := fileCheckPath(body.Path)
	if apiErr != nil {
		fileWriteError(w, r, apiErr)
		return
	}
	mode, apiErr := fileRequiredMode(body.Mode)
	if apiErr != nil {
		fileWriteError(w, r, apiErr)
		return
	}
	target, apiErr, proceed := s.resolveTarget(r.Context(), request.owner, request.sandboxID)
	if !fileRespondTarget(w, r, apiErr, proceed) {
		return
	}
	client, closeClient, ok := s.dialForRequest(w, r, target.NodeID)
	if !ok {
		return
	}
	defer s.closeClient(closeClient)

	info, err := client.ChmodFile(r.Context(), &nodev1.ChmodFileRequest{
		SandboxId: request.sandboxID,
		Path:      canonical,
		Mode:      mode,
	})
	if err != nil {
		fileWriteError(w, r, s.mapNodeError(r.Context(), fileOpChmod, request.sandboxID, err))
		return
	}
	if info == nil || info.GetPath() != canonical {
		fileWriteError(w, r, s.unaryContractViolation(r.Context(), fileOpChmod, request.sandboxID, "chmod_response"))
		return
	}
	entry, apiErr := s.fileEntry(r.Context(), fileOpChmod, request.sandboxID, info)
	if apiErr != nil {
		fileWriteError(w, r, apiErr)
		return
	}
	fileWriteSuccess(w, r, http.StatusOK, TypeFilePermissionsSet,
		NewFileInfoResponse(TypeFilePermissionsSet, entry))
}
