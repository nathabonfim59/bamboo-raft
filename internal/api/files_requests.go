package api

import (
	"bytes"
	"encoding/base64"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"io"
	"mime"
	"net/http"
	"path"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/nathabonfim59/bamboo-raft/internal/apierr"
)

// errFilePath reports a path that cannot be confined to the sandbox roots.
// The reason is fixed: it never echoes request text.
var errFilePath = errors.New("api: file path is outside /home/user and /tmp")

// fileQuery is the strict parsed query of one file operation.
type fileQuery struct {
	path      string
	pathSet   bool
	encoding  FileEncoding
	recursive bool
}

// fileParseQuery parses the query of one file operation. Unknown, duplicate
// or malformed parameters are 400 invalid_query. Each operation enables only
// the parameters the pin defines for it.
func fileParseQuery(r *http.Request, allowPath, allowEncoding, allowRecursive bool) (fileQuery, *apierr.Error) {
	values, apiErr := coreParseQuery(r)
	if apiErr != nil {
		return fileQuery{}, apiErr
	}
	for name := range values {
		switch {
		case allowPath && name == "path":
		case allowEncoding && name == "encoding":
		case allowRecursive && name == "recursive":
		default:
			return fileQuery{}, fileInvalidQuery(name, "unsupported query parameter")
		}
	}
	q := fileQuery{encoding: FileEncodingUTF8}
	raw, ok, err := singleValue(values, "path")
	if err != nil {
		return fileQuery{}, fileInvalidQuery("path", "must be given at most once")
	}
	if ok {
		q.path, q.pathSet = raw, true
	}
	if allowEncoding {
		raw, ok, err := singleValue(values, "encoding")
		if err != nil {
			return fileQuery{}, fileInvalidQuery("encoding", "must be given at most once")
		}
		if ok {
			encoding := FileEncoding(raw)
			if !encoding.valid() {
				return fileQuery{}, fileInvalidQuery("encoding", "must be utf8 or base64")
			}
			q.encoding = encoding
		}
	}
	if allowRecursive {
		raw, ok, err := singleValue(values, "recursive")
		if err != nil {
			return fileQuery{}, fileInvalidQuery("recursive", "must be given at most once")
		}
		if ok {
			value, parseErr := strconv.ParseBool(raw)
			if parseErr != nil {
				return fileQuery{}, fileInvalidQuery("recursive", "must be a boolean")
			}
			q.recursive = value
		}
	}
	return q, nil
}

// fileRequiredQueryPath returns the canonical path of a required query
// parameter. A missing parameter is invalid_query; a present but unusable one
// is invalid_path.
func fileRequiredQueryPath(q fileQuery) (string, *apierr.Error) {
	if !q.pathSet {
		return "", fileInvalidQuery("path", "path is required")
	}
	return fileCheckPath(q.path)
}

// fileOptionalQueryPath returns the canonical path of an optional query
// parameter or def when it is absent.
func fileOptionalQueryPath(q fileQuery, def string) (string, *apierr.Error) {
	if !q.pathSet {
		return def, nil
	}
	return fileCheckPath(q.path)
}

// fileCheckPath canonicalizes a provided path and maps every refusal to the
// pinned 400 invalid_path answer.
func fileCheckPath(raw string) (string, *apierr.Error) {
	canonical, err := fileCanonicalPath(raw)
	if err != nil {
		return "", fileInvalidPath()
	}
	return canonical, nil
}

// fileCanonicalPath resolves an API file path lexically: a relative path is
// taken from /home/user, the result is cleaned, and it must land on or under
// /home/user or /tmp. Dot-dot segments that stay inside those roots are
// allowed; only the final confined path matters. Symlinks are resolved by the
// guest, never here. An empty path, a NUL byte or a path over the length bound
// is refused.
func fileCanonicalPath(raw string) (string, error) {
	if raw == "" || strings.ContainsRune(raw, 0) || !utf8.ValidString(raw) {
		return "", errFilePath
	}
	if !path.IsAbs(raw) {
		raw = "/home/user/" + raw
	}
	cleaned := path.Clean(raw)
	if len(cleaned) > maxFilePathBytes {
		return "", errFilePath
	}
	for _, base := range []string{"/home/user", "/tmp"} {
		if cleaned == base || strings.HasPrefix(cleaned, base+"/") {
			return cleaned, nil
		}
	}
	return "", errFilePath
}

// filePathIsRoot reports whether canonical is one of the sandbox roots that
// cannot be deleted or moved.
func filePathIsRoot(canonical string) bool {
	return canonical == "/home/user" || canonical == "/tmp"
}

// fileRejectRoot refuses an operation on /home/user or /tmp themselves.
func fileRejectRoot(canonical string) *apierr.Error {
	if filePathIsRoot(canonical) {
		return fileInvalidPathMessage("cannot delete or move /home/user or /tmp")
	}
	return nil
}

// fileParseMode parses octal permission bits. The result is at most 0o7777;
// an empty string or a non-octal digit is refused. Arbitrary zero padding is
// accepted, so "0000000755" is the same mode as "0755".
func fileParseMode(raw string) (uint32, bool) {
	if raw == "" {
		return 0, false
	}
	for i := 0; i < len(raw); i++ {
		if raw[i] < '0' || raw[i] > '7' {
			return 0, false
		}
	}
	value, err := strconv.ParseUint(raw, 8, 32)
	if err != nil || value > 0o7777 {
		return 0, false
	}
	return uint32(value), true
}

// fileRequiredMode parses a required mode member. A missing, empty or invalid
// member is invalid_json.
func fileRequiredMode(raw string) (uint32, *apierr.Error) {
	if raw == "" {
		return 0, fileInvalidJSON("mode is required")
	}
	mode, ok := fileParseMode(raw)
	if !ok {
		return 0, fileInvalidJSON("mode must be octal permission bits up to 7777")
	}
	return mode, nil
}

// fileReadJSONBody reads one bounded JSON body. ErrBodyTooLarge is returned
// before the whole body is buffered.
func fileReadJSONBody(r *http.Request, limit int64) ([]byte, error) {
	if r.Body == nil {
		return nil, ErrEmptyBody
	}
	raw, err := io.ReadAll(io.LimitReader(r.Body, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(raw)) > limit {
		return nil, ErrBodyTooLarge
	}
	return raw, nil
}

// fileRequireJSONMedia rejects a declared non-JSON media type. A missing
// Content-Type is accepted because the body is still decoded strictly.
func fileRequireJSONMedia(r *http.Request) *apierr.Error {
	raw := r.Header.Get("Content-Type")
	if raw == "" {
		return nil
	}
	mediaType, _, err := mime.ParseMediaType(raw)
	if err != nil || !strings.EqualFold(mediaType, "application/json") {
		return fileInvalidJSON("Content-Type must be application/json")
	}
	return nil
}

// fileDecodeJSON strictly decodes raw into syntax: unknown and duplicate
// members and trailing values are refused.
func fileDecodeJSON(raw []byte, syntax any) error {
	return json.Unmarshal(raw, syntax, json.RejectUnknownMembers(true))
}

// fileWriteSyntax is the strict first pass of the writeFile body.
type fileWriteSyntax struct {
	Path     jsontext.Value `json:"path,omitzero"`
	Content  jsontext.Value `json:"content,omitzero"`
	Encoding jsontext.Value `json:"encoding,omitzero"`
}

// fileFolderSyntax is the strict first pass of the createFolder body.
type fileFolderSyntax struct {
	Path jsontext.Value `json:"path,omitzero"`
	Mode jsontext.Value `json:"mode,omitzero"`
}

// fileMoveSyntax is the strict first pass of the moveFile body.
type fileMoveSyntax struct {
	Source      jsontext.Value `json:"source,omitzero"`
	Destination jsontext.Value `json:"destination,omitzero"`
	Overwrite   jsontext.Value `json:"overwrite,omitzero"`
}

// filePermissionsSyntax is the strict first pass of the setFilePermissions
// body.
type filePermissionsSyntax struct {
	Path jsontext.Value `json:"path,omitzero"`
	Mode jsontext.Value `json:"mode,omitzero"`
}

// fileDecodeWrite parses and validates the writeFile body. Typed member and
// null checks are invalid_json; the decoded byte limit is enforced later.
func fileDecodeWrite(r *http.Request) (FileWriteRequest, *apierr.Error) {
	var request FileWriteRequest
	if apiErr := fileRequireJSONMedia(r); apiErr != nil {
		return request, apiErr
	}
	raw, err := fileReadJSONBody(r, maxFileWriteBodyBytes)
	if err != nil {
		return request, fileBodyReadError(err)
	}
	var syntax fileWriteSyntax
	if err := fileDecodeJSON(raw, &syntax); err != nil {
		return request, fileInvalidJSON("request body is not valid JSON")
	}
	if _, apiErr := fileRequiredString(syntax.Path, "path"); apiErr != nil {
		return request, apiErr
	}
	if _, apiErr := fileRequiredString(syntax.Content, "content"); apiErr != nil {
		return request, apiErr
	}
	if _, apiErr := fileOptionalEncoding(syntax.Encoding); apiErr != nil {
		return request, apiErr
	}
	if err := json.Unmarshal(raw, &request); err != nil {
		return request, fileInvalidJSON("request body is not valid JSON")
	}
	if request.Encoding == "" {
		request.Encoding = FileEncodingUTF8
	}
	return request, nil
}

// fileDecodeFolder parses and validates the createFolder body.
func fileDecodeFolder(r *http.Request) (FileFolderRequest, *apierr.Error) {
	var request FileFolderRequest
	if apiErr := fileRequireJSONMedia(r); apiErr != nil {
		return request, apiErr
	}
	raw, err := fileReadJSONBody(r, maxFileJSONBodyBytes)
	if err != nil {
		return request, fileJSONBodyError(err)
	}
	var syntax fileFolderSyntax
	if err := fileDecodeJSON(raw, &syntax); err != nil {
		return request, fileInvalidJSON("request body is not valid JSON")
	}
	if _, apiErr := fileRequiredString(syntax.Path, "path"); apiErr != nil {
		return request, apiErr
	}
	mode, modeSet, apiErr := fileOptionalString(syntax.Mode, "mode")
	if apiErr != nil {
		return request, apiErr
	}
	if modeSet && mode == "" {
		return request, fileInvalidJSON("mode must be octal permission bits up to 7777")
	}
	if err := json.Unmarshal(raw, &request); err != nil {
		return request, fileInvalidJSON("request body is not valid JSON")
	}
	return request, nil
}

// fileDecodeMove parses and validates the moveFile body.
func fileDecodeMove(r *http.Request) (FileMoveRequest, *apierr.Error) {
	var request FileMoveRequest
	if apiErr := fileRequireJSONMedia(r); apiErr != nil {
		return request, apiErr
	}
	raw, err := fileReadJSONBody(r, maxFileJSONBodyBytes)
	if err != nil {
		return request, fileJSONBodyError(err)
	}
	var syntax fileMoveSyntax
	if err := fileDecodeJSON(raw, &syntax); err != nil {
		return request, fileInvalidJSON("request body is not valid JSON")
	}
	if _, apiErr := fileRequiredString(syntax.Source, "source"); apiErr != nil {
		return request, apiErr
	}
	if _, apiErr := fileRequiredString(syntax.Destination, "destination"); apiErr != nil {
		return request, apiErr
	}
	if _, apiErr := fileOptionalBool(syntax.Overwrite, "overwrite"); apiErr != nil {
		return request, apiErr
	}
	if err := json.Unmarshal(raw, &request); err != nil {
		return request, fileInvalidJSON("request body is not valid JSON")
	}
	return request, nil
}

// fileDecodePermissions parses and validates the setFilePermissions body.
func fileDecodePermissions(r *http.Request) (FilePermissionsRequest, *apierr.Error) {
	var request FilePermissionsRequest
	if apiErr := fileRequireJSONMedia(r); apiErr != nil {
		return request, apiErr
	}
	raw, err := fileReadJSONBody(r, maxFileJSONBodyBytes)
	if err != nil {
		return request, fileJSONBodyError(err)
	}
	var syntax filePermissionsSyntax
	if err := fileDecodeJSON(raw, &syntax); err != nil {
		return request, fileInvalidJSON("request body is not valid JSON")
	}
	if _, apiErr := fileRequiredString(syntax.Path, "path"); apiErr != nil {
		return request, apiErr
	}
	if _, apiErr := fileRequiredString(syntax.Mode, "mode"); apiErr != nil {
		return request, apiErr
	}
	if err := json.Unmarshal(raw, &request); err != nil {
		return request, fileInvalidJSON("request body is not valid JSON")
	}
	return request, nil
}

// fileBodyReadError maps a writeFile body read failure. A body past the raw
// transport bound is refused as the pinned 413 without claiming the decoded
// size: the bound covers worst-case JSON escaping of an inline file, but
// whitespace or other body inflation can also exceed it. Any other read
// failure is invalid_json.
func fileBodyReadError(err error) *apierr.Error {
	if errors.Is(err, ErrBodyTooLarge) {
		return NewFileTooLargeError(FileInlineLimitBytes, 0, false)
	}
	return fileInvalidJSON("request body is not valid JSON")
}

// fileJSONBodyError maps a small JSON body read failure to invalid_json.
func fileJSONBodyError(_ error) *apierr.Error {
	return fileInvalidJSON("request body is not valid JSON")
}

// fileJSONString reports whether value is a JSON string (including the empty
// one).
func fileJSONString(value jsontext.Value) bool {
	value = trimJSON(value)
	return len(value) > 0 && value[0] == '"'
}

// fileRequiredString validates a required string member.
func fileRequiredString(value jsontext.Value, name string) (string, *apierr.Error) {
	value = trimJSON(value)
	switch {
	case len(value) == 0 || equalJSON(value, "null"):
		return "", fileInvalidJSON(name + " is required")
	case !fileJSONString(value):
		return "", fileInvalidJSON(name + " must be a string")
	}
	var s string
	if err := json.Unmarshal(value, &s); err != nil {
		return "", fileInvalidJSON(name + " must be a string")
	}
	return s, nil
}

// fileOptionalString validates an optional string member.
func fileOptionalString(value jsontext.Value, name string) (string, bool, *apierr.Error) {
	value = trimJSON(value)
	if len(value) == 0 {
		return "", false, nil
	}
	if equalJSON(value, "null") || !fileJSONString(value) {
		return "", false, fileInvalidJSON(name + " must be a string")
	}
	var s string
	if err := json.Unmarshal(value, &s); err != nil {
		return "", false, fileInvalidJSON(name + " must be a string")
	}
	return s, true, nil
}

// fileOptionalBool validates an optional boolean member.
func fileOptionalBool(value jsontext.Value, name string) (bool, *apierr.Error) {
	value = trimJSON(value)
	if len(value) == 0 {
		return false, nil
	}
	if !equalJSON(value, "true") && !equalJSON(value, "false") {
		return false, fileInvalidJSON(name + " must be a boolean")
	}
	return equalJSON(value, "true"), nil
}

// fileOptionalEncoding validates an optional encoding member.
func fileOptionalEncoding(value jsontext.Value) (FileEncoding, *apierr.Error) {
	raw, ok, apiErr := fileOptionalString(value, "encoding")
	if apiErr != nil {
		return "", apiErr
	}
	if !ok {
		return FileEncodingUTF8, nil
	}
	encoding := FileEncoding(raw)
	if !encoding.valid() {
		return "", fileInvalidJSON("encoding must be utf8 or base64")
	}
	return encoding, nil
}

// fileDecodeContent converts the request content to bytes. Invalid UTF-8 text
// and malformed base64 are invalid_json; a decoded payload above the inline
// limit is the pinned 413.
func fileDecodeContent(request FileWriteRequest) ([]byte, *apierr.Error) {
	var data []byte
	switch request.Encoding {
	case FileEncodingBase64:
		decoded, err := base64.StdEncoding.DecodeString(request.Content)
		if err != nil {
			return nil, fileInvalidJSON("content is not valid base64")
		}
		data = decoded
	case FileEncodingUTF8:
		if !utf8.ValidString(request.Content) {
			return nil, fileInvalidJSON("content is not valid UTF-8")
		}
		data = []byte(request.Content)
	default:
		return nil, fileInvalidJSON("encoding must be utf8 or base64")
	}
	if int64(len(data)) > FileInlineLimitBytes {
		return nil, NewFileTooLargeError(FileInlineLimitBytes, int64(len(data)), true)
	}
	return data, nil
}

// trimJSON trims surrounding JSON whitespace.
func trimJSON(value jsontext.Value) jsontext.Value {
	return bytes.TrimSpace(value)
}

// equalJSON reports whether value equals the literal expected.
func equalJSON(value jsontext.Value, expected string) bool {
	return string(value) == expected
}
