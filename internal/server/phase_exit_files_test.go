package server_test

// Representative phase-exit file and signed-download coverage. The dedicated
// handler suites already exercise every error edge; this flow proves the same
// public routes through the compiled server: folder/file write, read, info,
// list, permissions, move, a signed download with no bearer, and recursive
// delete.

import (
	"bytes"
	"io"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
)

// files exercises one concise, successful core-file and download round trip.
func (h *phaseExitHarness) files(t *testing.T, id string) {
	t.Helper()
	base := "/api/v1/sandboxes/" + id + "/files"
	folder := "/home/user/phase-exit"
	source := folder + "/hello.txt"
	moved := folder + "/hello-moved.txt"
	content := "phase exit file\n"

	folderResponse := h.apiOK(t, http.MethodPost, base+"/folder",
		phaseExitMarshal(t, api.FileFolderRequest{Path: folder, Mode: "0755"}), http.StatusOK)
	var created api.FileInfoResponse
	e2eDecode(t, "create folder", folderResponse.body, &created)
	if !created.Success || created.Path != folder || !created.IsDir || created.Mode != "0755" {
		t.Fatal("create folder response differs")
	}

	writeResponse := h.apiOK(t, http.MethodPut, base,
		phaseExitMarshal(t, api.FileWriteRequest{Path: source, Content: content, Encoding: api.FileEncodingUTF8}), http.StatusOK)
	var written api.FileWriteResponse
	e2eDecode(t, "write file", writeResponse.body, &written)
	if !written.Success || written.Path != source || written.Size != int64(len(content)) {
		t.Fatal("write file response differs")
	}

	query := url.Values{"path": {source}}
	readResponse := h.apiOK(t, http.MethodGet, base+"?"+query.Encode(), nil, http.StatusOK)
	var read api.FileReadResponse
	e2eDecode(t, "read file", readResponse.body, &read)
	if !read.Success || read.Path != source || read.Size != int64(len(content)) ||
		read.Encoding != api.FileEncodingUTF8 || read.Content != content {
		t.Fatal("read file response differs")
	}

	infoResponse := h.apiOK(t, http.MethodGet, base+"/info?"+query.Encode(), nil, http.StatusOK)
	var info api.FileInfoResponse
	e2eDecode(t, "file info", infoResponse.body, &info)
	if !info.Success || info.Path != source || info.IsDir || info.Size != int64(len(content)) || info.Mode != "0644" {
		t.Fatal("file info response differs")
	}

	listQuery := url.Values{"path": {folder}}
	listResponse := h.apiOK(t, http.MethodGet, base+"/list?"+listQuery.Encode(), nil, http.StatusOK)
	var list api.FileListResponse
	e2eDecode(t, "list files", listResponse.body, &list)
	if !list.Success || list.Path != folder || len(list.Files) != 1 ||
		list.Files[0].Name != "hello.txt" || list.Truncated {
		t.Fatal("list files response differs")
	}

	permissionsResponse := h.apiOK(t, http.MethodPost, base+"/permissions",
		phaseExitMarshal(t, api.FilePermissionsRequest{Path: source, Mode: "0640"}), http.StatusOK)
	var permissions api.FileInfoResponse
	e2eDecode(t, "file permissions", permissionsResponse.body, &permissions)
	if !permissions.Success || permissions.Path != source || permissions.Mode != "0640" {
		t.Fatal("file permissions response differs")
	}

	moveResponse := h.apiOK(t, http.MethodPost, base+"/move",
		phaseExitMarshal(t, api.FileMoveRequest{Source: source, Destination: moved}), http.StatusOK)
	var movedResponse api.FileMoveResponse
	e2eDecode(t, "move file", moveResponse.body, &movedResponse)
	if !movedResponse.Success || movedResponse.Path != source || movedResponse.Destination != moved {
		t.Fatal("move file response differs")
	}

	h.download(t, id, moved, content)

	deleteQuery := url.Values{"path": {folder}, "recursive": {"true"}}
	deleteResponse := h.apiOK(t, http.MethodDelete, base+"?"+deleteQuery.Encode(), nil, http.StatusOK)
	var deleted api.FileDeleteResponse
	e2eDecode(t, "delete file", deleteResponse.body, &deleted)
	if !deleted.Success || deleted.Path != folder {
		t.Fatal("delete file response differs")
	}
}

// download issues a signed URL and fetches it without the bearer credential.
func (h *phaseExitHarness) download(t *testing.T, id, path, want string) {
	t.Helper()
	response := h.apiOK(t, http.MethodPost, "/api/v1/sandboxes/"+id+"/files/download-url",
		phaseExitMarshal(t, api.FileURLRequest{Path: path}), http.StatusOK)
	var issued api.FileURLResponse
	e2eDecode(t, "download URL", response.body, &issued)
	if issued.Path != path || issued.Method != api.FileMethodGet || issued.URL == "" ||
		!issued.ExpiresAt.After(time.Now()) {
		t.Fatal("download URL response differs")
	}

	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, issued.URL, nil)
	if err != nil {
		t.Fatal("build signed download request failed")
	}
	downloaded, err := h.fixture.httpClient.Do(request)
	if err != nil {
		t.Fatal("signed download request failed")
	}
	defer downloaded.Body.Close()
	body, err := io.ReadAll(io.LimitReader(downloaded.Body, 1<<20))
	if err != nil {
		t.Fatal("signed download body read failed")
	}
	if downloaded.StatusCode != http.StatusOK || !bytes.Equal(body, []byte(want)) {
		t.Fatal("signed download body differs")
	}
	if contentType := downloaded.Header.Get("Content-Type"); contentType != "application/octet-stream" {
		t.Fatalf("signed download content type = %q", contentType)
	}
	if disposition := downloaded.Header.Get("Content-Disposition"); disposition == "" ||
		!bytes.Contains([]byte(disposition), []byte("attachment")) ||
		!bytes.Contains([]byte(disposition), []byte("hello-moved.txt")) {
		t.Fatal("signed download disposition differs")
	}
}
