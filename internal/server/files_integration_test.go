package server

import (
	"bytes"
	"encoding/base64"
	"encoding/json/v2"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
	"github.com/nathabonfim59/bamboo-raft/internal/compat"
	"github.com/nathabonfim59/bamboo-raft/internal/server/config"
)

// fileIntegrationExpect asserts the actual status before decoding the body,
// then validates the complete response against the v1 API document and the
// published subset.
func fileIntegrationExpect(t *testing.T, v1 *compat.Validator, operationID string, response runResponse, wantStatus int) {
	t.Helper()
	if response.status != wantStatus {
		if operationID == "createDownloadUrl" {
			// The success body carries a live signed URL, which must never
			// reach test output.
			t.Fatalf("%s status = %d, want %d", operationID, response.status, wantStatus)
		}
		t.Fatalf("%s status = %d, want %d; body: %s", operationID, response.status, wantStatus, response.body)
	}
	runTestCheck(t, v1, operationID, response)
}

// fileIntegrationExpectExtension asserts and validates one public signed
// receiver response against the extension document.
func fileIntegrationExpectExtension(t *testing.T, extension *compat.Validator, operationID string, response runResponse, wantStatus int) {
	t.Helper()
	if response.status != wantStatus {
		// The receiver body may be a large file; report the status only.
		t.Fatalf("%s status = %d, want %d", operationID, response.status, wantStatus)
	}
	if err := extension.Response(operationID, response.status, response.header, response.body); err != nil {
		t.Errorf("%s %d failed extension spec validation: %v", operationID, response.status, err)
	}
}

// fileIntegrationWriteDisk writes a file directly into nodefake's guest tree.
// It is test setup for boundary sizes that the 5 MiB inline route cannot write;
// every assertion still goes through the real HTTP API.
func fileIntegrationWriteDisk(t *testing.T, env *runEnv, id, relative string, data []byte) {
	t.Helper()
	path := filepath.Join(env.nodeRoot, "vms", id, "home", "user", filepath.FromSlash(relative))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("create guest directory: %v", err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write guest file: %v", err)
	}
}

// fileIntegrationGet performs one raw GET, used for the public receiver where
// no bearer header and no JSON body are involved.
func fileIntegrationGet(t *testing.T, env *runEnv, rawURL string) runResponse {
	t.Helper()
	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, rawURL, nil)
	if err != nil {
		// rawURL is a signed credential; never echo it or an error that may
		// embed it.
		t.Fatal("build receiver request failed")
	}
	response, err := env.client.Do(request)
	if err != nil {
		// Report only the concrete error type; the error text may include the
		// signed URL.
		t.Fatalf("receiver request failed (%T)", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read receiver body: %v", err)
	}
	return runResponse{status: response.StatusCode, header: response.Header.Clone(), body: body}
}

// TestRunFilesAndDownloads exercises the real API, Postgres and nodefake
// routes for all eight core file operations and the signed download URL.
// It requires BBRAFT_TEST_DATABASE_URL; without it dbtest skips through
// newRunEnv.
func TestRunFilesAndDownloads(t *testing.T) {
	if testing.Short() {
		t.Skip("files integration allocates the 5 MiB boundary fixtures")
	}
	env := newRunEnv(t)
	v1, extension := runTestValidators(t)
	env.start(config.Roles{config.RoleAPI: true, config.RoleLifecycle: true, config.RoleNodeHub: true})
	env.startNode(0)
	env.waitNodeFresh()
	env.waitAPI()
	id := env.createSandbox(`{"type":"small"}`)
	env.waitReady(id)
	base := "/api/v1/sandboxes/" + id + "/files"

	// Write, read and metadata round trip.
	written := env.do(http.MethodPut, base, env.key, `{"path":"notes/hello.txt","content":"hello\nworld\n"}`, nil)
	fileIntegrationExpect(t, v1, "writeFile", written, http.StatusOK)
	var writeBody api.FileWriteResponse
	if err := json.Unmarshal(written.body, &writeBody); err != nil {
		t.Fatalf("write response is malformed: %v", err)
	}
	if !writeBody.Success || writeBody.Path != "/home/user/notes/hello.txt" || writeBody.Encoding != api.FileEncodingUTF8 || writeBody.Size != 12 {
		t.Fatalf("write response = %+v", writeBody)
	}

	read := env.do(http.MethodGet, "/api/v1/sandboxes/"+id+"/files?path=notes/hello.txt", env.key, "", nil)
	fileIntegrationExpect(t, v1, "readFile", read, http.StatusOK)
	var readBody api.FileReadResponse
	if err := json.Unmarshal(read.body, &readBody); err != nil {
		t.Fatalf("read response is malformed: %v", err)
	}
	if readBody.Content != "hello\nworld\n" || readBody.Size != 12 || readBody.Encoding != api.FileEncodingUTF8 {
		t.Fatalf("read response = %+v", readBody)
	}

	encodedBody := `{"path":"notes/binary.bin","content":"AAEC/w==","encoding":"base64"}`
	encoded := env.do(http.MethodPut, base, env.key, encodedBody, nil)
	fileIntegrationExpect(t, v1, "writeFile", encoded, http.StatusOK)
	readEncoded := env.do(http.MethodGet, "/api/v1/sandboxes/"+id+"/files?path=notes/binary.bin&encoding=base64", env.key, "", nil)
	fileIntegrationExpect(t, v1, "readFile", readEncoded, http.StatusOK)
	var encodedRead api.FileReadResponse
	if err := json.Unmarshal(readEncoded.body, &encodedRead); err != nil {
		t.Fatalf("base64 read response is malformed: %v", err)
	}
	if encodedRead.Encoding != api.FileEncodingBase64 || encodedRead.Content != "AAEC/w==" {
		t.Fatalf("base64 read response = %+v", encodedRead)
	}

	info := env.do(http.MethodGet, "/api/v1/sandboxes/"+id+"/files/info?path=notes/hello.txt", env.key, "", nil)
	fileIntegrationExpect(t, v1, "getFileInfo", info, http.StatusOK)
	var infoBody api.FileInfoResponse
	if err := json.Unmarshal(info.body, &infoBody); err != nil {
		t.Fatalf("info response is malformed: %v", err)
	}
	if infoBody.Path != "/home/user/notes/hello.txt" || infoBody.Name != "hello.txt" || infoBody.IsDir || infoBody.Size != 12 {
		t.Fatalf("info response = %+v", infoBody)
	}

	list := env.do(http.MethodGet, "/api/v1/sandboxes/"+id+"/files/list?path=notes", env.key, "", nil)
	fileIntegrationExpect(t, v1, "listFiles", list, http.StatusOK)
	var listBody api.FileListResponse
	if err := json.Unmarshal(list.body, &listBody); err != nil {
		t.Fatalf("list response is malformed: %v", err)
	}
	if listBody.Path != "/home/user/notes" || len(listBody.Files) < 2 {
		t.Fatalf("list response = %+v", listBody)
	}

	folder := env.do(http.MethodPost, "/api/v1/sandboxes/"+id+"/files/folder", env.key, `{"path":"work","mode":"0700"}`, nil)
	fileIntegrationExpect(t, v1, "createFolder", folder, http.StatusOK)
	var folderBody api.FileInfoResponse
	if err := json.Unmarshal(folder.body, &folderBody); err != nil {
		t.Fatalf("folder response is malformed: %v", err)
	}
	if !folderBody.IsDir || folderBody.Mode != "0700" {
		t.Fatalf("folder response = %+v", folderBody)
	}

	// The approved explicit-0000 protocol: an existing directory keeps its
	// mode when created again with another mode.
	zero := env.do(http.MethodPost, "/api/v1/sandboxes/"+id+"/files/folder", env.key, `{"path":"locked","mode":"0000"}`, nil)
	fileIntegrationExpect(t, v1, "createFolder", zero, http.StatusOK)
	var zeroBody api.FileInfoResponse
	if err := json.Unmarshal(zero.body, &zeroBody); err != nil || zeroBody.Mode != "0000" {
		t.Fatalf("explicit 0000 folder = %+v, %v", zeroBody, err)
	}
	again := env.do(http.MethodPost, "/api/v1/sandboxes/"+id+"/files/folder", env.key, `{"path":"locked","mode":"0755"}`, nil)
	fileIntegrationExpect(t, v1, "createFolder", again, http.StatusOK)
	var againBody api.FileInfoResponse
	if err := json.Unmarshal(again.body, &againBody); err != nil || againBody.Mode != "0000" {
		t.Fatalf("existing 0000 folder changed mode: %+v, %v", againBody, err)
	}

	moved := env.do(http.MethodPost, "/api/v1/sandboxes/"+id+"/files/move", env.key, `{"source":"notes/hello.txt","destination":"work/hello.txt"}`, nil)
	fileIntegrationExpect(t, v1, "moveFile", moved, http.StatusOK)
	var moveBody api.FileMoveResponse
	if err := json.Unmarshal(moved.body, &moveBody); err != nil || moveBody.Path != "/home/user/notes/hello.txt" || moveBody.Destination != "/home/user/work/hello.txt" {
		t.Fatalf("move response = %+v, %v", moveBody, err)
	}

	chmod := env.do(http.MethodPost, "/api/v1/sandboxes/"+id+"/files/permissions", env.key, `{"path":"work/hello.txt","mode":"0640"}`, nil)
	fileIntegrationExpect(t, v1, "setFilePermissions", chmod, http.StatusOK)
	var chmodBody api.FileInfoResponse
	if err := json.Unmarshal(chmod.body, &chmodBody); err != nil || chmodBody.Mode != "0640" {
		t.Fatalf("permissions response = %+v, %v", chmodBody, err)
	}

	// Seed a nested child so the recursive delete below is not deleting an
	// already-empty directory.
	nestedFolder := env.do(http.MethodPost, "/api/v1/sandboxes/"+id+"/files/folder", env.key, `{"path":"work/nested"}`, nil)
	fileIntegrationExpect(t, v1, "createFolder", nestedFolder, http.StatusOK)
	nestedWrite := env.do(http.MethodPut, base, env.key, `{"path":"work/nested/child.txt","content":"nested child\n"}`, nil)
	fileIntegrationExpect(t, v1, "writeFile", nestedWrite, http.StatusOK)

	// Documented 400s and malformed bodies.
	invalidPath := env.do(http.MethodGet, "/api/v1/sandboxes/"+id+"/files?path=%2Fetc%2Fpasswd", env.key, "", nil)
	fileIntegrationExpect(t, v1, "readFile", invalidPath, http.StatusBadRequest)
	invalidQuery := env.do(http.MethodGet, "/api/v1/sandboxes/"+id+"/files?path=notes%2Fhello.txt&encoding=hex", env.key, "", nil)
	fileIntegrationExpect(t, v1, "readFile", invalidQuery, http.StatusBadRequest)
	invalidJSON := env.do(http.MethodPost, "/api/v1/sandboxes/"+id+"/files/folder", env.key, `{"mode":"0755"}`, nil)
	fileIntegrationExpect(t, v1, "createFolder", invalidJSON, http.StatusBadRequest)

	// Tenant and authentication boundaries.
	unauthorized := env.do(http.MethodGet, "/api/v1/sandboxes/"+id+"/files/info?path=work/hello.txt", "", "", nil)
	fileIntegrationExpect(t, v1, "getFileInfo", unauthorized, http.StatusUnauthorized)
	otherOwner := env.do(http.MethodGet, "/api/v1/sandboxes/"+id+"/files/info?path=work/hello.txt", env.otherKey, "", nil)
	fileIntegrationExpect(t, v1, "getFileInfo", otherOwner, http.StatusNotFound)
	missingSandbox := env.do(http.MethodGet, "/api/v1/sandboxes/bx_22222222/files/info?path=x", env.key, "", nil)
	fileIntegrationExpect(t, v1, "getFileInfo", missingSandbox, http.StatusNotFound)

	// The 5 MiB inline boundary. The exact write goes through the real HTTP
	// route and both the write and read successes validate against the pin and
	// the subset.
	inline := bytes.Repeat([]byte{'i'}, int(api.FileInlineLimitBytes))
	inlineBase64 := base64.StdEncoding.EncodeToString(inline)
	exactWriteBody := `{"path":"inline.bin","content":"` + inlineBase64 + `","encoding":"base64"}`
	exactWrite := env.do(http.MethodPut, base, env.key, exactWriteBody, nil)
	fileIntegrationExpect(t, v1, "writeFile", exactWrite, http.StatusOK)
	var exactWriteBodyResponse api.FileWriteResponse
	if err := json.Unmarshal(exactWrite.body, &exactWriteBodyResponse); err != nil {
		t.Fatalf("exactly 5 MiB write response is malformed: %v", err)
	}
	if exactWriteBodyResponse.Size != api.FileInlineLimitBytes || exactWriteBodyResponse.Encoding != api.FileEncodingBase64 {
		t.Fatalf("exactly 5 MiB write response = %+v", exactWriteBodyResponse)
	}

	exact := env.do(http.MethodGet, "/api/v1/sandboxes/"+id+"/files?path=inline.bin&encoding=base64", env.key, "", nil)
	fileIntegrationExpect(t, v1, "readFile", exact, http.StatusOK)
	var exactBody api.FileReadResponse
	if err := json.Unmarshal(exact.body, &exactBody); err != nil {
		t.Fatalf("exactly 5 MiB read response is malformed: %v", err)
	}
	if exactBody.Size != api.FileInlineLimitBytes || exactBody.Content != inlineBase64 {
		t.Fatalf("exactly 5 MiB read = size %d, content match %v", exactBody.Size, exactBody.Content == inlineBase64)
	}

	tooBig := bytes.Repeat([]byte{'i'}, int(api.FileInlineLimitBytes)+1)
	fileIntegrationWriteDisk(t, env, id, "too-big.bin", tooBig)
	readTooBig := env.do(http.MethodGet, "/api/v1/sandboxes/"+id+"/files?path=too-big.bin", env.key, "", nil)
	fileIntegrationExpect(t, v1, "readFile", readTooBig, http.StatusRequestEntityTooLarge)

	tooBigBase64 := base64.StdEncoding.EncodeToString(tooBig)
	writeTooBigBody := `{"path":"too-big-write.bin","content":"` + tooBigBase64 + `","encoding":"base64"}`
	writeTooBig := env.do(http.MethodPut, base, env.key, writeTooBigBody, nil)
	fileIntegrationExpect(t, v1, "writeFile", writeTooBig, http.StatusRequestEntityTooLarge)

	// Signed download URL: issuance, public receiver, headers, tampering and
	// the wrong method.
	downloadFile := []byte("signed download bytes")
	fileIntegrationWriteDisk(t, env, id, "download.txt", downloadFile)
	issued := env.do(http.MethodPost, "/api/v1/sandboxes/"+id+"/files/download-url", env.key, `{"path":"download.txt","ttlSeconds":60}`, nil)
	fileIntegrationExpect(t, v1, "createDownloadUrl", issued, http.StatusOK)
	var issuedBody api.FileURLResponse
	if err := json.Unmarshal(issued.body, &issuedBody); err != nil {
		t.Fatalf("download URL response is malformed: %v", err)
	}
	if issuedBody.Method != api.FileMethodGet || issuedBody.Path != "/home/user/download.txt" ||
		!strings.HasPrefix(issuedBody.URL, env.cfg.Server.PublicURL+"/api/bbraft/downloads/") {
		// Do not print the response: its URL is a live credential.
		t.Fatalf("download URL response has method %q, path %q or an untrusted URL", issuedBody.Method, issuedBody.Path)
	}
	received := fileIntegrationGet(t, env, issuedBody.URL)
	fileIntegrationExpectExtension(t, extension, "bbraftDownloadReceiver", received, http.StatusOK)
	if !bytes.Equal(received.body, downloadFile) {
		t.Fatal("downloaded bytes differ from the file")
	}
	if ct := received.header.Get("Content-Type"); !strings.HasPrefix(ct, "application/octet-stream") {
		t.Fatalf("download Content-Type = %q", ct)
	}
	if disposition := received.header.Get("Content-Disposition"); !strings.Contains(disposition, "attachment") || !strings.Contains(disposition, "download.txt") {
		t.Fatalf("download Content-Disposition = %q", disposition)
	}
	if cache := received.header.Get("Cache-Control"); cache != "no-store" {
		t.Fatalf("download Cache-Control = %q", cache)
	}

	tampered := issuedBody.URL[:len(issuedBody.URL)-1]
	if strings.HasSuffix(issuedBody.URL, "A") {
		tampered += "B"
	} else {
		tampered += "A"
	}
	fileIntegrationExpectExtension(t, extension, "bbraftDownloadReceiver", fileIntegrationGet(t, env, tampered), http.StatusNotFound)
	fileIntegrationExpectExtension(t, extension, "bbraftDownloadReceiver", env.do(http.MethodPost, "/api/bbraft/downloads/not-a-token", "", "", nil), http.StatusMethodNotAllowed)

	// A directory is InvalidArgument at the node and therefore a 400 before
	// any response header.
	folderURL := env.do(http.MethodPost, "/api/v1/sandboxes/"+id+"/files/download-url", env.key, `{"path":"work","ttlSeconds":60}`, nil)
	fileIntegrationExpect(t, v1, "createDownloadUrl", folderURL, http.StatusOK)
	var folderURLBody api.FileURLResponse
	if err := json.Unmarshal(folderURL.body, &folderURLBody); err != nil {
		t.Fatalf("folder download URL is malformed: %v", err)
	}
	fileIntegrationExpectExtension(t, extension, "bbraftDownloadReceiver", fileIntegrationGet(t, env, folderURLBody.URL), http.StatusBadRequest)

	// Issuance errors share the declared statuses and the standing 500 row.
	noBearer := env.do(http.MethodPost, "/api/v1/sandboxes/"+id+"/files/download-url", "", `{"path":"download.txt"}`, nil)
	fileIntegrationExpect(t, v1, "createDownloadUrl", noBearer, http.StatusUnauthorized)
	foreign := env.do(http.MethodPost, "/api/v1/sandboxes/"+id+"/files/download-url", env.otherKey, `{"path":"download.txt"}`, nil)
	fileIntegrationExpect(t, v1, "createDownloadUrl", foreign, http.StatusNotFound)
	badIssue := env.do(http.MethodPost, "/api/v1/sandboxes/"+id+"/files/download-url", env.key, `{"path":"/etc/passwd"}`, nil)
	fileIntegrationExpect(t, v1, "createDownloadUrl", badIssue, http.StatusBadRequest)

	// A signed URL issued before the stop observes the archived state on
	// redemption and answers 409 without touching a node.
	stopURL := env.do(http.MethodPost, "/api/v1/sandboxes/"+id+"/files/download-url", env.key, `{"path":"download.txt","ttlSeconds":60}`, nil)
	fileIntegrationExpect(t, v1, "createDownloadUrl", stopURL, http.StatusOK)
	var stopURLBody api.FileURLResponse
	if err := json.Unmarshal(stopURL.body, &stopURLBody); err != nil {
		t.Fatalf("stop download URL is malformed: %v", err)
	}

	// Stop, assert the archived 409, resume and prove the file survived.
	stopped := env.do(http.MethodPost, "/api/v1/sandboxes/"+id+"/stop", env.key, `{}`, nil)
	if stopped.status != http.StatusAccepted {
		t.Fatalf("stop status = %d; body: %s", stopped.status, stopped.body)
	}
	env.lifecycleRunWaitState(id, "archived", 30*time.Second)
	archivedInfo := env.do(http.MethodGet, "/api/v1/sandboxes/"+id+"/files/info?path=work/hello.txt", env.key, "", nil)
	fileIntegrationExpect(t, v1, "getFileInfo", archivedInfo, http.StatusConflict)
	fileIntegrationExpectExtension(t, extension, "bbraftDownloadReceiver", fileIntegrationGet(t, env, stopURLBody.URL), http.StatusConflict)

	resumed := env.do(http.MethodPost, "/api/v1/sandboxes/"+id+"/resume", env.key, `{}`, nil)
	if resumed.status != http.StatusAccepted {
		t.Fatalf("resume status = %d; body: %s", resumed.status, resumed.body)
	}
	env.waitReady(id)
	persisted := env.do(http.MethodGet, "/api/v1/sandboxes/"+id+"/files/info?path=work/hello.txt", env.key, "", nil)
	fileIntegrationExpect(t, v1, "getFileInfo", persisted, http.StatusOK)
	persistedRead := env.do(http.MethodGet, "/api/v1/sandboxes/"+id+"/files?path=work/hello.txt", env.key, "", nil)
	fileIntegrationExpect(t, v1, "readFile", persistedRead, http.StatusOK)
	var persistedBody api.FileReadResponse
	if err := json.Unmarshal(persistedRead.body, &persistedBody); err != nil || persistedBody.Content != "hello\nworld\n" {
		t.Fatalf("file after resume = %+v, %v", persistedBody, err)
	}

	// A 6 MiB file exercises the signed receiver beyond the inline cap. It is
	// written directly into nodefake's guest tree because the inline write
	// route correctly refuses it.
	big := bytes.Repeat([]byte("big-file-"), (6<<20)/len("big-file-")+1)
	big = big[:6<<20]
	fileIntegrationWriteDisk(t, env, id, "download-big.bin", big)
	bigURL := env.do(http.MethodPost, "/api/v1/sandboxes/"+id+"/files/download-url", env.key, `{"path":"download-big.bin","ttlSeconds":60}`, nil)
	fileIntegrationExpect(t, v1, "createDownloadUrl", bigURL, http.StatusOK)
	var bigURLBody api.FileURLResponse
	if err := json.Unmarshal(bigURL.body, &bigURLBody); err != nil {
		t.Fatalf("big download URL is malformed: %v", err)
	}
	bigDownload := fileIntegrationGet(t, env, bigURLBody.URL)
	fileIntegrationExpectExtension(t, extension, "bbraftDownloadReceiver", bigDownload, http.StatusOK)
	if !bytes.Equal(bigDownload.body, big) {
		t.Fatalf("big download length = %d, want %d", len(bigDownload.body), len(big))
	}

	// Delete a file and a non-empty directory, then delete the sandbox and
	// prove the route is owner-scoped missing afterwards.
	deleted := env.do(http.MethodDelete, "/api/v1/sandboxes/"+id+"/files?path=work/hello.txt", env.key, "", nil)
	fileIntegrationExpect(t, v1, "deleteFile", deleted, http.StatusOK)
	var deletedBody api.FileDeleteResponse
	if err := json.Unmarshal(deleted.body, &deletedBody); err != nil || !deletedBody.Success || deletedBody.Path != "/home/user/work/hello.txt" {
		t.Fatalf("delete response = %+v, %v", deletedBody, err)
	}
	// work still holds nested/child.txt, so this delete is genuinely
	// recursive.
	recursive := env.do(http.MethodDelete, "/api/v1/sandboxes/"+id+"/files?path=work&recursive=true", env.key, "", nil)
	fileIntegrationExpect(t, v1, "deleteFile", recursive, http.StatusOK)
	var recursiveBody api.FileDeleteResponse
	if err := json.Unmarshal(recursive.body, &recursiveBody); err != nil || !recursiveBody.Success || recursiveBody.Path != "/home/user/work" {
		t.Fatalf("recursive delete response = %+v, %v", recursiveBody, err)
	}
	nestedGone := env.do(http.MethodGet, "/api/v1/sandboxes/"+id+"/files/info?path=work/nested/child.txt", env.key, "", nil)
	fileIntegrationExpect(t, v1, "getFileInfo", nestedGone, http.StatusNotFound)
	rootDelete := env.do(http.MethodDelete, "/api/v1/sandboxes/"+id+"/files?path=%2Fhome%2Fuser&recursive=true", env.key, "", nil)
	fileIntegrationExpect(t, v1, "deleteFile", rootDelete, http.StatusBadRequest)

	operation, deletedSandbox := env.deleteSandbox(id)
	runTestCheck(t, v1, "deleteSandbox", deletedSandbox)
	completed := env.waitDeletionComplete(operation)
	runTestCheck(t, v1, "getDeletionOperation", completed)
	missing := env.do(http.MethodGet, "/api/v1/sandboxes/"+id+"/files/info?path=x", env.key, "", nil)
	fileIntegrationExpect(t, v1, "getFileInfo", missing, http.StatusNotFound)
}
