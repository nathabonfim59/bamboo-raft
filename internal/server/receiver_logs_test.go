package server

import (
	"bytes"
	"encoding/json/v2"
	"net/http"
	"strings"
	"testing"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
	"github.com/nathabonfim59/bamboo-raft/internal/server/config"
)

// receiverLogSentinels are recognizable fake tokens for the two public
// receiver routes. They are invalid on purpose: the rejected path must be as
// redacted as a successful one.
const (
	receiverLogUploadSentinel   = "receiverLogUploadToken7f3c9a1b5d2e4f60"
	receiverLogDownloadSentinel = "receiverLogDownloadToken1a2b3c4d5e6f7081"
)

// TestRunReceiverTokensNeverReachRoleLogs exercises the real API role over TCP
// with both public signed receivers, one issued success and one rejected token
// each. After every role has joined, no captured role log may contain a token
// or a full receiver path: an access logger added later must redact them.
// Failure output is a fixed classification only; the success tokens are live
// credentials and must never be printed.
func TestRunReceiverTokensNeverReachRoleLogs(t *testing.T) {
	env := newRunEnv(t)
	v1, extension := runTestValidators(t)
	stop := env.start(config.Roles{config.RoleAPI: true, config.RoleLifecycle: true, config.RoleNodeHub: true})
	env.startNode(0)
	env.waitNodeFresh()
	env.waitAPI()
	id := env.createSandbox(`{"type":"small"}`)
	env.waitReady(id)

	// Upload success: issue a signed URL through the bearer route, then use
	// the token as the only credential on the public receiver.
	issuedUpload := env.do(http.MethodPost, "/api/v1/sandboxes/"+id+"/files/upload-url", env.key,
		`{"path":"/home/user/receiver-log.txt","ttlSeconds":60}`, nil)
	if issuedUpload.status != http.StatusOK {
		t.Fatalf("upload issue status = %d", issuedUpload.status)
	}
	runTestCheck(t, v1, "createUploadUrl", issuedUpload)
	var uploadURL struct {
		URL string `json:"url"`
	}
	if err := json.Unmarshal(issuedUpload.body, &uploadURL); err != nil {
		t.Fatal("upload URL response is malformed")
	}
	uploadToken := strings.TrimPrefix(uploadURL.URL, env.cfg.Server.PublicURL+api.UploadReceiverPrefix+"/")
	if uploadToken == uploadURL.URL || uploadToken == "" {
		t.Fatal("upload URL does not use the trusted receiver base")
	}
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPut, uploadURL.URL,
		bytes.NewReader([]byte("receiver log fixture")))
	if err != nil {
		t.Fatal("could not build the upload receiver request")
	}
	request.Header.Set("Content-Type", "application/octet-stream")
	response, err := env.client.Do(request)
	if err != nil {
		t.Fatal("upload receiver request failed")
	}
	uploadSuccess := runResponse{status: response.StatusCode, header: response.Header.Clone()}
	body := new(bytes.Buffer)
	_, readErr := body.ReadFrom(response.Body)
	response.Body.Close()
	if readErr != nil {
		t.Fatal("upload receiver response could not be read")
	}
	uploadSuccess.body = body.Bytes()
	if uploadSuccess.status != http.StatusOK {
		t.Fatalf("upload receiver status = %d", uploadSuccess.status)
	}
	runTestCheck(t, extension, "bbraftUploadReceiver", uploadSuccess)

	// Upload rejection: the invalid sentinel is the whole path credential.
	rejectedUpload := env.do(http.MethodPut, api.UploadReceiverPrefix+"/"+receiverLogUploadSentinel, "",
		"", nil)
	if rejectedUpload.status != http.StatusNotFound {
		t.Fatalf("rejected upload status = %d", rejectedUpload.status)
	}

	// Download success: seed one guest file, issue a signed URL and use the
	// public receiver without a bearer header.
	fileIntegrationWriteDisk(t, env, id, "receiver-log.txt", []byte("receiver download fixture"))
	issuedDownload := env.do(http.MethodPost, "/api/v1/sandboxes/"+id+"/files/download-url", env.key,
		`{"path":"receiver-log.txt","ttlSeconds":60}`, nil)
	if issuedDownload.status != http.StatusOK {
		t.Fatalf("download issue status = %d", issuedDownload.status)
	}
	runTestCheck(t, v1, "createDownloadUrl", issuedDownload)
	var downloadURL struct {
		URL string `json:"url"`
	}
	if err := json.Unmarshal(issuedDownload.body, &downloadURL); err != nil {
		t.Fatal("download URL response is malformed")
	}
	downloadToken := strings.TrimPrefix(downloadURL.URL, env.cfg.Server.PublicURL+api.DownloadReceiverPrefix+"/")
	if downloadToken == downloadURL.URL || downloadToken == "" {
		t.Fatal("download URL does not use the trusted receiver base")
	}
	downloadSuccess := fileIntegrationGet(t, env, downloadURL.URL)
	if downloadSuccess.status != http.StatusOK {
		t.Fatalf("download receiver status = %d", downloadSuccess.status)
	}
	runTestCheck(t, extension, "bbraftDownloadReceiver", downloadSuccess)

	// Download rejection: the invalid sentinel is the whole path credential.
	rejectedDownload := fileIntegrationGet(t, env, env.apiBase+api.DownloadReceiverPrefix+"/"+receiverLogDownloadSentinel)
	if rejectedDownload.status != http.StatusNotFound {
		t.Fatalf("rejected download status = %d", rejectedDownload.status)
	}

	// Join every role (and therefore drain every handler) before reading the
	// synchronized log buffer. Never print the logs: they may hold live
	// credentials if a regression exists.
	stop()
	logs := env.logs.String()
	for _, test := range []struct {
		classification string
		secret         string
	}{
		{"upload-token", uploadToken},
		{"download-token", downloadToken},
		{"upload-sentinel", receiverLogUploadSentinel},
		{"download-sentinel", receiverLogDownloadSentinel},
		{"upload-path", api.UploadReceiverPrefix + "/" + uploadToken},
		{"download-path", api.DownloadReceiverPrefix + "/" + downloadToken},
	} {
		if strings.Contains(logs, test.secret) {
			t.Fatalf("role logs exposed a receiver credential (classification: %s)", test.classification)
		}
	}
}
