package server

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/nathabonfim59/bamboo-raft/internal/apierr"
	"github.com/nathabonfim59/bamboo-raft/internal/compat"
)

// TestDownloadReceiverExtensionSpec validates the public signed-download
// receiver shapes against api/bbraft-ext.yaml independently of bearer auth.
func TestDownloadReceiverExtensionSpec(t *testing.T) {
	validator := compat.LoadFile(t, filepath.Join(compat.Root(t), "api", "bbraft-ext.yaml"))

	success := httptest.NewRecorder()
	success.Header().Set("Content-Type", "application/octet-stream")
	success.WriteHeader(http.StatusOK)
	if _, err := success.Write([]byte("raw file bytes")); err != nil {
		t.Fatalf("write success body: %v", err)
	}
	if err := validator.Response("bbraftDownloadReceiver", http.StatusOK, success.Header(), success.Body.Bytes()); err != nil {
		t.Errorf("download success failed the extension spec: %v", err)
	}

	for _, test := range []struct {
		status int
		code   string
	}{
		{http.StatusBadRequest, "invalid_path"},
		{http.StatusNotFound, "not_found"},
		{http.StatusMethodNotAllowed, "method_not_allowed"},
		{http.StatusConflict, "sandbox_not_ready"},
		{http.StatusInternalServerError, "http_500"},
		{http.StatusBadGateway, "sandbox_direct_failed"},
	} {
		t.Run(test.code, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodGet, "/api/bbraft/downloads/bad", nil)
			apierr.WriteError(recorder, request, apierr.New(test.status, test.code, "message"))
			if err := validator.Response("bbraftDownloadReceiver", test.status, recorder.Header(), recorder.Body.Bytes()); err != nil {
				t.Errorf("download %d %s failed the extension spec: %v", test.status, test.code, err)
			}
		})
	}
}

// TestUploadReceiverMethodNotAllowedSpec covers the additive 405 row of the
// existing upload receiver. It does not change any upload handler behavior.
func TestUploadReceiverMethodNotAllowedSpec(t *testing.T) {
	validator := compat.LoadFile(t, filepath.Join(compat.Root(t), "api", "bbraft-ext.yaml"))
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/api/bbraft/uploads/bad", nil)
	apierr.WriteError(recorder, request, apierr.New(http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed"))
	if err := validator.Response("bbraftUploadReceiver", http.StatusMethodNotAllowed, recorder.Header(), recorder.Body.Bytes()); err != nil {
		t.Errorf("upload 405 failed the extension spec: %v", err)
	}
}
