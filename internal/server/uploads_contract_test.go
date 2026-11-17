package server

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
	"github.com/nathabonfim59/bamboo-raft/internal/apierr"
	"github.com/nathabonfim59/bamboo-raft/internal/compat"
)

func TestUploadReceiverExtensionSpec(t *testing.T) {
	validator := compat.LoadFile(t, filepath.Join(compat.Root(t), "api", "bbraft-ext.yaml"))
	success := httptest.NewRecorder()
	apierr.Write(success, http.StatusOK, api.TypeFileUploaded, api.UploadedFile{Path: "/home/user/file.txt", Size: 3})
	if err := validator.Response("bbraftUploadReceiver", http.StatusOK, success.Header(), success.Body.Bytes()); err != nil {
		t.Error("upload success failed the extension spec")
	}
	missing := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPut, "/api/bbraft/uploads/bad", nil)
	apierr.WriteError(missing, request, apierr.New(http.StatusNotFound, apierr.CodeNotFound, "not found"))
	if err := validator.Response("bbraftUploadReceiver", http.StatusNotFound, missing.Header(), missing.Body.Bytes()); err != nil {
		t.Error("upload 404 failed the extension spec")
	}
}
