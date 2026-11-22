package compat

import (
	"net/http"
	"testing"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
)

func TestSpecOperations(t *testing.T) {
	subset := Load(t)
	if subset.OpenAPIVersion() != "3.1.0" {
		t.Errorf("subset openapi version = %q", subset.OpenAPIVersion())
	}
	if len(subset.Operations()) == 0 {
		t.Fatal("subset document has no operations")
	}
	for _, operationID := range []string{
		"me", "sandboxes", "create", "get", "deleteSandbox", "getDeletionOperation",
		"command", "createUploadUrl", "listSavedSnapshots", "saveSavedSnapshot",
		"getSavedSnapshot", "deleteSavedSnapshot",
		"update", "stop", "getStop", "resume", "fork",
		"readFile", "writeFile", "deleteFile", "listFiles", "getFileInfo",
		"createFolder", "moveFile", "setFilePermissions", "createDownloadUrl",
		"sshKey",
	} {
		if _, ok := subset.Operation(operationID); !ok {
			t.Errorf("subset document is missing operation %q", operationID)
		}
	}
}

// TestCoreRouteConstantsMatchSubset keeps internal/api's registered core paths
// and methods identical to the v1 API document.
func TestCoreRouteConstantsMatchSubset(t *testing.T) {
	subset := Load(t)
	routes := []struct {
		method string
		path   string
	}{
		{http.MethodGet, api.PathMe},
		{http.MethodGet, api.PathSandboxes},
		{http.MethodPost, api.PathSandboxes},
		{http.MethodGet, api.PathSandbox},
		{http.MethodDelete, api.PathSandbox},
		{http.MethodPatch, api.PathSandbox},
		{http.MethodGet, api.PathDeletionOperation},
		{http.MethodPost, api.PathSandboxStop},
		{http.MethodGet, api.PathSandboxStops},
		{http.MethodPost, api.PathSandboxResume},
		{http.MethodPost, api.PathSandboxFork},
	}
	for _, route := range routes {
		if !subset.HasOperation(route.method, route.path) {
			t.Errorf("%s %s is registered but absent from api/bbraft-v1.yaml", route.method, route.path)
		}
	}
}

// TestFileDownloadAndSSHRouteConstantsMatchSubset keeps the file, signed
// download and SSH route constants identical to the v1 API document.
func TestFileDownloadAndSSHRouteConstantsMatchSubset(t *testing.T) {
	subset := Load(t)
	routes := []struct {
		method string
		path   string
	}{
		{http.MethodGet, api.FilesPath},
		{http.MethodPut, api.FilesPath},
		{http.MethodDelete, api.FilesPath},
		{http.MethodGet, api.FilesListPath},
		{http.MethodGet, api.FilesInfoPath},
		{http.MethodPost, api.FilesFolderPath},
		{http.MethodPost, api.FilesMovePath},
		{http.MethodPost, api.FilesPermissionsPath},
		{http.MethodPost, api.DownloadsPath},
		{http.MethodPost, api.PathSandboxSSHKey},
	}
	for _, route := range routes {
		if !subset.HasOperation(route.method, route.path) {
			t.Errorf("%s %s is registered but absent from api/bbraft-v1.yaml", route.method, route.path)
		}
	}
}

// TestSubsetSpecAcceptsOurFixtures proves the document we publish describes
// the shapes our own handlers emit, not only the upstream ones.
func TestSubsetSpecAcceptsOurFixtures(t *testing.T) {
	subset := Load(t)
	tests := []struct {
		fixture     string
		operationID string
		status      int
		mediaType   string
	}{
		{"sandbox-list.json", "sandboxes", http.StatusOK, jsonMediaType},
		{"sandbox-info.json", "get", http.StatusOK, jsonMediaType},
		{"sandbox-created.json", "create", http.StatusAccepted, jsonMediaType},
		{"sandbox-deleting.json", "deleteSandbox", http.StatusAccepted, jsonMediaType},
		{"deletion-operation.json", "getDeletionOperation", http.StatusOK, jsonMediaType},
		{"me.json", "me", http.StatusOK, jsonMediaType},
		{"command-finished.json", "command", http.StatusOK, jsonMediaType},
		{"command-stopped.json", "stopCommand", http.StatusOK, jsonMediaType},
		{"saved-snapshot-list.json", "listSavedSnapshots", http.StatusOK, jsonMediaType},
		{"saved-snapshot-info.json", "getSavedSnapshot", http.StatusOK, jsonMediaType},
		{"saved-snapshot-saving.json", "saveSavedSnapshot", http.StatusAccepted, jsonMediaType},
		{"saved-snapshot-deleted.json", "deleteSavedSnapshot", http.StatusOK, jsonMediaType},
		{"file-upload-url.json", "createUploadUrl", http.StatusOK, jsonMediaType},
		{"file-download-url.json", "createDownloadUrl", http.StatusOK, jsonMediaType},
		{"ssh-key-configured.json", "sshKey", http.StatusOK, jsonMediaType},
		{"error-ssh-host-key-unavailable.json", "sshKey", http.StatusServiceUnavailable, jsonMediaType},
		{"file-read.json", "readFile", http.StatusOK, jsonMediaType},
		{"file-written.json", "writeFile", http.StatusOK, jsonMediaType},
		{"file-deleted.json", "deleteFile", http.StatusOK, jsonMediaType},
		{"file-list.json", "listFiles", http.StatusOK, jsonMediaType},
		{"file-info.json", "getFileInfo", http.StatusOK, jsonMediaType},
		{"folder-created.json", "createFolder", http.StatusOK, jsonMediaType},
		{"file-moved.json", "moveFile", http.StatusOK, jsonMediaType},
		{"file-permissions-set.json", "setFilePermissions", http.StatusOK, jsonMediaType},
		{"error-machine-not-running.json", "readFile", http.StatusConflict, jsonMediaType},
		{"sandbox-stopping.json", "stop", http.StatusAccepted, jsonMediaType},
		{"sandbox-stopped.json", "stop", http.StatusOK, jsonMediaType},
		{"stop-operation.json", "getStop", http.StatusOK, jsonMediaType},
		{"sandbox-resuming.json", "resume", http.StatusAccepted, jsonMediaType},
		{"sandbox-info-stopped.json", "get", http.StatusOK, jsonMediaType},
		{"sandbox-info-error.json", "get", http.StatusOK, jsonMediaType},
		{"error-sandbox-not-ready.json", "resume", http.StatusConflict, jsonMediaType},
		{"error-invalid-query.json", "sandboxes", http.StatusBadRequest, jsonMediaType},
		{"error-unauthorized.json", "me", http.StatusUnauthorized, jsonMediaType},
		{"error-not-found.json", "get", http.StatusNotFound, jsonMediaType},
		{"error-conflict.json", "create", http.StatusConflict, jsonMediaType},
		{"error-sandbox-starting.json", "command", http.StatusConflict, jsonMediaType},
		{"error-invalid-json.json", "create", http.StatusBadRequest, jsonMediaType},
		{"error-no-ready-machine.json", "create", http.StatusServiceUnavailable, jsonMediaType},
		{"error-sandbox-direct-failed.json", "command", http.StatusBadGateway, jsonMediaType},
		{"error-save-in-progress.json", "saveSavedSnapshot", http.StatusConflict, jsonMediaType},
	}
	for _, test := range tests {
		t.Run(test.fixture, func(t *testing.T) {
			err := subset.Response(test.operationID, test.status, mediaType(test.mediaType), fixture(t, test.fixture))
			if err != nil {
				t.Errorf("fixture does not validate against api/bbraft-v1.yaml: %v", err)
			}
		})
	}
	for _, name := range []string{"command-stream.ndjson", "command-stream-error.ndjson"} {
		t.Run(name, func(t *testing.T) {
			err := subset.NDJSON("command", http.StatusOK, mediaType(ndjsonMediaType), fixture(t, name))
			if err != nil {
				t.Errorf("stream does not validate against api/bbraft-v1.yaml: %v", err)
			}
		})
	}
	t.Run("command-attached.ndjson", func(t *testing.T) {
		if err := subset.NDJSON("attachCommand", http.StatusOK,
			mediaType(ndjsonMediaType), fixture(t, "command-attached.ndjson")); err != nil {
			t.Errorf("attach stream does not validate against api/bbraft-v1.yaml: %v", err)
		}
	})
}
