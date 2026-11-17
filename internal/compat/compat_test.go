package compat

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const (
	jsonMediaType   = "application/json"
	ndjsonMediaType = "application/x-ndjson"
)

func mediaType(value string) http.Header {
	return http.Header{"Content-Type": []string{value}}
}

func fixture(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("read fixture %s: %v", name, err)
	}
	return data
}

func TestPinnedSpecAndGoldenFixtures(t *testing.T) {
	validator := Load(t)
	if validator.OpenAPIVersion() != "3.1.0" {
		t.Fatalf("openapi version = %q, want 3.1.0", validator.OpenAPIVersion())
	}
	for _, operationID := range []string{
		"me", "sandboxes", "create", "get", "deleteSandbox", "getDeletionOperation",
		"command", "createUploadUrl", "listSavedSnapshots", "saveSavedSnapshot",
		"getSavedSnapshot", "deleteSavedSnapshot",
	} {
		if _, ok := validator.Operation(operationID); !ok {
			t.Errorf("operation %q missing from the v1 API document", operationID)
		}
	}

	tests := []struct {
		name        string
		fixture     string
		operationID string
		status      int
		mediaType   string
	}{
		{"sandbox list", "sandbox-list.json", "sandboxes", http.StatusOK, jsonMediaType},
		{"sandbox info", "sandbox-info.json", "get", http.StatusOK, jsonMediaType},
		{"sandbox created", "sandbox-created.json", "create", http.StatusAccepted, jsonMediaType},
		{"sandbox deleting", "sandbox-deleting.json", "deleteSandbox", http.StatusAccepted, jsonMediaType},
		{"deletion operation", "deletion-operation.json", "getDeletionOperation", http.StatusOK, jsonMediaType},
		{"me", "me.json", "me", http.StatusOK, jsonMediaType},
		{"command finished", "command-finished.json", "command", http.StatusOK, jsonMediaType},
		{"saved snapshot list", "saved-snapshot-list.json", "listSavedSnapshots", http.StatusOK, jsonMediaType},
		{"saved snapshot info", "saved-snapshot-info.json", "getSavedSnapshot", http.StatusOK, jsonMediaType},
		{"saved snapshot saving", "saved-snapshot-saving.json", "saveSavedSnapshot", http.StatusAccepted, jsonMediaType},
		{"saved snapshot deleted", "saved-snapshot-deleted.json", "deleteSavedSnapshot", http.StatusOK, jsonMediaType},
		{"file upload url", "file-upload-url.json", "createUploadUrl", http.StatusOK, jsonMediaType},
		{"sandbox stopping", "sandbox-stopping.json", "stop", http.StatusAccepted, jsonMediaType},
		{"sandbox stopped", "sandbox-stopped.json", "stop", http.StatusOK, jsonMediaType},
		{"stop operation", "stop-operation.json", "getStop", http.StatusOK, jsonMediaType},
		{"sandbox resuming", "sandbox-resuming.json", "resume", http.StatusAccepted, jsonMediaType},
		{"sandbox info stopped", "sandbox-info-stopped.json", "get", http.StatusOK, jsonMediaType},
		{"sandbox info error", "sandbox-info-error.json", "get", http.StatusOK, jsonMediaType},
		{"error sandbox not ready", "error-sandbox-not-ready.json", "resume", http.StatusConflict, jsonMediaType},
		{"error invalid query", "error-invalid-query.json", "sandboxes", http.StatusBadRequest, jsonMediaType},
		{"error unauthorized", "error-unauthorized.json", "me", http.StatusUnauthorized, jsonMediaType},
		{"error not found", "error-not-found.json", "get", http.StatusNotFound, jsonMediaType},
		{"error conflict", "error-conflict.json", "create", http.StatusConflict, jsonMediaType},
		{"error sandbox starting", "error-sandbox-starting.json", "command", http.StatusConflict, jsonMediaType},
		{"error no ready machine", "error-no-ready-machine.json", "create", http.StatusServiceUnavailable, jsonMediaType},
		{"error save in progress", "error-save-in-progress.json", "saveSavedSnapshot", http.StatusConflict, jsonMediaType},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validator.Response(test.operationID, test.status, mediaType(test.mediaType), fixture(t, test.fixture))
			if err != nil {
				t.Fatalf("golden fixture does not validate: %v", err)
			}
		})
	}
}

func TestNDJSONGoldenFrames(t *testing.T) {
	validator := Load(t)
	for _, name := range []string{"command-stream.ndjson", "command-stream-error.ndjson"} {
		t.Run(name, func(t *testing.T) {
			err := validator.NDJSON("command", http.StatusOK, mediaType(ndjsonMediaType), fixture(t, name))
			if err != nil {
				t.Fatalf("golden stream does not validate: %v", err)
			}
		})
	}
}

func TestNegativeMutations(t *testing.T) {
	validator := Load(t)
	tests := []struct {
		name        string
		operationID string
		status      int
		mediaType   string
		body        string
	}{
		{
			name:        "missing required sandbox field",
			operationID: "sandboxes",
			status:      http.StatusOK,
			mediaType:   jsonMediaType,
			body:        `{"ok":true,"type":"sandbox.list","sandboxes":[{"id":"bx_23456789","name":"x","state":"idle","snapshotAvailable":false}],"pageInfo":{"nextCursor":null,"hasMore":false,"limit":100}}`,
		},
		{
			name:        "wrong const envelope type",
			operationID: "sandboxes",
			status:      http.StatusOK,
			mediaType:   jsonMediaType,
			body:        `{"ok":true,"type":"sandbox.listx","sandboxes":[],"pageInfo":{"nextCursor":null,"hasMore":false,"limit":100}}`,
		},
		{
			name:        "wrong sandbox state enum",
			operationID: "sandboxes",
			status:      http.StatusOK,
			mediaType:   jsonMediaType,
			body:        `{"ok":true,"type":"sandbox.list","sandboxes":[{"id":"bx_23456789","name":"x","state":"deleting","desktopAvailable":false,"snapshotAvailable":false}],"pageInfo":{"nextCursor":null,"hasMore":false,"limit":100}}`,
		},
		{
			name:        "missing explicit nextCursor member",
			operationID: "sandboxes",
			status:      http.StatusOK,
			mediaType:   jsonMediaType,
			body:        `{"ok":true,"type":"sandbox.list","sandboxes":[],"pageInfo":{"hasMore":false,"limit":100}}`,
		},
		{
			name:        "nextCursor wrong type",
			operationID: "sandboxes",
			status:      http.StatusOK,
			mediaType:   jsonMediaType,
			body:        `{"ok":true,"type":"sandbox.list","sandboxes":[],"pageInfo":{"nextCursor":7,"hasMore":false,"limit":100}}`,
		},
		{
			name:        "required sandbox name null",
			operationID: "get",
			status:      http.StatusOK,
			mediaType:   jsonMediaType,
			body:        `{"ok":true,"type":"sandbox.info","sandbox":{"id":"bx_23456789","name":null,"state":"idle","desktopAvailable":false,"snapshotAvailable":false}}`,
		},
		{
			name:        "wrong create status enum",
			operationID: "create",
			status:      http.StatusAccepted,
			mediaType:   jsonMediaType,
			body:        `{"ok":true,"type":"sandbox.created","status":"done","ttlSeconds":3600,"sandbox":{"id":"bx_23456789","name":"x","state":"provisioning","desktopAvailable":false,"snapshotAvailable":false}}`,
		},
		{
			name:        "deletion status deleting is not pinned",
			operationID: "deleteSandbox",
			status:      http.StatusAccepted,
			mediaType:   jsonMediaType,
			body:        `{"ok":true,"type":"sandbox.deleting","operation":{"id":"bdop_0123456789abcdef0123456789abcdef","kind":"sandbox","targetId":"bx_23456789","reason":"explicit","status":"deleting","attemptCount":0,"requestedAt":"2026-11-08T12:10:00Z","completedAt":null}}`,
		},
		{
			name:        "deletion id pattern",
			operationID: "getDeletionOperation",
			status:      http.StatusOK,
			mediaType:   jsonMediaType,
			body:        `{"ok":true,"type":"deletion.operation","operation":{"id":"op_1","kind":"sandbox","targetId":"bx_23456789","reason":"explicit","status":"pending","attemptCount":0,"requestedAt":"2026-11-08T12:10:00Z","completedAt":null}}`,
		},
		{
			name:        "deletion stage enum",
			operationID: "getDeletionOperation",
			status:      http.StatusOK,
			mediaType:   jsonMediaType,
			body:        `{"ok":true,"type":"deletion.operation","operation":{"id":"bdop_0123456789abcdef0123456789abcdef","kind":"sandbox","targetId":"bx_23456789","reason":"explicit","status":"processing","stage":"zapping","attemptCount":0,"requestedAt":"2026-11-08T12:10:00Z","completedAt":null}}`,
		},
		{
			name:        "missing operation",
			operationID: "deleteSandbox",
			status:      http.StatusAccepted,
			mediaType:   jsonMediaType,
			body:        `{"ok":true,"type":"sandbox.deleting"}`,
		},
		{
			name:        "error envelope missing requestId",
			operationID: "me",
			status:      http.StatusUnauthorized,
			mediaType:   jsonMediaType,
			body:        `{"ok":false,"type":"sandbox.error","status":401,"code":"unauthorized","message":"Unauthorized","error":{"code":"unauthorized","message":"Unauthorized","status":401}}`,
		},
		{
			name:        "error envelope status wrong type",
			operationID: "me",
			status:      http.StatusUnauthorized,
			mediaType:   jsonMediaType,
			body:        `{"ok":false,"type":"sandbox.error","status":"401","code":"unauthorized","message":"Unauthorized","error":{"code":"unauthorized","message":"Unauthorized","status":401},"requestId":"req_1"}`,
		},
		{
			name:        "command exitCode missing",
			operationID: "command",
			status:      http.StatusOK,
			mediaType:   jsonMediaType,
			body:        `{"ok":true,"type":"command.finished","success":true,"stdout":"","stderr":"","timedOut":false}`,
		},
		{
			name:        "command exitCode wrong type",
			operationID: "command",
			status:      http.StatusOK,
			mediaType:   jsonMediaType,
			body:        `{"ok":true,"type":"command.finished","success":true,"exitCode":"0","stdout":"","stderr":"","timedOut":false}`,
		},
		{
			name:        "saved snapshot name pattern",
			operationID: "listSavedSnapshots",
			status:      http.StatusOK,
			mediaType:   jsonMediaType,
			body:        `{"ok":true,"type":"snapshot.saved.list","snapshots":[{"name":"Bad_Name","status":"ready","sourceSandboxId":"bx_23456789","createdAt":"2026-11-08T12:00:00Z"}]}`,
		},
		{
			name:        "file url method enum",
			operationID: "createUploadUrl",
			status:      http.StatusOK,
			mediaType:   jsonMediaType,
			body:        `{"ok":true,"type":"file.upload_url","url":"https://api.example.test/u","method":"POST","path":"/tmp/f","expiresAt":"2026-11-08T12:15:00Z"}`,
		},
		{
			name:        "wrong content type",
			operationID: "sandboxes",
			status:      http.StatusOK,
			mediaType:   "text/plain",
			body:        `{"ok":true,"type":"sandbox.list","sandboxes":[],"pageInfo":{"nextCursor":null,"hasMore":false,"limit":100}}`,
		},
		{
			name:        "undocumented status",
			operationID: "sandboxes",
			status:      http.StatusCreated,
			mediaType:   jsonMediaType,
			body:        `{"ok":true,"type":"sandbox.list","sandboxes":[],"pageInfo":{"nextCursor":null,"hasMore":false,"limit":100}}`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validator.Response(test.operationID, test.status, mediaType(test.mediaType), []byte(test.body))
			if err == nil {
				t.Fatal("validator accepted a mutation that violates the pin")
			}
		})
	}
}

func TestMissingContentTypeRejected(t *testing.T) {
	validator := Load(t)
	err := validator.Response("sandboxes", http.StatusOK, http.Header{}, fixture(t, "sandbox-list.json"))
	if err == nil {
		t.Fatal("validator accepted a response without Content-Type")
	}
}

func TestNDJSONRejectsBadFrames(t *testing.T) {
	validator := Load(t)
	tests := []struct {
		name string
		body string
	}{
		{"invalid frame type", `{"type":"stdout"}` + "\n" + `{"type":"bogus"}` + "\n"},
		{"exitCode wrong type", `{"type":"exit","exitCode":"0"}` + "\n"},
		{"invalid json line", `{"type":"exit"}` + "\n" + `{` + "\n"},
		{"empty stream", "\n\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validator.NDJSON("command", http.StatusOK, mediaType(ndjsonMediaType), []byte(test.body))
			if err == nil {
				t.Fatal("validator accepted an invalid stream")
			}
		})
	}
}

func TestNDJSONReportsFrameNumber(t *testing.T) {
	validator := Load(t)
	body := "{\"type\":\"started\"}\n{\"type\":\"bogus\"}\n"
	err := validator.NDJSON("command", http.StatusOK, mediaType(ndjsonMediaType), []byte(body))
	if err == nil || !strings.Contains(err.Error(), "frame 2") {
		t.Errorf("error = %v, want frame 2", err)
	}
}

func TestRootIsRepositoryRoot(t *testing.T) {
	root := Root(t)
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Errorf("root = %s: %v", root, err)
	}
	if _, err := os.Stat(filepath.Join(root, SpecPath)); err != nil {
		t.Errorf("root = %s: %v", root, err)
	}
}

func TestLoadFileRejectsMissing(t *testing.T) {
	if _, err := load(context.Background(), filepath.Join(t.TempDir(), "missing.yaml")); err == nil {
		t.Error("load accepted a missing file")
	}
}
