package api_test

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
	"github.com/nathabonfim59/bamboo-raft/internal/auth"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// fileTestOperation is one pinned file operation and its minimal valid call.
type fileTestOperation struct {
	op     string
	method string
	target string
	body   string
}

// fileTestOperations lists the eight operations with calls that reach the
// target lookup.
func fileTestOperations() []fileTestOperation {
	base := "/api/v1/sandboxes/" + fileTestSandboxID
	return []fileTestOperation{
		{"readFile", http.MethodGet, base + "/files?path=note.txt", ""},
		{"writeFile", http.MethodPut, base + "/files", `{"path":"note.txt","content":"x"}`},
		{"deleteFile", http.MethodDelete, base + "/files?path=note.txt", ""},
		{"listFiles", http.MethodGet, base + "/files/list", ""},
		{"getFileInfo", http.MethodGet, base + "/files/info?path=note.txt", ""},
		{"createFolder", http.MethodPost, base + "/files/folder", `{"path":"dir"}`},
		{"moveFile", http.MethodPost, base + "/files/move", `{"source":"a.txt","destination":"b.txt"}`},
		{"setFilePermissions", http.MethodPost, base + "/files/permissions", `{"path":"a.txt","mode":"644"}`},
	}
}

// fileTestOperationFor returns the minimal call of op.
func fileTestOperationFor(op string) fileTestOperation {
	for _, operation := range fileTestOperations() {
		if operation.op == op {
			return operation
		}
	}
	panic("unknown file operation " + op)
}

// fileTestHandler returns the handler of op from handlers.
func fileTestHandler(t *testing.T, handlers api.FilesHandlers, op string) http.HandlerFunc {
	t.Helper()
	switch op {
	case "readFile":
		return handlers.ReadFile
	case "writeFile":
		return handlers.WriteFile
	case "deleteFile":
		return handlers.DeleteFile
	case "listFiles":
		return handlers.ListFiles
	case "getFileInfo":
		return handlers.GetFileInfo
	case "createFolder":
		return handlers.CreateFolder
	case "moveFile":
		return handlers.MoveFile
	case "setFilePermissions":
		return handlers.SetFilePermissions
	}
	t.Fatalf("unknown file operation %q", op)
	return nil
}

// fileTestErrorBody mirrors the pinned ErrorEnvelope.
type fileTestErrorBody struct {
	OK        bool   `json:"ok"`
	Type      string `json:"type"`
	Status    int    `json:"status"`
	Code      string `json:"code"`
	Message   string `json:"message"`
	RequestID string `json:"requestId"`
	Error     struct {
		Code    string         `json:"code"`
		Message string         `json:"message"`
		Status  int            `json:"status"`
		Details map[string]any `json:"details"`
	} `json:"error"`
}

// fileTestApprovedStatuses is the exact approved file-operation error table.
// There is no fallback: every other status or code is rejected.
var fileTestApprovedStatuses = map[int][]string{
	http.StatusBadRequest:            {"invalid_json", "invalid_path", "invalid_query"},
	http.StatusUnauthorized:          {"unauthorized"},
	http.StatusNotFound:              {"not_found"},
	http.StatusConflict:              {"machine_not_running", "sandbox_not_ready", "sandbox_starting"},
	http.StatusRequestEntityTooLarge: {"file_too_large"},
	http.StatusInternalServerError:   {"http_500"},
	http.StatusBadGateway:            {"sandbox_direct_failed"},
}

// fileTestCheckApproved rejects an error body outside the approved table or
// with a malformed envelope.
func fileTestCheckApproved(t *testing.T, recorder *httptest.ResponseRecorder) fileTestErrorBody {
	t.Helper()
	contentType := recorder.Header().Get("Content-Type")
	if !strings.HasPrefix(contentType, "application/json") {
		t.Fatalf("Content-Type = %q, want application/json", contentType)
	}
	var body fileTestErrorBody
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode error envelope: %v\nbody: %s", err, recorder.Body.Bytes())
	}
	if err := fileTestApprovedError(body); err != nil {
		t.Fatalf("error envelope is not approved: %v\nbody: %s", err, recorder.Body.Bytes())
	}
	return body
}

// fileTestApprovedError validates the exact envelope shape and the approved
// status/code table.
func fileTestApprovedError(body fileTestErrorBody) error {
	if body.OK {
		return errors.New("ok must be false")
	}
	if body.Type != "sandbox.error" {
		return fmt.Errorf("type = %q", body.Type)
	}
	codes, ok := fileTestApprovedStatuses[body.Status]
	if !ok {
		return fmt.Errorf("status %d is not approved", body.Status)
	}
	approved := false
	for _, code := range codes {
		if body.Code == code {
			approved = true
			break
		}
	}
	if !approved {
		return fmt.Errorf("code %q is not approved for status %d", body.Code, body.Status)
	}
	if body.Message == "" || body.Error.Message != body.Message {
		return errors.New("message is empty or does not match the nested message")
	}
	if body.Error.Code != body.Code || body.Error.Status != body.Status {
		return errors.New("nested error does not match the envelope")
	}
	if !strings.HasPrefix(body.RequestID, "req_") {
		return fmt.Errorf("requestId = %q", body.RequestID)
	}
	return nil
}

// fileTestAssertError asserts one exact status and code and validates the
// approved envelope.
func fileTestAssertError(t *testing.T, recorder *httptest.ResponseRecorder, wantStatus int, wantCode string) fileTestErrorBody {
	t.Helper()
	if recorder.Code != wantStatus {
		t.Fatalf("status = %d, want %d\nbody: %s", recorder.Code, wantStatus, recorder.Body.Bytes())
	}
	body := fileTestCheckApproved(t, recorder)
	if body.Code != wantCode {
		t.Fatalf("code = %q, want %q\nbody: %s", body.Code, wantCode, recorder.Body.Bytes())
	}
	return body
}

// fileTestCountingFactory counts factory calls and returns an unusable client
// so every node call fails loudly.
func fileTestCountingFactory(calls *atomic.Int32) api.NodeClientFactory {
	return func(string) (nodev1.NodeClient, func() error, error) {
		calls.Add(1)
		return nil, nil, errors.New("node factory must not be reached")
	}
}

// fileTestReadyLookup is a ready owner-scoped target lookup.
func fileTestReadyLookup(calls *atomic.Int32) api.CommandTargetLookup {
	return fileTestLookupFunc(func(context.Context, string, string) (api.CommandTarget, error) {
		if calls != nil {
			calls.Add(1)
		}
		return api.CommandTarget{NodeID: fileTestNodeID, State: api.StateReady}, nil
	})
}

func TestFilesConstructorValidation(t *testing.T) {
	if _, err := api.NewFilesHandlers(api.FilesOptions{}); err == nil {
		t.Error("missing lookup accepted")
	}
	if _, err := api.NewFilesHandlers(api.FilesOptions{Lookup: fileTestReadyLookup(nil)}); err == nil {
		t.Error("missing node-client factory accepted")
	}
}

func TestFilesUnauthorized(t *testing.T) {
	for _, operation := range fileTestOperations() {
		t.Run(operation.op, func(t *testing.T) {
			var lookupCalls, factoryCalls atomic.Int32
			handlers := newFileTestLight(t, fileTestReadyLookup(&lookupCalls), fileTestCountingFactory(&factoryCalls))
			request := fileTestRequest(operation.method, operation.target, operation.body, nil, fileTestSandboxID)
			recorder := fileTestServe(fileTestHandler(t, handlers, operation.op), request)
			body := fileTestAssertError(t, recorder, http.StatusUnauthorized, "unauthorized")
			if body.Error.Details != nil {
				t.Errorf("unauthorized details = %v, want none", body.Error.Details)
			}
			if lookupCalls.Load() != 0 || factoryCalls.Load() != 0 {
				t.Errorf("lookup=%d factory=%d calls without an identity", lookupCalls.Load(), factoryCalls.Load())
			}
		})
	}
}

func TestFilesUnknownAndForeignSandbox(t *testing.T) {
	for _, operation := range fileTestOperations() {
		t.Run(operation.op, func(t *testing.T) {
			var factoryCalls atomic.Int32
			var owner, sandboxID string
			lookup := fileTestLookupFunc(func(_ context.Context, gotOwner, gotID string) (api.CommandTarget, error) {
				owner, sandboxID = gotOwner, gotID
				return api.CommandTarget{}, api.ErrCommandTargetNotFound
			})
			handlers := newFileTestLight(t, lookup, fileTestCountingFactory(&factoryCalls))
			request := fileTestRequest(operation.method, operation.target, operation.body, fileTestIdentity(), fileTestSandboxID)
			recorder := fileTestServe(fileTestHandler(t, handlers, operation.op), request)
			fileTestAssertError(t, recorder, http.StatusNotFound, "not_found")
			if owner != fileTestOwner || sandboxID != fileTestSandboxID {
				t.Errorf("lookup = owner %q sandbox %q", owner, sandboxID)
			}
			if factoryCalls.Load() != 0 {
				t.Errorf("node factory reached %d times for an unknown sandbox", factoryCalls.Load())
			}
		})
	}
}

func TestFilesMissingSandboxParameter(t *testing.T) {
	operation := fileTestOperationFor("readFile")
	var lookupCalls, factoryCalls atomic.Int32
	handlers := newFileTestLight(t, fileTestReadyLookup(&lookupCalls), fileTestCountingFactory(&factoryCalls))
	request := fileTestRequest(operation.method, operation.target, "", fileTestIdentity(), "")
	recorder := fileTestServe(handlers.ReadFile, request)
	fileTestAssertError(t, recorder, http.StatusNotFound, "not_found")
	if lookupCalls.Load() != 0 || factoryCalls.Load() != 0 {
		t.Error("missing sandbox parameter reached the lookup or the node")
	}
}

// TestFilesInvalidInput checks that malformed calls are refused before the
// owner lookup and the node.
func TestFilesInvalidInput(t *testing.T) {
	base := "/api/v1/sandboxes/" + fileTestSandboxID
	tests := []struct {
		name   string
		op     string
		target string
		body   string
		status int
		code   string
	}{
		{"read missing path", "readFile", base + "/files", "", 400, "invalid_query"},
		{"read duplicate path", "readFile", base + "/files?path=a&path=b", "", 400, "invalid_query"},
		{"read unknown query", "readFile", base + "/files?path=a&bogus=1", "", 400, "invalid_query"},
		{"read bad encoding", "readFile", base + "/files?path=a&encoding=hex", "", 400, "invalid_query"},
		{"read duplicate encoding", "readFile", base + "/files?path=a&encoding=utf8&encoding=base64", "", 400, "invalid_query"},
		{"read recursive unsupported", "readFile", base + "/files?path=a&recursive=true", "", 400, "invalid_query"},
		{"read empty path", "readFile", base + "/files?path=", "", 400, "invalid_path"},
		{"read outside root", "readFile", base + "/files?path=%2Fetc%2Fpasswd", "", 400, "invalid_path"},
		{"read invalid utf8", "readFile", base + "/files?path=%FF", "", 400, "invalid_path"},
		{"read traversal", "readFile", base + "/files?path=..%2F..%2Fetc%2Fpasswd", "", 400, "invalid_path"},
		{"read nul path", "readFile", base + "/files?path=a%00b", "", 400, "invalid_path"},

		{"write empty body", "writeFile", base + "/files", "", 400, "invalid_json"},
		{"write bad json", "writeFile", base + "/files", "{", 400, "invalid_json"},
		{"write unknown member", "writeFile", base + "/files", `{"path":"a","content":"x","bogus":1}`, 400, "invalid_json"},
		{"write duplicate member", "writeFile", base + "/files", `{"path":"a","path":"b","content":"x"}`, 400, "invalid_json"},
		{"write trailing json", "writeFile", base + "/files", `{"path":"a","content":"x"} {}`, 400, "invalid_json"},
		{"write missing path", "writeFile", base + "/files", `{"content":"x"}`, 400, "invalid_json"},
		{"write null path", "writeFile", base + "/files", `{"path":null,"content":"x"}`, 400, "invalid_json"},
		{"write path type", "writeFile", base + "/files", `{"path":5,"content":"x"}`, 400, "invalid_json"},
		{"write missing content", "writeFile", base + "/files", `{"path":"a"}`, 400, "invalid_json"},
		{"write null content", "writeFile", base + "/files", `{"path":"a","content":null}`, 400, "invalid_json"},
		{"write content type", "writeFile", base + "/files", `{"path":"a","content":5}`, 400, "invalid_json"},
		{"write bad encoding", "writeFile", base + "/files", `{"path":"a","content":"x","encoding":"hex"}`, 400, "invalid_json"},
		{"write null encoding", "writeFile", base + "/files", `{"path":"a","content":"x","encoding":null}`, 400, "invalid_json"},
		{"write bad base64", "writeFile", base + "/files", `{"path":"a","content":"%%%","encoding":"base64"}`, 400, "invalid_json"},
		{"write empty path", "writeFile", base + "/files", `{"path":"","content":"x"}`, 400, "invalid_path"},
		{"write traversal path", "writeFile", base + "/files", `{"path":"../../etc/passwd","content":"x"}`, 400, "invalid_path"},
		{"write query unsupported", "writeFile", base + "/files?path=a", `{"path":"a","content":"x"}`, 400, "invalid_query"},

		{"delete missing path", "deleteFile", base + "/files", "", 400, "invalid_query"},
		{"delete empty path", "deleteFile", base + "/files?path=", "", 400, "invalid_path"},
		{"delete traversal", "deleteFile", base + "/files?path=../../etc/passwd", "", 400, "invalid_path"},
		{"delete bad recursive", "deleteFile", base + "/files?path=a&recursive=yes", "", 400, "invalid_query"},
		{"delete duplicate path", "deleteFile", base + "/files?path=a&path=b", "", 400, "invalid_query"},
		{"delete home root", "deleteFile", base + "/files?path=%2Fhome%2Fuser", "", 400, "invalid_path"},
		{"delete tmp root", "deleteFile", base + "/files?path=%2Ftmp", "", 400, "invalid_path"},
		{"delete unknown query", "deleteFile", base + "/files?path=a&bogus=1", "", 400, "invalid_query"},

		{"list outside", "listFiles", base + "/files/list?path=%2Fvar%2Flog", "", 400, "invalid_path"},
		{"list empty path", "listFiles", base + "/files/list?path=", "", 400, "invalid_path"},
		{"list duplicate path", "listFiles", base + "/files/list?path=a&path=b", "", 400, "invalid_query"},
		{"list unknown query", "listFiles", base + "/files/list?bogus=1", "", 400, "invalid_query"},

		{"info missing path", "getFileInfo", base + "/files/info", "", 400, "invalid_query"},
		{"info outside", "getFileInfo", base + "/files/info?path=%2Fetc%2Fpasswd", "", 400, "invalid_path"},
		{"info encoding unsupported", "getFileInfo", base + "/files/info?path=a&encoding=utf8", "", 400, "invalid_query"},

		{"folder empty body", "createFolder", base + "/files/folder", "", 400, "invalid_json"},
		{"folder missing path", "createFolder", base + "/files/folder", `{"mode":"755"}`, 400, "invalid_json"},
		{"folder null path", "createFolder", base + "/files/folder", `{"path":null}`, 400, "invalid_json"},
		{"folder empty path", "createFolder", base + "/files/folder", `{"path":""}`, 400, "invalid_path"},
		{"folder mode type", "createFolder", base + "/files/folder", `{"path":"a","mode":755}`, 400, "invalid_json"},
		{"folder empty mode", "createFolder", base + "/files/folder", `{"path":"a","mode":""}`, 400, "invalid_json"},
		{"folder null mode", "createFolder", base + "/files/folder", `{"path":"a","mode":null}`, 400, "invalid_json"},
		{"folder mode digit", "createFolder", base + "/files/folder", `{"path":"a","mode":"789"}`, 400, "invalid_json"},
		{"folder mode large", "createFolder", base + "/files/folder", `{"path":"a","mode":"10000"}`, 400, "invalid_json"},
		{"folder duplicate", "createFolder", base + "/files/folder", `{"path":"a","path":"b"}`, 400, "invalid_json"},
		{"folder query", "createFolder", base + "/files/folder?path=a", `{"path":"a"}`, 400, "invalid_query"},

		{"move missing source", "moveFile", base + "/files/move", `{"destination":"b"}`, 400, "invalid_json"},
		{"move missing destination", "moveFile", base + "/files/move", `{"source":"a"}`, 400, "invalid_json"},
		{"move null source", "moveFile", base + "/files/move", `{"source":null,"destination":"b"}`, 400, "invalid_json"},
		{"move destination type", "moveFile", base + "/files/move", `{"source":"a","destination":5}`, 400, "invalid_json"},
		{"move overwrite type", "moveFile", base + "/files/move", `{"source":"a","destination":"b","overwrite":"yes"}`, 400, "invalid_json"},
		{"move null overwrite", "moveFile", base + "/files/move", `{"source":"a","destination":"b","overwrite":null}`, 400, "invalid_json"},
		{"move empty source", "moveFile", base + "/files/move", `{"source":"","destination":"b"}`, 400, "invalid_path"},
		{"move traversal", "moveFile", base + "/files/move", `{"source":"a","destination":"../../etc/passwd"}`, 400, "invalid_path"},
		{"move home root", "moveFile", base + "/files/move", `{"source":"/home/user","destination":"b"}`, 400, "invalid_path"},
		{"move tmp root destination", "moveFile", base + "/files/move", `{"source":"a","destination":"/tmp"}`, 400, "invalid_path"},

		{"permissions missing path", "setFilePermissions", base + "/files/permissions", `{"mode":"644"}`, 400, "invalid_json"},
		{"permissions missing mode", "setFilePermissions", base + "/files/permissions", `{"path":"a"}`, 400, "invalid_json"},
		{"permissions empty mode", "setFilePermissions", base + "/files/permissions", `{"path":"a","mode":""}`, 400, "invalid_json"},
		{"permissions bad mode", "setFilePermissions", base + "/files/permissions", `{"path":"a","mode":"abc"}`, 400, "invalid_json"},
		{"permissions mode digit", "setFilePermissions", base + "/files/permissions", `{"path":"a","mode":"789"}`, 400, "invalid_json"},
		{"permissions mode overflow", "setFilePermissions", base + "/files/permissions", `{"path":"a","mode":"77777"}`, 400, "invalid_json"},
		{"permissions mode large", "setFilePermissions", base + "/files/permissions", `{"path":"a","mode":"10000"}`, 400, "invalid_json"},
		{"permissions bad path", "setFilePermissions", base + "/files/permissions", `{"path":"/etc/passwd","mode":"644"}`, 400, "invalid_path"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var lookupCalls, factoryCalls atomic.Int32
			handlers := newFileTestLight(t, fileTestReadyLookup(&lookupCalls), fileTestCountingFactory(&factoryCalls))
			request := fileTestRequest(fileTestMethodFor(test.op), test.target, test.body, fileTestIdentity(), fileTestSandboxID)
			recorder := fileTestServe(fileTestHandler(t, handlers, test.op), request)
			fileTestAssertError(t, recorder, test.status, test.code)
			if lookupCalls.Load() != 0 {
				t.Errorf("invalid request reached the lookup %d times", lookupCalls.Load())
			}
			if factoryCalls.Load() != 0 {
				t.Errorf("invalid request reached the node factory %d times", factoryCalls.Load())
			}
		})
	}
}

// fileTestMethodFor returns the pinned method of one file operation.
func fileTestMethodFor(op string) string {
	return fileTestOperationFor(op).method
}

func TestFilesStateMapping(t *testing.T) {
	tests := []struct {
		name      string
		state     api.SandboxState
		status    int
		code      string
		retryable bool
		hasRetry  bool
	}{
		{"archived", api.StateArchived, 409, "machine_not_running", false, true},
		{"error", api.StateError, 409, "sandbox_not_ready", false, true},
		{"cancelled", api.StateCancelled, 409, "sandbox_not_ready", false, true},
		{"archiving", api.StateArchiving, 409, "sandbox_not_ready", false, true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			lookup := fileTestLookupFunc(func(context.Context, string, string) (api.CommandTarget, error) {
				return api.CommandTarget{NodeID: fileTestNodeID, State: test.state}, nil
			})
			var factoryCalls atomic.Int32
			handlers := newFileTestLight(t, lookup, fileTestCountingFactory(&factoryCalls))
			operation := fileTestOperationFor("readFile")
			request := fileTestRequest(operation.method, operation.target, "", fileTestIdentity(), fileTestSandboxID)
			recorder := fileTestServe(handlers.ReadFile, request)
			body := fileTestAssertError(t, recorder, test.status, test.code)
			if body.Error.Details["state"] != string(test.state) {
				t.Errorf("state detail = %v, want %q", body.Error.Details["state"], test.state)
			}
			if retryable, ok := body.Error.Details["retryable"].(bool); !ok || retryable != test.retryable {
				t.Errorf("retryable detail = %v, want %v", body.Error.Details["retryable"], test.retryable)
			}
			if factoryCalls.Load() != 0 {
				t.Errorf("non-ready sandbox reached the node %d times", factoryCalls.Load())
			}
		})
	}
}

func TestFilesStartingSandboxTimeout(t *testing.T) {
	var lookupCalls atomic.Int32
	lookup := fileTestLookupFunc(func(context.Context, string, string) (api.CommandTarget, error) {
		lookupCalls.Add(1)
		return api.CommandTarget{State: api.StateProvisioning}, nil
	})
	var factoryCalls atomic.Int32
	handlers := newFileTestLight(t, lookup, fileTestCountingFactory(&factoryCalls), func(options *api.FilesOptions) {
		options.ReadyDeadline = 10 * time.Millisecond
		options.ReadyPoll = time.Millisecond
	})
	operation := fileTestOperationFor("readFile")
	request := fileTestRequest(operation.method, operation.target, "", fileTestIdentity(), fileTestSandboxID)
	recorder := fileTestServe(handlers.ReadFile, request)
	body := fileTestAssertError(t, recorder, 409, "sandbox_starting")
	if retryable, ok := body.Error.Details["retryable"].(bool); !ok || !retryable {
		t.Errorf("sandbox_starting retryable = %v, want true", body.Error.Details["retryable"])
	}
	if body.Error.Details["state"] != string(api.StateProvisioning) {
		t.Errorf("state detail = %v", body.Error.Details["state"])
	}
	if lookupCalls.Load() < 1 {
		t.Errorf("lookup calls = %d, want at least the initial lookup", lookupCalls.Load())
	}
	if factoryCalls.Load() != 0 {
		t.Error("starting sandbox reached the node")
	}
}

func TestFilesCallerCancelWhileStarting(t *testing.T) {
	started := make(chan struct{})
	lookup := fileTestLookupFunc(func(ctx context.Context, _, _ string) (api.CommandTarget, error) {
		select {
		case <-started:
		default:
			close(started)
		}
		<-ctx.Done()
		return api.CommandTarget{}, ctx.Err()
	})
	handlers := newFileTestLight(t, lookup, fileTestCountingFactory(new(atomic.Int32)))
	operation := fileTestOperationFor("readFile")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	request := fileTestRequest(operation.method, operation.target, "", fileTestIdentity(), fileTestSandboxID).WithContext(
		auth.WithIdentity(ctx, auth.Identity{Owner: fileTestOwner}))
	writer := newFileTestResponseWriter()
	done := make(chan struct{})
	go func() {
		defer close(done)
		handlers.ReadFile(writer, request)
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		cancel()
		fileTestJoin(t, done)
		t.Fatal("lookup never started")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not return after cancellation")
	}
	if writer.wrote {
		t.Errorf("canceled handler wrote status %d body %q", writer.status, writer.body.String())
	}
}

func TestFilesNodeErrorMapping(t *testing.T) {
	type testCase struct {
		name      string
		op        string
		call      fileTestRPC
		code      codes.Code
		status    int
		apiCode   string
		retryable bool
		message   string
	}
	tests := []testCase{
		{"read not found", "readFile", fileTestRead, codes.NotFound, 404, "not_found", false, ""},
		{"read invalid", "readFile", fileTestRead, codes.InvalidArgument, 400, "invalid_path", false, ""},
		{"read precondition", "readFile", fileTestRead, codes.FailedPrecondition, 409, "sandbox_not_ready", false, ""},
		{"read already exists", "readFile", fileTestRead, codes.AlreadyExists, 502, "sandbox_direct_failed", true, ""},
		{"read exhausted", "readFile", fileTestRead, codes.ResourceExhausted, 502, "sandbox_direct_failed", true, ""},
		{"read internal", "readFile", fileTestRead, codes.Internal, 502, "sandbox_direct_failed", true, ""},
		{"list internal", "listFiles", fileTestList, codes.Internal, 502, "sandbox_direct_failed", true, ""},
		{"info internal", "getFileInfo", fileTestStat, codes.Internal, 502, "sandbox_direct_failed", true, ""},
		{"write internal", "writeFile", fileTestWrite, codes.Internal, 502, "sandbox_direct_failed", true, ""},
		{"delete internal", "deleteFile", fileTestDelete, codes.Internal, 502, "sandbox_direct_failed", false, ""},
		{"delete exhausted", "deleteFile", fileTestDelete, codes.ResourceExhausted, 502, "sandbox_direct_failed", false, ""},
		{"folder exists", "createFolder", fileTestMkdir, codes.AlreadyExists, 400, "invalid_path", false, "path exists and is not a directory"},
		{"folder exhausted", "createFolder", fileTestMkdir, codes.ResourceExhausted, 502, "sandbox_direct_failed", false, ""},
		{"move exists", "moveFile", fileTestMove, codes.AlreadyExists, 400, "invalid_path", false, "destination exists; set overwrite"},
		{"move internal", "moveFile", fileTestMove, codes.Internal, 502, "sandbox_direct_failed", false, ""},
		{"permissions internal", "setFilePermissions", fileTestChmod, codes.Internal, 502, "sandbox_direct_failed", false, ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			factory := fileTestRPCError(test.call, status.Error(test.code, "native secret text"))
			handlers := newFileTestLight(t, fileTestReadyLookup(nil), factory)
			operation := fileTestOperationFor(test.op)
			request := fileTestRequest(operation.method, operation.target, operation.body, fileTestIdentity(), fileTestSandboxID)
			recorder := fileTestServe(fileTestHandler(t, handlers, test.op), request)
			body := fileTestAssertError(t, recorder, test.status, test.apiCode)
			if test.message != "" && body.Message != test.message {
				t.Errorf("message = %q, want %q", body.Message, test.message)
			}
			if strings.Contains(recorder.Body.String(), "native secret text") {
				t.Error("native node text reached the wire")
			}
			if test.status == 502 {
				retryable, ok := body.Error.Details["retryable"].(bool)
				if !ok || retryable != test.retryable {
					t.Errorf("retryable = %v, want %v", body.Error.Details["retryable"], test.retryable)
				}
			}
			if test.status == 503 {
				t.Error("503 must never be produced by a file operation")
			}
		})
	}
}

// TestFilesRejectsUnlistedErrors proves the strict local table rejects status
// and code combinations outside the approved rows.
func TestFilesRejectsUnlistedErrors(t *testing.T) {
	base := fileTestErrorBody{
		OK:        false,
		Type:      "sandbox.error",
		Status:    http.StatusConflict,
		Code:      "conflict",
		Message:   "conflict",
		RequestID: "req_test",
	}
	base.Error.Code = base.Code
	base.Error.Message = base.Message
	base.Error.Status = base.Status
	if err := fileTestApprovedError(base); err == nil {
		t.Error("409 conflict accepted")
	}

	unlisted := []struct {
		name   string
		status int
		code   string
	}{
		{"conflict", http.StatusConflict, "conflict"},
		{"out of capacity", http.StatusServiceUnavailable, "out_of_capacity"},
		{"no ready machine", http.StatusServiceUnavailable, "no_ready_machine"},
		{"teapot", http.StatusTeapot, "teapot"},
		{"409 unknown", http.StatusConflict, "already_exists"},
		{"502 wrong code", http.StatusBadGateway, "http_500"},
	}
	for _, test := range unlisted {
		t.Run(test.name, func(t *testing.T) {
			body := base
			body.Status, body.Code = test.status, test.code
			body.Error.Status, body.Error.Code = test.status, test.code
			if err := fileTestApprovedError(body); err == nil {
				t.Errorf("%d %s accepted", test.status, test.code)
			}
		})
	}

	malformed := []struct {
		name   string
		mutate func(*fileTestErrorBody)
	}{
		{"ok true", func(body *fileTestErrorBody) { body.OK = true }},
		{"wrong type", func(body *fileTestErrorBody) { body.Type = "file.read" }},
		{"nested code", func(body *fileTestErrorBody) { body.Error.Code = "invalid_path" }},
		{"nested status", func(body *fileTestErrorBody) { body.Error.Status = http.StatusBadRequest }},
		{"empty message", func(body *fileTestErrorBody) { body.Message = "" }},
		{"different messages", func(body *fileTestErrorBody) { body.Error.Message = "other" }},
		{"missing request id", func(body *fileTestErrorBody) { body.RequestID = "" }},
		{"short request id", func(body *fileTestErrorBody) { body.RequestID = "abc" }},
	}
	for _, test := range malformed {
		t.Run(test.name, func(t *testing.T) {
			body := base
			body.Status, body.Code = http.StatusNotFound, "not_found"
			body.Message = "not found"
			body.Error.Status, body.Error.Code, body.Error.Message = body.Status, body.Code, body.Message
			test.mutate(&body)
			if err := fileTestApprovedError(body); err == nil {
				t.Error("malformed envelope accepted")
			}
		})
	}
}

func TestFilesRegisterRoutes(t *testing.T) {
	hit := func(name string) http.HandlerFunc {
		return func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("X-Hit", name)
			w.WriteHeader(http.StatusNoContent)
		}
	}
	handlers := api.FilesHandlers{
		ReadFile:           hit("read"),
		WriteFile:          hit("write"),
		DeleteFile:         hit("delete"),
		ListFiles:          hit("list"),
		GetFileInfo:        hit("info"),
		CreateFolder:       hit("folder"),
		MoveFile:           hit("move"),
		SetFilePermissions: hit("permissions"),
	}
	mux := http.NewServeMux()
	api.RegisterFiles(mux, handlers)
	base := "/api/v1/sandboxes/" + fileTestSandboxID
	tests := []struct {
		method string
		target string
		want   string
	}{
		{http.MethodGet, base + "/files?path=a", "read"},
		{http.MethodPut, base + "/files", "write"},
		{http.MethodDelete, base + "/files?path=a", "delete"},
		{http.MethodGet, base + "/files/list", "list"},
		{http.MethodGet, base + "/files/info?path=a", "info"},
		{http.MethodPost, base + "/files/folder", "folder"},
		{http.MethodPost, base + "/files/move", "move"},
		{http.MethodPost, base + "/files/permissions", "permissions"},
	}
	for _, test := range tests {
		t.Run(test.method+" "+test.target, func(t *testing.T) {
			request := httptest.NewRequest(test.method, test.target, nil)
			recorder := httptest.NewRecorder()
			mux.ServeHTTP(recorder, request)
			if recorder.Code != http.StatusNoContent {
				t.Fatalf("status = %d, want 204", recorder.Code)
			}
			if got := recorder.Header().Get("X-Hit"); got != test.want {
				t.Errorf("handler = %q, want %q", got, test.want)
			}
		})
	}
	t.Run("wrong method", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		mux.ServeHTTP(recorder, httptest.NewRequest(http.MethodPatch, base+"/files", nil))
		if recorder.Code != http.StatusMethodNotAllowed {
			t.Errorf("status = %d, want 405", recorder.Code)
		}
	})
	t.Run("nil handlers unregistered", func(t *testing.T) {
		empty := http.NewServeMux()
		api.RegisterFiles(empty, api.FilesHandlers{})
		recorder := httptest.NewRecorder()
		empty.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, base+"/files/list", nil))
		if recorder.Code != http.StatusNotFound {
			t.Errorf("status = %d, want 404 for an unregistered route", recorder.Code)
		}
	})
}
