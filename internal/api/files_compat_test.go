package api_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
	"github.com/nathabonfim59/bamboo-raft/internal/compat"
)

// fileTestValidators loads the v1 API document.
func fileTestValidators(t *testing.T) []*compat.Validator {
	t.Helper()
	return []*compat.Validator{
		compat.Load(t),
	}
}

// fileTestValidateResponse validates one recorded response against every
// supplied document.
func fileTestValidateResponse(t *testing.T, validators []*compat.Validator, operationID string, status int, recorder *httptest.ResponseRecorder) {
	t.Helper()
	header := http.Header{"Content-Type": []string{"application/json"}}
	for _, validator := range validators {
		if err := validator.Response(operationID, status, header, recorder.Body.Bytes()); err != nil {
			t.Errorf("%s %d did not validate against the spec: %v\nbody: %s",
				operationID, status, err, recorder.Body.Bytes())
		}
	}
}

// TestFilesSuccessMatchesPin validates every file success envelope against the
// pinned document and the v1 API document. Approved-but-undocumented error
// statuses are additionally exercised through the same validators in
// TestFilesApprovedErrorsMatchPinAndSubset; the exact local table in
// files_test.go remains a fast negative oracle for unapproved status/code
// combinations.
func TestFilesSuccessMatchesPin(t *testing.T) {
	validators := fileTestValidators(t)
	env := newFileTestEnv(t)
	base := "/api/v1/sandboxes/" + fileTestSandboxID

	env.writeDisk("/home/user/read.txt", []byte("hello"))
	env.writeDisk("/home/user/delete.txt", []byte("bye"))
	env.writeDisk("/home/user/move.txt", []byte("move"))

	tests := []struct {
		op      string
		target  string
		body    string
		success string
	}{
		{"readFile", base + "/files?path=read.txt", "", api.TypeFileRead},
		{"writeFile", base + "/files", `{"path":"write.txt","content":"hello"}`, api.TypeFileWritten},
		{"deleteFile", base + "/files?path=delete.txt", "", api.TypeFileDeleted},
		{"listFiles", base + "/files/list", "", api.TypeFileList},
		{"getFileInfo", base + "/files/info?path=read.txt", "", api.TypeFileInfo},
		{"createFolder", base + "/files/folder", `{"path":"folder"}`, api.TypeFolderCreated},
		{"moveFile", base + "/files/move", `{"source":"move.txt","destination":"moved.txt"}`, api.TypeFileMoved},
		{"setFilePermissions", base + "/files/permissions", `{"path":"read.txt","mode":"600"}`, api.TypeFilePermissionsSet},
	}
	for _, test := range tests {
		t.Run(test.op, func(t *testing.T) {
			recorder := env.call(t, test.op, test.target, test.body)
			fileTestAssertSuccess(t, recorder, test.success)
			fileTestValidateResponse(t, validators, test.op, http.StatusOK, recorder)
		})
	}
}

// TestFilesDocumentedErrorsMatchPin validates the documented 400 answers and
// the read/write 413 answer against the pinned response tables.
func TestFilesDocumentedErrorsMatchPin(t *testing.T) {
	validators := fileTestValidators(t)
	env := newFileTestEnv(t)
	base := "/api/v1/sandboxes/" + fileTestSandboxID

	tests := []struct {
		name    string
		op      string
		target  string
		body    string
		status  int
		apiCode string
	}{
		{"read invalid path", "readFile", base + "/files?path=%2Fetc%2Fpasswd", "", 400, "invalid_path"},
		{"read invalid query", "readFile", base + "/files?path=a&encoding=hex", "", 400, "invalid_query"},
		{"write invalid json", "writeFile", base + "/files", `{"path":"a"}`, 400, "invalid_json"},
		{"write invalid path", "writeFile", base + "/files", `{"path":"/etc/passwd","content":"x"}`, 400, "invalid_path"},
		{"delete invalid path", "deleteFile", base + "/files?path=%2Fhome%2Fuser", "", 400, "invalid_path"},
		{"list invalid path", "listFiles", base + "/files/list?path=%2Fvar", "", 400, "invalid_path"},
		{"info invalid query", "getFileInfo", base + "/files/info?path=a&encoding=utf8", "", 400, "invalid_query"},
		{"folder invalid json", "createFolder", base + "/files/folder", `{"mode":"755"}`, 400, "invalid_json"},
		{"move invalid json", "moveFile", base + "/files/move", `{"source":"a"}`, 400, "invalid_json"},
		{"permissions invalid json", "setFilePermissions", base + "/files/permissions", `{"path":"a"}`, 400, "invalid_json"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recorder := env.call(t, test.op, test.target, test.body)
			fileTestAssertError(t, recorder, test.status, test.apiCode)
			fileTestValidateResponse(t, validators, test.op, test.status, recorder)
		})
	}

	if testing.Short() {
		t.Skip("allocates 5 MiB fixtures")
	}
	over := bytes.Repeat([]byte{'x'}, int(api.FileInlineLimitBytes)+1)
	env.writeDisk("/home/user/large.bin", over)
	recorder := env.call(t, "readFile", base+"/files?path=large.bin", "")
	fileTestAssertError(t, recorder, http.StatusRequestEntityTooLarge, "file_too_large")
	fileTestValidateResponse(t, validators, "readFile", http.StatusRequestEntityTooLarge, recorder)

	raw := bytes.Repeat([]byte{0x00}, int(api.FileInlineLimitBytes)+1)
	body := `{"path":"large.bin","content":"` + base64.StdEncoding.EncodeToString(raw) + `","encoding":"base64"}`
	recorder = env.call(t, "writeFile", base+"/files", body)
	fileTestAssertError(t, recorder, http.StatusRequestEntityTooLarge, "file_too_large")
	fileTestValidateResponse(t, validators, "writeFile", http.StatusRequestEntityTooLarge, recorder)
}

// fileTestRPCFor maps a file operation to the node RPC its happy path calls.
func fileTestRPCFor(op string) fileTestRPC {
	switch op {
	case "readFile":
		return fileTestRead
	case "writeFile":
		return fileTestWrite
	case "deleteFile":
		return fileTestDelete
	case "listFiles":
		return fileTestList
	case "getFileInfo":
		return fileTestStat
	case "createFolder":
		return fileTestMkdir
	case "moveFile":
		return fileTestMove
	case "setFilePermissions":
		return fileTestChmod
	}
	panic("unknown file operation " + op)
}

// TestFilesApprovedErrorsMatchPinAndSubset validates every approved, but
// pinned-table-omitted, file error status against the v1 API document for every
// operation. The decoded envelope assertions stay in files_test.go; this test
// proves the wire shape itself is accepted by the reviewer-visible specs.
func TestFilesApprovedErrorsMatchPinAndSubset(t *testing.T) {
	validators := fileTestValidators(t)
	ops := fileTestOperations()

	t.Run("401 unauthorized", func(t *testing.T) {
		for _, operation := range ops {
			t.Run(operation.op, func(t *testing.T) {
				var factoryCalls atomic.Int32
				handlers := newFileTestLight(t, fileTestReadyLookup(nil), fileTestCountingFactory(&factoryCalls))
				request := fileTestRequest(operation.method, operation.target, operation.body, nil, fileTestSandboxID)
				recorder := fileTestServe(fileTestHandler(t, handlers, operation.op), request)
				fileTestAssertError(t, recorder, http.StatusUnauthorized, "unauthorized")
				fileTestValidateResponse(t, validators, operation.op, http.StatusUnauthorized, recorder)
			})
		}
	})

	t.Run("404 not_found", func(t *testing.T) {
		lookup := fileTestLookupFunc(func(context.Context, string, string) (api.CommandTarget, error) {
			return api.CommandTarget{}, api.ErrCommandTargetNotFound
		})
		for _, operation := range ops {
			t.Run(operation.op, func(t *testing.T) {
				handlers := newFileTestLight(t, lookup, fileTestCountingFactory(new(atomic.Int32)))
				request := fileTestRequest(operation.method, operation.target, operation.body, fileTestIdentity(), fileTestSandboxID)
				recorder := fileTestServe(fileTestHandler(t, handlers, operation.op), request)
				fileTestAssertError(t, recorder, http.StatusNotFound, "not_found")
				fileTestValidateResponse(t, validators, operation.op, http.StatusNotFound, recorder)
			})
		}
	})

	for _, test := range []struct {
		name  string
		state api.SandboxState
		code  string
	}{
		{"409 machine_not_running", api.StateArchived, "machine_not_running"},
		{"409 sandbox_not_ready", api.StateError, "sandbox_not_ready"},
		{"409 sandbox_starting", api.StateProvisioning, "sandbox_starting"},
	} {
		t.Run(test.name, func(t *testing.T) {
			lookup := fileTestLookupFunc(func(context.Context, string, string) (api.CommandTarget, error) {
				return api.CommandTarget{NodeID: fileTestNodeID, State: test.state}, nil
			})
			for _, operation := range ops {
				t.Run(operation.op, func(t *testing.T) {
					handlers := newFileTestLight(t, lookup, fileTestCountingFactory(new(atomic.Int32)))
					request := fileTestRequest(operation.method, operation.target, operation.body, fileTestIdentity(), fileTestSandboxID)
					recorder := fileTestServe(fileTestHandler(t, handlers, operation.op), request)
					fileTestAssertError(t, recorder, http.StatusConflict, test.code)
					fileTestValidateResponse(t, validators, operation.op, http.StatusConflict, recorder)
				})
			}
		})
	}

	t.Run("500 http_500", func(t *testing.T) {
		lookup := fileTestLookupFunc(func(context.Context, string, string) (api.CommandTarget, error) {
			return api.CommandTarget{}, errors.New("storage unavailable")
		})
		for _, operation := range ops {
			t.Run(operation.op, func(t *testing.T) {
				handlers := newFileTestLight(t, lookup, fileTestCountingFactory(new(atomic.Int32)))
				request := fileTestRequest(operation.method, operation.target, operation.body, fileTestIdentity(), fileTestSandboxID)
				recorder := fileTestServe(fileTestHandler(t, handlers, operation.op), request)
				fileTestAssertError(t, recorder, http.StatusInternalServerError, "http_500")
				fileTestValidateResponse(t, validators, operation.op, http.StatusInternalServerError, recorder)
			})
		}
	})

	t.Run("502 sandbox_direct_failed", func(t *testing.T) {
		for _, operation := range ops {
			t.Run(operation.op, func(t *testing.T) {
				factory := fileTestRPCError(fileTestRPCFor(operation.op), status.Error(codes.Internal, "native node text"))
				handlers := newFileTestLight(t, fileTestReadyLookup(nil), factory)
				request := fileTestRequest(operation.method, operation.target, operation.body, fileTestIdentity(), fileTestSandboxID)
				recorder := fileTestServe(fileTestHandler(t, handlers, operation.op), request)
				body := fileTestAssertError(t, recorder, http.StatusBadGateway, "sandbox_direct_failed")
				if body.Message == "" || bytes.Contains(recorder.Body.Bytes(), []byte("native node text")) {
					t.Error("node text reached the wire")
				}
				fileTestValidateResponse(t, validators, operation.op, http.StatusBadGateway, recorder)
			})
		}
	})
}
