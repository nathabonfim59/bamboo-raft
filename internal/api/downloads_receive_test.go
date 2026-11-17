package api_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
	"github.com/nathabonfim59/bamboo-raft/internal/apierr"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

func downloadTestReceive(t *testing.T, handlers api.DownloadsHandlers, method, token, query string) *httptest.ResponseRecorder {
	t.Helper()
	target := "/api/bbraft/downloads/" + token + query
	request := httptest.NewRequest(method, target, nil)
	request.SetPathValue(api.DownloadTokenParam, token)
	recorder := httptest.NewRecorder()
	handlers.Receive(recorder, request)
	return recorder
}

func TestDownloadReceiveRealStack(t *testing.T) {
	env := downloadTestStartEnv(t)
	lookup := downloadTestReadyLookup()
	handlers := downloadTestHandlers(t, lookup, env.factory(), newDownloadTestClock())

	files := []struct {
		name string
		path string
		data []byte
	}{
		{name: "empty", path: "/home/user/empty.bin", data: []byte{}},
		{name: "small", path: "/home/user/notes.txt", data: []byte("hello downloads\n")},
		{name: "multi chunk", path: "/home/user/data/chunks.bin", data: bytes.Repeat([]byte("0123456789abcdef"), (2<<20+123)/16)},
		{name: "above api read limit", path: "/tmp/large.bin", data: bytes.Repeat([]byte{0xa5}, 5<<20+12345)},
	}
	for _, file := range files {
		t.Run(file.name, func(t *testing.T) {
			env.writeFile(t, downloadTestSandboxID, file.path, file.data)
			token, response := downloadTestIssueToken(t, handlers, downloadTestOwner, downloadTestSandboxID, file.path)
			if response.Path != file.path {
				t.Fatalf("issued path = %q, want %q", response.Path, file.path)
			}
			recorder := downloadTestReceive(t, handlers, http.MethodGet, token, "")
			if recorder.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200", recorder.Code)
			}
			if got := recorder.Body.Bytes(); !bytes.Equal(got, file.data) {
				t.Fatalf("body = %d bytes, want %d", len(got), len(file.data))
			}
			header := recorder.Result().Header
			if header.Get("Content-Type") != "application/octet-stream" {
				t.Errorf("Content-Type = %q", header.Get("Content-Type"))
			}
			if header.Get("Cache-Control") != "no-store" {
				t.Errorf("Cache-Control = %q", header.Get("Cache-Control"))
			}
			if header.Get("X-Content-Type-Options") != "nosniff" {
				t.Errorf("X-Content-Type-Options = %q", header.Get("X-Content-Type-Options"))
			}
			if header.Get("Content-Length") != "" {
				t.Errorf("Content-Length = %q, want absent", header.Get("Content-Length"))
			}
			mediaType, params, err := mime.ParseMediaType(header.Get("Content-Disposition"))
			if err != nil || mediaType != "attachment" {
				t.Errorf("Content-Disposition = %q", header.Get("Content-Disposition"))
			}
			if name := file.path[strings.LastIndex(file.path, "/")+1:]; params["filename"] != name {
				t.Errorf("filename = %q, want %q", params["filename"], name)
			}
		})
	}
}

func TestDownloadReceiveHeadersAreSafe(t *testing.T) {
	// Build the control filename from actual CR and LF bytes, and assert that
	// below before it ever crosses JSON or the token.
	controlName := "a\r" + "\n" + "X-Evil: 1.txt"
	if !strings.Contains(controlName, "\r\n") {
		t.Fatal("control filename does not contain actual CRLF bytes")
	}
	tests := []struct {
		name string
		path string
	}{
		{name: "space", path: "/home/user/report 1.txt"},
		{name: "quote", path: `/home/user/a"b.txt`},
		{name: "unicode", path: "/home/user/naïve file.txt"},
		{name: "control", path: "/home/user/" + controlName},
		{name: "semicolon", path: "/home/user/a;b.txt"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			clock := newDownloadTestClock()
			stream := downloadTestScriptedStream([]*nodev1.ReadFileResponse{
				downloadTestInfoMessage(test.path, 0),
			}, nil)
			handlers := downloadTestHandlers(t, downloadTestReadyLookup(), downloadTestFactory(&downloadTestNodeClient{
				readFile: func(ctx context.Context, _ *nodev1.ReadFileRequest) (nodev1.Node_ReadFileClient, error) {
					stream.ctx = ctx
					return stream, nil
				},
			}), clock)
			token, _ := downloadTestIssueToken(t, handlers, downloadTestOwner, downloadTestSandboxID, test.path)
			if test.name == "control" {
				payload := downloadTestDecodeToken(t, token)
				if !strings.Contains(payload.Path, "\r\n") {
					t.Fatal("token carry path lost the actual CRLF bytes")
				}
			}
			recorder := downloadTestReceive(t, handlers, http.MethodGet, token, "")
			if recorder.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200", recorder.Code)
			}
			raw := recorder.Result().Header.Get("Content-Disposition")
			for _, r := range raw {
				if r < 0x20 || r == 0x7f {
					t.Fatalf("Content-Disposition contains a raw control byte %#x", r)
				}
			}
			mediaType, params, err := mime.ParseMediaType(raw)
			if err != nil || mediaType != "attachment" {
				t.Fatalf("Content-Disposition = %q, err = %v", raw, err)
			}
			name := test.path[strings.LastIndex(test.path, "/")+1:]
			if params["filename"] != name {
				t.Errorf("filename = %q, want %q", params["filename"], name)
			}
		})
	}
}

func TestDownloadReceiveTargetErrors(t *testing.T) {
	tests := []struct {
		name       string
		target     api.CommandTarget
		lookupErr  error
		wantStatus int
		wantCode   string
	}{
		{name: "foreign or deleted", lookupErr: api.ErrCommandTargetNotFound, wantStatus: http.StatusNotFound, wantCode: apierr.CodeNotFound},
		{name: "lookup fault", lookupErr: errors.New("db unavailable"), wantStatus: http.StatusInternalServerError, wantCode: apierr.CodeInternal},
		{name: "archived", target: api.CommandTarget{NodeID: downloadTestNodeID, State: api.StateArchived}, wantStatus: http.StatusConflict, wantCode: apierr.CodeSandboxNotReady},
		{name: "error", target: api.CommandTarget{NodeID: downloadTestNodeID, State: api.StateError}, wantStatus: http.StatusConflict, wantCode: apierr.CodeSandboxNotReady},
		{name: "ready without node", target: api.CommandTarget{State: api.StateReady}, wantStatus: http.StatusInternalServerError, wantCode: apierr.CodeInternal},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			clock := newDownloadTestClock()
			issuer := downloadTestHandlers(t, downloadTestReadyLookup(), downloadTestRejectingFactory(t).factory(), clock)
			token, _ := downloadTestIssueToken(t, issuer, downloadTestOwner, downloadTestSandboxID, "x")

			lookup := &downloadTestLookup{target: test.target, err: test.lookupErr}
			factory := downloadTestRejectingFactory(t)
			handlers := downloadTestHandlers(t, lookup, factory.factory(), clock)
			recorder := downloadTestReceive(t, handlers, http.MethodGet, token, "")
			envelope := downloadTestTargets(t, recorder, test.wantStatus, test.wantCode)
			if test.wantCode == apierr.CodeSandboxNotReady {
				if retryable, _ := envelope.Error.Details["retryable"].(bool); retryable {
					t.Error("not-ready answer is retryable")
				}
			}
			if factory.calls.Load() != 0 {
				t.Fatal("a failed lookup dialed the node")
			}
			if calls := lookup.snapshot(); len(calls) != 1 || calls[0].Owner != downloadTestOwner {
				t.Errorf("lookup calls = %+v", calls)
			}
		})
	}
}

func TestDownloadReceiveNoBearerAndSignedClaims(t *testing.T) {
	clock := newDownloadTestClock()
	issuer := downloadTestHandlers(t, downloadTestReadyLookup(), downloadTestRejectingFactory(t).factory(), clock)
	token, _ := downloadTestIssueToken(t, issuer, downloadTestOwner, downloadTestSandboxID, "/home/user/good.txt")

	lookup := downloadTestReadyLookup()
	stream := downloadTestScriptedStream([]*nodev1.ReadFileResponse{
		downloadTestInfoMessage("/home/user/good.txt", 4),
		downloadTestDataMessage([]byte("good")),
	}, nil)
	var requestPath string
	factory := downloadTestFactory(&downloadTestNodeClient{readFile: func(ctx context.Context, request *nodev1.ReadFileRequest) (nodev1.Node_ReadFileClient, error) {
		requestPath = request.GetPath()
		stream.ctx = ctx
		return stream, nil
	}})
	handlers := downloadTestHandlers(t, lookup, factory, clock)
	request := httptest.NewRequest(http.MethodGet,
		"/api/bbraft/downloads/"+token+"?path=/etc/passwd&owner="+downloadTestOtherOwner, strings.NewReader(`{"path":"/etc/passwd"}`))
	request.SetPathValue(api.DownloadTokenParam, token)
	request.Header.Set("Authorization", "Bearer foreign-key")
	request.Header.Set("X-Org", downloadTestOtherOwner)
	recorder := httptest.NewRecorder()
	handlers.Receive(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", recorder.Code)
	}
	if got := recorder.Body.String(); got != "good" {
		t.Errorf("body = %q", got)
	}
	if requestPath != "/home/user/good.txt" {
		t.Fatalf("node path = %q, query or body rebound the signed path", requestPath)
	}
	if calls := lookup.snapshot(); len(calls) != 1 ||
		calls[0].Owner != downloadTestOwner || calls[0].SandboxID != downloadTestSandboxID {
		t.Errorf("lookup calls = %+v, want the signed owner and sandbox", calls)
	}
}

func TestDownloadReceiveNodeErrorsPreHeaders(t *testing.T) {
	tests := []struct {
		name       string
		startErr   error
		recvErr    error
		wantStatus int
		wantCode   string
	}{
		{name: "not found", recvErr: status.Error(codes.NotFound, "gone"), wantStatus: http.StatusNotFound, wantCode: apierr.CodeNotFound},
		{name: "invalid argument", recvErr: status.Error(codes.InvalidArgument, "not regular"), wantStatus: http.StatusBadRequest, wantCode: apierr.CodeInvalidPath},
		{name: "failed precondition", recvErr: status.Error(codes.FailedPrecondition, "not ready"), wantStatus: http.StatusConflict, wantCode: apierr.CodeSandboxNotReady},
		{name: "resource exhausted is not capacity", recvErr: status.Error(codes.ResourceExhausted, "full"), wantStatus: http.StatusBadGateway, wantCode: apierr.CodeSandboxDirectFailed},
		{name: "internal", recvErr: status.Error(codes.Internal, "boom"), wantStatus: http.StatusBadGateway, wantCode: apierr.CodeSandboxDirectFailed},
		{name: "unavailable", startErr: status.Error(codes.Unavailable, "down"), wantStatus: http.StatusBadGateway, wantCode: apierr.CodeSandboxDirectFailed},
		{name: "plain error", startErr: errors.New("dial failed"), wantStatus: http.StatusBadGateway, wantCode: apierr.CodeSandboxDirectFailed},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			clock := newDownloadTestClock()
			issuer := downloadTestHandlers(t, downloadTestReadyLookup(), downloadTestRejectingFactory(t).factory(), clock)
			token, _ := downloadTestIssueToken(t, issuer, downloadTestOwner, downloadTestSandboxID, "x")

			factory := downloadTestFactory(&downloadTestNodeClient{readFile: func(ctx context.Context, _ *nodev1.ReadFileRequest) (nodev1.Node_ReadFileClient, error) {
				if test.startErr != nil {
					return nil, test.startErr
				}
				stream := &downloadTestStream{recv: func() (*nodev1.ReadFileResponse, error) {
					return nil, test.recvErr
				}}
				stream.ctx = ctx
				return stream, nil
			}})
			handlers := downloadTestHandlers(t, downloadTestReadyLookup(), factory, clock)
			recorder := downloadTestReceive(t, handlers, http.MethodGet, token, "")
			envelope := downloadTestTargets(t, recorder, test.wantStatus, test.wantCode)
			if test.wantStatus == http.StatusBadGateway {
				if retryable, _ := envelope.Error.Details["retryable"].(bool); !retryable {
					t.Error("502 file read is not retryable")
				}
			}
		})
	}
}

func TestDownloadReceiveInvalidFirstMessage(t *testing.T) {
	info := func(mutate func(*nodev1.FileInfo)) *nodev1.ReadFileResponse {
		message := downloadTestInfoMessage("/home/user/x", 0)
		mutate(message.GetInfo())
		return message
	}
	tests := []struct {
		name    string
		message *nodev1.ReadFileResponse
	}{
		{name: "nil message", message: nil},
		{name: "typed nil info", message: &nodev1.ReadFileResponse{Part: (*nodev1.ReadFileResponse_Info)(nil)}},
		{name: "data before info", message: downloadTestDataMessage([]byte("x"))},
		{name: "wrong path", message: info(func(i *nodev1.FileInfo) { i.Path = "/home/user/other" })},
		{name: "wrong name", message: info(func(i *nodev1.FileInfo) { i.Name = "other" })},
		{name: "directory", message: info(func(i *nodev1.FileInfo) { i.Type = nodev1.FileType_FILE_TYPE_DIRECTORY })},
		{name: "negative size", message: info(func(i *nodev1.FileInfo) { i.Size = -1 })},
		{name: "mode above 07777", message: info(func(i *nodev1.FileInfo) { i.Mode = 0o10000 })},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			clock := newDownloadTestClock()
			issuer := downloadTestHandlers(t, downloadTestReadyLookup(), downloadTestRejectingFactory(t).factory(), clock)
			token, _ := downloadTestIssueToken(t, issuer, downloadTestOwner, downloadTestSandboxID, "/home/user/x")

			stream := downloadTestScriptedStream([]*nodev1.ReadFileResponse{test.message}, nil)
			factory := downloadTestFactory(&downloadTestNodeClient{readFile: func(ctx context.Context, _ *nodev1.ReadFileRequest) (nodev1.Node_ReadFileClient, error) {
				stream.ctx = ctx
				return stream, nil
			}})
			handlers := downloadTestHandlers(t, downloadTestReadyLookup(), factory, clock)
			recorder := downloadTestReceive(t, handlers, http.MethodGet, token, "")
			downloadTestTargets(t, recorder, http.StatusBadGateway, apierr.CodeSandboxDirectFailed)
			if recorder.Body.Len() > 0 && !strings.Contains(recorder.Body.String(), `"ok":false`) {
				t.Fatal("pre-header failure wrote non-envelope bytes")
			}
		})
	}
}

// TestDownloadReceiveTypedNilDataPart proves a malformed oneof wrapper is a
// protocol failure, never a nil-pointer panic.
func TestDownloadReceiveTypedNilDataPart(t *testing.T) {
	clock := newDownloadTestClock()
	issuer := downloadTestHandlers(t, downloadTestReadyLookup(), downloadTestRejectingFactory(t).factory(), clock)
	token, _ := downloadTestIssueToken(t, issuer, downloadTestOwner, downloadTestSandboxID, "/home/user/x")

	stream := downloadTestScriptedStream([]*nodev1.ReadFileResponse{
		downloadTestInfoMessage("/home/user/x", 100),
		downloadTestDataMessage([]byte("ok")),
		{Part: (*nodev1.ReadFileResponse_Data)(nil)},
	}, nil)
	handlers := downloadTestHandlers(t, downloadTestReadyLookup(), downloadTestFactory(&downloadTestNodeClient{
		readFile: func(ctx context.Context, _ *nodev1.ReadFileRequest) (nodev1.Node_ReadFileClient, error) {
			stream.ctx = ctx
			return stream, nil
		},
	}), clock)
	writer := newDownloadTestWriter()
	downloadTestExpectAbort(t, func() {
		request := httptest.NewRequest(http.MethodGet, "/api/bbraft/downloads/"+token, nil)
		request.SetPathValue(api.DownloadTokenParam, token)
		handlers.Receive(writer, request)
	})
	if got := writer.recorder.Body.String(); got != "ok" {
		t.Fatalf("body = %q, want the chunk before the malformed part", got)
	}
}

func TestDownloadReceiveToleratesChangingSizeAndOldTimestamp(t *testing.T) {
	tests := []struct {
		name     string
		infoSize int64
		mtime    int64
		data     []byte
	}{
		{name: "shrunk", infoSize: 1 << 20, mtime: -1, data: []byte("short")},
		{name: "grown from empty", infoSize: 0, mtime: 0, data: []byte("appeared")},
		{name: "same", infoSize: 4, mtime: 1234, data: []byte("same")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			clock := newDownloadTestClock()
			info := downloadTestInfoMessage("/home/user/x", test.infoSize)
			info.GetInfo().ModifiedAtUnixMs = test.mtime
			stream := downloadTestScriptedStream([]*nodev1.ReadFileResponse{info, downloadTestDataMessage(test.data)}, nil)
			handlers := downloadTestHandlers(t, downloadTestReadyLookup(), downloadTestFactory(&downloadTestNodeClient{
				readFile: func(ctx context.Context, _ *nodev1.ReadFileRequest) (nodev1.Node_ReadFileClient, error) {
					stream.ctx = ctx
					return stream, nil
				},
			}), clock)
			token, _ := downloadTestIssueToken(t, handlers, downloadTestOwner, downloadTestSandboxID, "/home/user/x")
			recorder := downloadTestReceive(t, handlers, http.MethodGet, token, "")
			if recorder.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200", recorder.Code)
			}
			if !bytes.Equal(recorder.Body.Bytes(), test.data) {
				t.Fatalf("body = %q, want %q", recorder.Body.Bytes(), test.data)
			}
		})
	}
}

// downloadTestStartAbortingServer runs the receiver on a real net/http server.
// The test reads the response and expects the transport error of an aborted
// body, never a clean EOF.
func downloadTestStartAbortingServer(t *testing.T, handlers api.DownloadsHandlers, token string) (body []byte, status int, readErr error) {
	t.Helper()
	server := downloadTestServe(t, handlers.Receive, "GET "+api.DownloadReceiverPath)
	response, err := server.Client().Get(server.URL + "/api/bbraft/downloads/" + token)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer response.Body.Close()
	body, readErr = io.ReadAll(response.Body)
	return body, response.StatusCode, readErr
}

func TestDownloadReceiveMidstreamAbort(t *testing.T) {
	tests := []struct {
		name     string
		messages []*nodev1.ReadFileResponse
		terminal error
		wantBody string
	}{
		{
			name: "node failure",
			messages: []*nodev1.ReadFileResponse{
				downloadTestInfoMessage("/home/user/x", 100),
				downloadTestDataMessage([]byte("partial")),
			},
			terminal: status.Error(codes.Internal, "node died"),
			wantBody: "partial",
		},
		{
			name: "repeated info",
			messages: []*nodev1.ReadFileResponse{
				downloadTestInfoMessage("/home/user/x", 100),
				downloadTestDataMessage([]byte("once")),
				downloadTestInfoMessage("/home/user/x", 100),
			},
			wantBody: "once",
		},
		{
			name: "oversized chunk",
			messages: []*nodev1.ReadFileResponse{
				downloadTestInfoMessage("/home/user/x", 1<<20+1),
				downloadTestDataMessage([]byte("ok")),
				downloadTestDataMessage(bytes.Repeat([]byte{'x'}, 1<<20+1)),
			},
			wantBody: "ok",
		},
		{
			name: "nil part",
			messages: []*nodev1.ReadFileResponse{
				downloadTestInfoMessage("/home/user/x", 100),
				downloadTestDataMessage([]byte("ok")),
				{},
			},
			wantBody: "ok",
		},
		{
			name: "nil response instead of EOF",
			messages: []*nodev1.ReadFileResponse{
				downloadTestInfoMessage("/home/user/x", 100),
				downloadTestDataMessage([]byte("ok")),
			},
			terminal: nil,
			wantBody: "ok",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// A nil terminal in this table means "return a nil message and nil
			// error", which is not EOF and must abort. The scripted helper
			// uses nil for EOF, so build the stream explicitly.
			var index int
			stream := &downloadTestStream{}
			stream.recv = func() (*nodev1.ReadFileResponse, error) {
				if index < len(test.messages) {
					message := test.messages[index]
					index++
					return message, nil
				}
				if test.terminal != nil {
					return nil, test.terminal
				}
				if test.name == "nil response instead of EOF" {
					return nil, nil
				}
				return nil, io.EOF
			}
			handlers := downloadTestHandlers(t, downloadTestReadyLookup(), downloadTestFactory(&downloadTestNodeClient{
				readFile: func(ctx context.Context, _ *nodev1.ReadFileRequest) (nodev1.Node_ReadFileClient, error) {
					stream.ctx = ctx
					return stream, nil
				},
			}), newDownloadTestClock())
			token, _ := downloadTestIssueToken(t, handlers, downloadTestOwner, downloadTestSandboxID, "/home/user/x")
			body, status, err := downloadTestStartAbortingServer(t, handlers, token)
			if status != http.StatusOK {
				t.Fatalf("status = %d, want 200", status)
			}
			if err == nil {
				t.Fatal("aborted body read returned a clean EOF")
			}
			if string(body) != test.wantBody {
				t.Fatalf("body = %q, want %q", body, test.wantBody)
			}
			if bytes.Contains(body, []byte(`"ok":false`)) || bytes.Contains(body, []byte(`"type":"sandbox.error"`)) {
				t.Fatal("JSON error was appended to a download stream")
			}
		})
	}
}

func TestDownloadIssueCanceledDoesNotRespond(t *testing.T) {
	lookup := downloadTestReadyLookup()
	handlers := downloadTestHandlers(t, lookup, downloadTestRejectingFactory(t).factory(), newDownloadTestClock())
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	request := httptest.NewRequest(http.MethodPost, "/api/v1/sandboxes/"+downloadTestSandboxID+"/files/download-url",
		strings.NewReader(`{"path":"x"}`))
	request.SetPathValue(api.ParamSandboxID, downloadTestSandboxID)
	request.Header.Set("Content-Type", "application/json")
	request = request.WithContext(withDownloadTestIdentity(request.WithContext(ctx), downloadTestOwner))
	recorder := httptest.NewRecorder()
	handlers.Issue(recorder, request)
	if recorder.Body.Len() != 0 {
		t.Fatal("issue wrote a response after the request was canceled")
	}
	if calls := lookup.snapshot(); len(calls) != 0 {
		t.Errorf("lookup calls = %d, want 0: entry cancellation must skip the lookup", len(calls))
	}
}

// TestDownloadIssueCanceledDuringLookup proves the post-lookup cancellation
// path independently: the lookup ran, then its cancel must suppress the token
// and response.
func TestDownloadIssueCanceledDuringLookup(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	lookup := &downloadTestCancelingLookup{
		target: api.CommandTarget{NodeID: downloadTestNodeID, State: api.StateReady},
		cancel: cancel,
	}
	handlers := downloadTestHandlers(t, lookup, downloadTestRejectingFactory(t).factory(), newDownloadTestClock())
	request := httptest.NewRequest(http.MethodPost, "/api/v1/sandboxes/"+downloadTestSandboxID+"/files/download-url",
		strings.NewReader(`{"path":"x"}`))
	request.SetPathValue(api.ParamSandboxID, downloadTestSandboxID)
	request.Header.Set("Content-Type", "application/json")
	request = request.WithContext(withDownloadTestIdentity(request.WithContext(ctx), downloadTestOwner))
	recorder := httptest.NewRecorder()
	handlers.Issue(recorder, request)
	if recorder.Body.Len() != 0 {
		t.Fatal("issue wrote a response after cancellation during the lookup")
	}
	if lookup.count() != 1 {
		t.Fatalf("lookup calls = %d, want 1", lookup.count())
	}
}

// downloadTestCanceledReceiverCase covers cancellation points before any HTTP
// header or node RPC: at entry, during the target lookup, after the client
// factory, after ReadFile opens and after the first Recv.
func TestDownloadReceiveCanceledBeforeHeadersOrRPC(t *testing.T) {
	tests := []struct {
		name           string
		cancelBefore   bool
		cancelInLookup bool
		cancelInDial   bool
		cancelOnOpen   bool
		cancelInFirst  bool
		lookupErr      error
	}{
		{name: "before lookup", cancelBefore: true},
		{name: "after lookup", cancelInLookup: true},
		{name: "lookup fault after cancel", cancelInLookup: true, lookupErr: errors.New("db unavailable")},
		{name: "after factory", cancelInDial: true},
		{name: "after readfile open", cancelOnOpen: true},
		{name: "after first recv", cancelInFirst: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			clock := newDownloadTestClock()
			issuer := downloadTestHandlers(t, downloadTestReadyLookup(), downloadTestRejectingFactory(t).factory(), clock)
			token, _ := downloadTestIssueToken(t, issuer, downloadTestOwner, downloadTestSandboxID, "/home/user/x")

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if test.cancelBefore {
				cancel()
			}
			target := api.CommandTarget{NodeID: downloadTestNodeID, State: api.StateReady}
			var lookup api.CommandTargetLookup
			var lookupCalls func() int
			if test.cancelInLookup {
				canceling := &downloadTestCancelingLookup{target: target, cancel: cancel, err: test.lookupErr}
				lookup = canceling
				lookupCalls = canceling.count
			} else {
				normal := &downloadTestLookup{target: target, err: test.lookupErr}
				lookup = normal
				lookupCalls = func() int { return normal.count() }
			}
			var reads atomic.Int64
			var recvs atomic.Int64
			closes := atomic.Int64{}
			factory := func(string) (nodev1.NodeClient, func() error, error) {
				if test.cancelInDial {
					cancel()
				}
				stream := &downloadTestStream{recv: func() (*nodev1.ReadFileResponse, error) {
					recvs.Add(1)
					if test.cancelInFirst {
						cancel()
						return nil, context.Canceled
					}
					return downloadTestInfoMessage("/home/user/x", 0), nil
				}}
				return &downloadTestNodeClient{readFile: func(streamCtx context.Context, _ *nodev1.ReadFileRequest) (nodev1.Node_ReadFileClient, error) {
					reads.Add(1)
					if test.cancelOnOpen {
						cancel()
					}
					stream.ctx = streamCtx
					return stream, nil
				}}, func() error { closes.Add(1); return nil }, nil
			}
			handlers := downloadTestHandlers(t, lookup, factory, clock)
			request := httptest.NewRequest(http.MethodGet, "/api/bbraft/downloads/"+token, nil)
			request.SetPathValue(api.DownloadTokenParam, token)
			request = request.WithContext(ctx)
			recorder := httptest.NewRecorder()
			handlers.Receive(recorder, request)
			if recorder.Body.Len() != 0 {
				t.Fatalf("receiver wrote %d bytes after cancellation", recorder.Body.Len())
			}
			if test.cancelBefore {
				if lookupCalls() != 0 {
					t.Errorf("lookup calls = %d, want 0 at entry cancellation", lookupCalls())
				}
			} else if lookupCalls() != 1 {
				t.Errorf("lookup calls = %d, want 1", lookupCalls())
			}
			if (test.cancelBefore || test.cancelInDial) && reads.Load() != 0 {
				t.Fatalf("receiver started ReadFile after the request was canceled")
			}
			if test.cancelOnOpen {
				if reads.Load() != 1 {
					t.Fatalf("ReadFile opens = %d, want 1", reads.Load())
				}
				if recvs.Load() != 0 {
					t.Fatalf("Recv calls = %d, want 0 after cancellation during open", recvs.Load())
				}
				if closes.Load() != 1 {
					t.Fatalf("node client closes = %d, want 1", closes.Load())
				}
			}
			if test.cancelInFirst && recvs.Load() != 1 {
				t.Fatalf("Recv calls = %d, want 1", recvs.Load())
			}
			if test.cancelInDial && closes.Load() != 1 {
				t.Fatalf("node client closes = %d, want 1 after cancellation", closes.Load())
			}
			if test.cancelInLookup && test.lookupErr != nil && reads.Load() != 0 {
				t.Fatal("a canceled lookup error still dialed the node")
			}
		})
	}
}

func TestDownloadReceiveServerContextCancelAfterHeadersAborts(t *testing.T) {
	clock := newDownloadTestClock()
	issuer := downloadTestHandlers(t, downloadTestReadyLookup(), downloadTestRejectingFactory(t).factory(), clock)
	token, _ := downloadTestIssueToken(t, issuer, downloadTestOwner, downloadTestSandboxID, "/home/user/x")

	tests := []struct {
		name string
		late func(*downloadTestStream) (*nodev1.ReadFileResponse, error)
	}{
		{name: "recv returns context error", late: func(stream *downloadTestStream) (*nodev1.ReadFileResponse, error) {
			<-stream.ctx.Done()
			return nil, stream.ctx.Err()
		}},
		{name: "recv returns EOF after cancel", late: func(stream *downloadTestStream) (*nodev1.ReadFileResponse, error) {
			<-stream.ctx.Done()
			return nil, io.EOF
		}},
		{name: "recv returns data after cancel", late: func(stream *downloadTestStream) (*nodev1.ReadFileResponse, error) {
			<-stream.ctx.Done()
			return downloadTestDataMessage([]byte("late")), nil
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recvBlocked := make(chan struct{})
			cancelNow := make(chan struct{})
			stream := &downloadTestStream{}
			var index int
			stream.recv = func() (*nodev1.ReadFileResponse, error) {
				switch index {
				case 0:
					index++
					return downloadTestInfoMessage("/home/user/x", 7), nil
				case 1:
					index++
					return downloadTestDataMessage([]byte("partial")), nil
				default:
					close(recvBlocked)
					return test.late(stream)
				}
			}
			handlers := downloadTestHandlers(t, downloadTestReadyLookup(), downloadTestFactory(&downloadTestNodeClient{
				readFile: func(ctx context.Context, _ *nodev1.ReadFileRequest) (nodev1.Node_ReadFileClient, error) {
					stream.ctx = ctx
					return stream, nil
				},
			}), clock)

			handlerDone := make(chan struct{})
			mux := http.NewServeMux()
			mux.HandleFunc("GET "+api.DownloadReceiverPath, func(w http.ResponseWriter, r *http.Request) {
				// Do not recover: net/http's ErrAbortHandler handling is what
				// aborts the connection. The deferred close still runs.
				defer close(handlerDone)
				ctx, cancel := context.WithCancel(r.Context())
				defer cancel()
				go func() {
					select {
					case <-cancelNow:
						cancel()
					case <-ctx.Done():
					}
				}()
				handlers.Receive(w, r.WithContext(ctx))
			})
			server := httptest.NewServer(mux)
			t.Cleanup(server.Close)

			response, err := server.Client().Get(server.URL + "/api/bbraft/downloads/" + token)
			if err != nil {
				t.Fatalf("GET: %v", err)
			}
			defer response.Body.Close()
			select {
			case <-recvBlocked:
			case <-time.After(5 * time.Second):
				t.Fatal("handler never blocked in Recv after the partial flush")
			}
			close(cancelNow)
			body, readErr := io.ReadAll(response.Body)
			if readErr == nil {
				t.Fatal("server-side cancellation after headers looked like a clean EOF")
			}
			if string(body) != "partial" {
				t.Fatalf("body = %q, want only the flushed partial data", body)
			}
			if bytes.Contains(body, []byte(`"ok":false`)) || bytes.Contains(body, []byte(`"type":"sandbox.error"`)) {
				t.Fatal("JSON error was appended to the download stream")
			}
			select {
			case <-handlerDone:
			case <-time.After(5 * time.Second):
				t.Fatal("handler did not abort after the server canceled the request context")
			}
		})
	}
}

func TestDownloadReceiveNilStream(t *testing.T) {
	clock := newDownloadTestClock()
	issuer := downloadTestHandlers(t, downloadTestReadyLookup(), downloadTestRejectingFactory(t).factory(), clock)
	token, _ := downloadTestIssueToken(t, issuer, downloadTestOwner, downloadTestSandboxID, "/home/user/x")

	handlers := downloadTestHandlers(t, downloadTestReadyLookup(), downloadTestFactory(&downloadTestNodeClient{
		readFile: func(context.Context, *nodev1.ReadFileRequest) (nodev1.Node_ReadFileClient, error) {
			return nil, nil
		},
	}), clock)
	recorder := downloadTestReceive(t, handlers, http.MethodGet, token, "")
	downloadTestTargets(t, recorder, http.StatusBadGateway, apierr.CodeSandboxDirectFailed)
}

func TestDownloadReceiveWriteDeadlineErrors(t *testing.T) {
	setErr := errors.New("deadline setter failed")
	tests := []struct {
		name        string
		fail        func(call int, deadline time.Time) bool
		wantWrites  int
		wantFlushes int
	}{
		{name: "first arm", fail: func(call int, deadline time.Time) bool {
			return call == 1 && !deadline.IsZero()
		}, wantWrites: 0, wantFlushes: 0},
		{name: "second arm before flush", fail: func(call int, deadline time.Time) bool {
			return call == 2 && !deadline.IsZero()
		}, wantWrites: 1, wantFlushes: 0},
		{name: "clear failure", fail: func(call int, deadline time.Time) bool {
			return call == 3 && deadline.IsZero()
		}, wantWrites: 1, wantFlushes: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			clock := newDownloadTestClock()
			issuer := downloadTestHandlers(t, downloadTestReadyLookup(), downloadTestRejectingFactory(t).factory(), clock)
			token, _ := downloadTestIssueToken(t, issuer, downloadTestOwner, downloadTestSandboxID, "/home/user/x")

			stream := downloadTestScriptedStream([]*nodev1.ReadFileResponse{
				downloadTestInfoMessage("/home/user/x", 8),
				downloadTestDataMessage([]byte("data")),
				downloadTestDataMessage([]byte("more")),
			}, nil)
			handlers := downloadTestHandlers(t, downloadTestReadyLookup(), downloadTestFactory(&downloadTestNodeClient{
				readFile: func(ctx context.Context, _ *nodev1.ReadFileRequest) (nodev1.Node_ReadFileClient, error) {
					stream.ctx = ctx
					return stream, nil
				},
			}), clock)

			writer := newDownloadTestWriter()
			var (
				calls   int
				writes  int
				flushes int
			)
			writer.setDeadline = func(deadline time.Time) error {
				calls++
				if test.fail(calls, deadline) {
					return setErr
				}
				return nil
			}
			writer.write = func(p []byte) (int, error) {
				writes++
				return writer.recorder.Write(p)
			}
			writer.flush = func() error {
				flushes++
				return nil
			}
			downloadTestExpectAbort(t, func() {
				request := httptest.NewRequest(http.MethodGet, "/api/bbraft/downloads/"+token, nil)
				request.SetPathValue(api.DownloadTokenParam, token)
				handlers.Receive(writer, request)
			})
			select {
			case <-stream.ctx.Done():
			case <-time.After(5 * time.Second):
				t.Fatal("deadline failure did not cancel the node RPC")
			}
			if writes != test.wantWrites {
				t.Errorf("writes = %d, want %d", writes, test.wantWrites)
			}
			if flushes != test.wantFlushes {
				t.Errorf("flushes = %d, want %d", flushes, test.wantFlushes)
			}
			if got := stream.recvCalls(); got != 2 {
				t.Errorf("Recv calls = %d, want 2 (no extra Recv after the failure)", got)
			}
		})
	}
}

func TestDownloadReceiveNoStoreOnErrors(t *testing.T) {
	clock := newDownloadTestClock()
	issuer := downloadTestHandlers(t, downloadTestReadyLookup(), downloadTestRejectingFactory(t).factory(), clock)
	token, _ := downloadTestIssueToken(t, issuer, downloadTestOwner, downloadTestSandboxID, "/home/user/x")

	t.Run("wrong method", func(t *testing.T) {
		lookup := downloadTestReadyLookup()
		handlers := downloadTestHandlers(t, lookup, downloadTestRejectingFactory(t).factory(), clock)
		recorder := downloadTestReceive(t, handlers, http.MethodPost, token, "")
		downloadTestTargets(t, recorder, http.StatusMethodNotAllowed, apierr.CodeNotFound)
		if got := recorder.Header().Get("Cache-Control"); got != "no-store" {
			t.Errorf("Cache-Control = %q, want no-store", got)
		}
	})
	t.Run("invalid token", func(t *testing.T) {
		handlers := downloadTestHandlers(t, downloadTestReadyLookup(), downloadTestRejectingFactory(t).factory(), clock)
		recorder := downloadTestReceive(t, handlers, http.MethodGet, "not-a-token", "")
		downloadTestTargets(t, recorder, http.StatusNotFound, apierr.CodeNotFound)
		if got := recorder.Header().Get("Cache-Control"); got != "no-store" {
			t.Errorf("Cache-Control = %q, want no-store", got)
		}
	})
	t.Run("lookup failure", func(t *testing.T) {
		lookup := &downloadTestLookup{err: errors.New("db unavailable")}
		handlers := downloadTestHandlers(t, lookup, downloadTestRejectingFactory(t).factory(), clock)
		recorder := downloadTestReceive(t, handlers, http.MethodGet, token, "")
		downloadTestTargets(t, recorder, http.StatusInternalServerError, apierr.CodeInternal)
		if got := recorder.Header().Get("Cache-Control"); got != "no-store" {
			t.Errorf("Cache-Control = %q, want no-store", got)
		}
	})
}

func TestDownloadReceiveClientDisconnectCancelsReadFile(t *testing.T) {
	clock := newDownloadTestClock()
	issuer := downloadTestHandlers(t, downloadTestReadyLookup(), downloadTestRejectingFactory(t).factory(), clock)
	token, _ := downloadTestIssueToken(t, issuer, downloadTestOwner, downloadTestSandboxID, "/home/user/x")

	started := make(chan struct{})
	done := make(chan struct{})
	var once sync.Once
	stream := &downloadTestStream{}
	var index int
	stream.recv = func() (*nodev1.ReadFileResponse, error) {
		switch index {
		case 0:
			index++
			return downloadTestInfoMessage("/home/user/x", 1), nil
		case 1:
			index++
			return downloadTestDataMessage([]byte("x")), nil
		default:
			once.Do(func() { close(started) })
			<-stream.ctx.Done()
			close(done)
			return nil, stream.ctx.Err()
		}
	}
	var closes atomic.Int64
	factory := func(string) (nodev1.NodeClient, func() error, error) {
		return &downloadTestNodeClient{readFile: func(ctx context.Context, _ *nodev1.ReadFileRequest) (nodev1.Node_ReadFileClient, error) {
			stream.ctx = ctx
			return stream, nil
		}}, func() error { closes.Add(1); return nil }, nil
	}
	handlers := downloadTestHandlers(t, downloadTestReadyLookup(), factory, clock)
	handlerDone := make(chan struct{})
	mux := http.NewServeMux()
	mux.HandleFunc("GET "+api.DownloadReceiverPath, func(w http.ResponseWriter, r *http.Request) {
		// Do not recover: net/http's ErrAbortHandler handling is what aborts
		// the connection. The deferred close still runs while unwinding.
		defer close(handlerDone)
		handlers.Receive(w, r)
	})
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	response, err := server.Client().Get(server.URL + "/api/bbraft/downloads/" + token)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	// Read the first flushed chunk, then drop the connection.
	one := make([]byte, 1)
	if _, err := io.ReadFull(response.Body, one); err != nil {
		response.Body.Close()
		t.Fatalf("read first byte: %v", err)
	}
	response.Body.Close()

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("handler never reached the blocking node Recv")
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("node stream was not canceled after the client disconnected")
	}
	// done closes inside Recv, before the handler's deferred client Close
	// runs. Join the actual handler before asserting the close count.
	select {
	case <-handlerDone:
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not return after the client disconnected")
	}
	if closes.Load() != 1 {
		t.Fatalf("node client closes = %d, want 1", closes.Load())
	}
}

func TestDownloadReceiveBackpressure(t *testing.T) {
	clock := newDownloadTestClock()
	issuer := downloadTestHandlers(t, downloadTestReadyLookup(), downloadTestRejectingFactory(t).factory(), clock)
	token, _ := downloadTestIssueToken(t, issuer, downloadTestOwner, downloadTestSandboxID, "/home/user/x")

	stream := downloadTestScriptedStream([]*nodev1.ReadFileResponse{
		downloadTestInfoMessage("/home/user/x", 6),
		downloadTestDataMessage([]byte("one")),
		downloadTestDataMessage([]byte("two")),
	}, nil)
	handlers := downloadTestHandlers(t, downloadTestReadyLookup(), downloadTestFactory(&downloadTestNodeClient{
		readFile: func(ctx context.Context, _ *nodev1.ReadFileRequest) (nodev1.Node_ReadFileClient, error) {
			stream.ctx = ctx
			return stream, nil
		},
	}), clock)

	writer := newDownloadTestWriter()
	entered := make(chan struct{})
	release := make(chan struct{})
	var gate sync.Once
	var releaseOnce sync.Once
	releaseAll := func() { releaseOnce.Do(func() { close(release) }) }
	writer.write = func(p []byte) (int, error) {
		gate.Do(func() {
			close(entered)
			<-release
		})
		return writer.recorder.Write(p)
	}
	result := make(chan struct{})
	go func() {
		defer close(result)
		defer func() { _ = recover() }() // keep a failed assertion from leaking the goroutine
		request := httptest.NewRequest(http.MethodGet, "/api/bbraft/downloads/"+token, nil)
		request.SetPathValue(api.DownloadTokenParam, token)
		handlers.Receive(writer, request)
	}()
	// Every failure path below must release the gate and join the handler.
	t.Cleanup(func() {
		releaseAll()
		select {
		case <-result:
		case <-time.After(5 * time.Second):
			t.Error("handler did not finish during cleanup")
		}
	})

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("handler never blocked in Write")
	}
	if got := stream.recvCalls(); got != 2 {
		t.Fatalf("Recv calls while Write is blocked = %d, want 2 (info + one data chunk)", got)
	}
	releaseAll()
	select {
	case <-result:
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not finish after Write was released")
	}
	if got := writer.recorder.Body.String(); got != "onetwo" {
		t.Fatalf("body = %q, want onetwo", got)
	}
	if writer.recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", writer.recorder.Code)
	}
}

func TestDownloadReceiveWriteAndFlushErrorsStopStream(t *testing.T) {
	writeErr := errors.New("client write failed")
	flushErr := errors.New("client flush failed")
	tests := []struct {
		name  string
		write func(*downloadTestResponseWriter)
	}{
		{name: "write", write: func(w *downloadTestResponseWriter) {
			w.write = func([]byte) (int, error) { return 0, writeErr }
		}},
		{name: "short write", write: func(w *downloadTestResponseWriter) {
			w.write = func(p []byte) (int, error) { return len(p) - 1, nil }
		}},
		{name: "flush", write: func(w *downloadTestResponseWriter) {
			w.flush = func() error { return flushErr }
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			clock := newDownloadTestClock()
			issuer := downloadTestHandlers(t, downloadTestReadyLookup(), downloadTestRejectingFactory(t).factory(), clock)
			token, _ := downloadTestIssueToken(t, issuer, downloadTestOwner, downloadTestSandboxID, "/home/user/x")

			stream := downloadTestScriptedStream([]*nodev1.ReadFileResponse{
				downloadTestInfoMessage("/home/user/x", 100),
				downloadTestDataMessage([]byte("data")),
				downloadTestDataMessage([]byte("more")),
			}, nil)
			handlers := downloadTestHandlers(t, downloadTestReadyLookup(), downloadTestFactory(&downloadTestNodeClient{
				readFile: func(ctx context.Context, _ *nodev1.ReadFileRequest) (nodev1.Node_ReadFileClient, error) {
					stream.ctx = ctx
					return stream, nil
				},
			}), clock)
			writer := newDownloadTestWriter()
			test.write(writer)
			downloadTestExpectAbort(t, func() {
				request := httptest.NewRequest(http.MethodGet, "/api/bbraft/downloads/"+token, nil)
				request.SetPathValue(api.DownloadTokenParam, token)
				handlers.Receive(writer, request)
			})
			select {
			case <-stream.ctx.Done():
			case <-time.After(5 * time.Second):
				t.Fatal("write/flush failure did not cancel the node RPC")
			}
			if got := stream.recvCalls(); got != 2 {
				t.Fatalf("Recv calls after the failure = %d, want 2", got)
			}
		})
	}
}

func TestDownloadReceiveFreshWriteDeadline(t *testing.T) {
	clock := newDownloadTestClock()
	issuer := downloadTestHandlers(t, downloadTestReadyLookup(), downloadTestRejectingFactory(t).factory(), clock)
	token, _ := downloadTestIssueToken(t, issuer, downloadTestOwner, downloadTestSandboxID, "/home/user/x")

	stream := downloadTestScriptedStream([]*nodev1.ReadFileResponse{
		downloadTestInfoMessage("/home/user/x", 6),
		downloadTestDataMessage([]byte("one")),
		downloadTestDataMessage([]byte("two")),
	}, nil)
	handlers := downloadTestHandlers(t, downloadTestReadyLookup(), downloadTestFactory(&downloadTestNodeClient{
		readFile: func(ctx context.Context, _ *nodev1.ReadFileRequest) (nodev1.Node_ReadFileClient, error) {
			stream.ctx = ctx
			return stream, nil
		},
	}), clock, func(o *api.DownloadsOptions) { o.WriteDeadline = 50 * time.Millisecond })

	var deadlines []time.Time
	var clears int
	writer := newDownloadTestWriter()
	writer.setDeadline = func(deadline time.Time) error {
		if deadline.IsZero() {
			clears++
			return nil
		}
		deadlines = append(deadlines, deadline)
		return nil
	}
	start := time.Now()
	request := httptest.NewRequest(http.MethodGet, "/api/bbraft/downloads/"+token, nil)
	request.SetPathValue(api.DownloadTokenParam, token)
	handlers.Receive(writer, request)
	if writer.recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", writer.recorder.Code)
	}
	if len(deadlines) != 4 {
		t.Fatalf("armed deadlines = %d, want a fresh bound before each Write and Flush (4)", len(deadlines))
	}
	if clears != 2 {
		t.Fatalf("deadline clears = %d, want one after each chunk (2)", clears)
	}
	for _, deadline := range deadlines {
		if deadline.Before(start.Add(50*time.Millisecond)) || deadline.After(time.Now().Add(50*time.Millisecond+time.Second)) {
			t.Fatalf("deadline %v is not a fresh 50ms bound", deadline)
		}
	}
}

// downloadTestDeadlineTracker records whether a write deadline is armed
// around each Write, Flush and node Recv. A deadline must never be active
// while the handler is only waiting on the node.
type downloadTestDeadlineTracker struct {
	mu             sync.Mutex
	active         bool
	deadlines      int
	inactiveWrites int
	inactiveFlush  int
	recvActive     []bool
}

func (d *downloadTestDeadlineTracker) set(deadline time.Time) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if deadline.IsZero() {
		d.active = false
		return nil
	}
	d.active = true
	d.deadlines++
	return nil
}

func (d *downloadTestDeadlineTracker) recordWrite() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.active {
		d.inactiveWrites++
	}
}

func (d *downloadTestDeadlineTracker) recordFlush() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.active {
		d.inactiveFlush++
	}
}

func (d *downloadTestDeadlineTracker) recordRecv() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.recvActive = append(d.recvActive, d.active)
}

func (d *downloadTestDeadlineTracker) result() (deadlines, inactiveWrites, inactiveFlush int, recvActive []bool, active bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.deadlines, d.inactiveWrites, d.inactiveFlush, append([]bool(nil), d.recvActive...), d.active
}

func TestDownloadReceiveWriteDeadlineClearedBeforeRecv(t *testing.T) {
	clock := newDownloadTestClock()
	issuer := downloadTestHandlers(t, downloadTestReadyLookup(), downloadTestRejectingFactory(t).factory(), clock)
	token, _ := downloadTestIssueToken(t, issuer, downloadTestOwner, downloadTestSandboxID, "/home/user/x")

	tracker := &downloadTestDeadlineTracker{}
	stream := &downloadTestStream{}
	var index int
	stream.recv = func() (*nodev1.ReadFileResponse, error) {
		tracker.recordRecv()
		messages := []*nodev1.ReadFileResponse{
			downloadTestInfoMessage("/home/user/x", 6),
			downloadTestDataMessage([]byte("one")),
			downloadTestDataMessage([]byte("two")),
		}
		if index < len(messages) {
			message := messages[index]
			index++
			return message, nil
		}
		return nil, io.EOF
	}
	handlers := downloadTestHandlers(t, downloadTestReadyLookup(), downloadTestFactory(&downloadTestNodeClient{
		readFile: func(ctx context.Context, _ *nodev1.ReadFileRequest) (nodev1.Node_ReadFileClient, error) {
			stream.ctx = ctx
			return stream, nil
		},
	}), clock, func(o *api.DownloadsOptions) { o.WriteDeadline = 50 * time.Millisecond })

	writer := newDownloadTestWriter()
	writer.setDeadline = tracker.set
	writer.write = func(p []byte) (int, error) {
		tracker.recordWrite()
		return writer.recorder.Write(p)
	}
	writer.flush = func() error {
		tracker.recordFlush()
		return nil
	}
	request := httptest.NewRequest(http.MethodGet, "/api/bbraft/downloads/"+token, nil)
	request.SetPathValue(api.DownloadTokenParam, token)
	handlers.Receive(writer, request)
	if writer.recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", writer.recorder.Code)
	}

	deadlines, inactiveWrites, inactiveFlush, recvActive, active := tracker.result()
	if deadlines != 4 {
		t.Errorf("deadline sets = %d, want 4 (fresh bound before each Write and Flush)", deadlines)
	}
	if inactiveWrites != 0 {
		t.Error("a Write ran without an armed deadline")
	}
	if inactiveFlush != 0 {
		t.Error("a Flush ran without an armed deadline")
	}
	for i, armed := range recvActive {
		if armed {
			t.Errorf("deadline was still armed at Recv %d", i+1)
		}
	}
	if active {
		t.Error("deadline was left armed after the handler returned")
	}
}

func TestNewDownloadsHandlersDependencies(t *testing.T) {
	lookup := downloadTestReadyLookup()
	factory := downloadTestRejectingFactory(t).factory()
	if _, err := api.NewDownloadsHandlers(api.DownloadsOptions{Clients: factory, SigningKey: downloadTestKey, BaseURL: downloadTestBaseURL}); err == nil {
		t.Error("nil lookup accepted")
	}
	if _, err := api.NewDownloadsHandlers(api.DownloadsOptions{Lookup: lookup, SigningKey: downloadTestKey, BaseURL: downloadTestBaseURL}); err == nil {
		t.Error("nil client factory accepted")
	}
	if _, err := api.NewDownloadsHandlers(api.DownloadsOptions{Lookup: lookup, Clients: factory, SigningKey: []byte("short"), BaseURL: downloadTestBaseURL}); err == nil {
		t.Error("short signing key accepted")
	}
	for _, base := range []string{
		"", "api.example.test", "ftp://api.example.test", "https://", "https://:443", "://bad",
		"https://api.example.test?x=1", "https://api.example.test#fragment", "https://user@api.example.test",
	} {
		if _, err := api.NewDownloadsHandlers(api.DownloadsOptions{Lookup: lookup, Clients: factory, SigningKey: downloadTestKey, BaseURL: base}); err == nil {
			t.Errorf("BaseURL %q accepted", base)
		}
	}
	if _, err := api.NewDownloadsHandlers(api.DownloadsOptions{
		Lookup: lookup, Clients: factory, SigningKey: downloadTestKey, BaseURL: downloadTestBaseURL, WriteDeadline: -time.Second,
	}); err == nil {
		t.Error("negative write deadline accepted")
	}
}

// downloadTestResponseWriter is a ResponseWriter with injectable write, flush
// and deadline behaviour for the streaming edge cases.
type downloadTestResponseWriter struct {
	recorder    *httptest.ResponseRecorder
	write       func([]byte) (int, error)
	flush       func() error
	setDeadline func(time.Time) error
}

func newDownloadTestWriter() *downloadTestResponseWriter {
	return &downloadTestResponseWriter{recorder: httptest.NewRecorder()}
}

func (w *downloadTestResponseWriter) Header() http.Header {
	return w.recorder.Header()
}

func (w *downloadTestResponseWriter) WriteHeader(status int) {
	w.recorder.WriteHeader(status)
}

func (w *downloadTestResponseWriter) Write(p []byte) (int, error) {
	if w.write != nil {
		return w.write(p)
	}
	return w.recorder.Write(p)
}

func (w *downloadTestResponseWriter) Flush() {
	_ = w.FlushError()
}

func (w *downloadTestResponseWriter) FlushError() error {
	if w.flush != nil {
		return w.flush()
	}
	w.recorder.Flush()
	return nil
}

func (w *downloadTestResponseWriter) SetWriteDeadline(deadline time.Time) error {
	if w.setDeadline != nil {
		return w.setDeadline(deadline)
	}
	return http.ErrNotSupported
}

func downloadTestExpectAbort(t *testing.T, fn func()) {
	t.Helper()
	defer func() {
		t.Helper()
		recovered := recover()
		if recovered == nil {
			t.Fatal("handler returned normally after a stream failure")
		}
		if recovered != http.ErrAbortHandler {
			t.Fatalf("panic = %v, want http.ErrAbortHandler", recovered)
		}
	}()
	fn()
}
