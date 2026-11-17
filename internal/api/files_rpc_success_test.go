package api_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
	"github.com/nathabonfim59/bamboo-raft/internal/auth"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// fileTestDecode decodes one JSON response body.
func fileTestDecode[T any](t *testing.T, recorder *httptest.ResponseRecorder) T {
	t.Helper()
	var value T
	if err := json.Unmarshal(recorder.Body.Bytes(), &value); err != nil {
		t.Fatalf("decode response: %v\nbody: %s", err, recorder.Body.Bytes())
	}
	return value
}

// fileTestCall runs one operation against env and returns its recorder.
func (e *fileTestEnv) call(t *testing.T, op, target, body string) *httptest.ResponseRecorder {
	t.Helper()
	request := fileTestRequest(fileTestMethodFor(op), target, body, fileTestIdentity(), fileTestSandboxID)
	return fileTestServe(e.handler(op), request)
}

// fileTestAssertSuccess asserts a 200 response and the pinned type.
func fileTestAssertSuccess(t *testing.T, recorder *httptest.ResponseRecorder, wantType string) {
	t.Helper()
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200\nbody: %s", recorder.Code, recorder.Body.Bytes())
	}
	var envelope struct {
		OK   bool   `json:"ok"`
		Type string `json:"type"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("decode success envelope: %v", err)
	}
	if !envelope.OK || envelope.Type != wantType {
		t.Fatalf("envelope = ok:%v type:%q, want true %q", envelope.OK, envelope.Type, wantType)
	}
}

func TestFilesReadWriteRoundTrip(t *testing.T) {
	env := newFileTestEnv(t)
	base := "/api/v1/sandboxes/" + fileTestSandboxID
	recorder := env.call(t, "writeFile", base+"/files", `{"path":"notes/input.txt","content":"hello\n"}`)
	fileTestAssertSuccess(t, recorder, api.TypeFileWritten)
	written := fileTestDecode[api.FileWriteResponse](t, recorder)
	if written.Path != "/home/user/notes/input.txt" || written.Encoding != api.FileEncodingUTF8 || written.Size != 6 {
		t.Errorf("write response = %+v", written)
	}
	if env.node.count(fileTestWrite) != 1 {
		t.Errorf("PutFile calls = %d, want 1", env.node.count(fileTestWrite))
	}
	env.node.mu.Lock()
	header := env.node.putHeader
	chunks := append([]int(nil), env.node.putChunks...)
	stored := append([]byte(nil), env.node.putData...)
	env.node.mu.Unlock()
	if header.GetPath() != "/home/user/notes/input.txt" || header.GetMode() != 0 {
		t.Errorf("PutFile header = %+v", header)
	}
	if len(chunks) != 1 || chunks[0] != 6 || string(stored) != "hello\n" {
		t.Errorf("PutFile chunks = %v data %q", chunks, stored)
	}
	if got := string(env.readDisk("/home/user/notes/input.txt")); got != "hello\n" {
		t.Errorf("disk = %q", got)
	}

	recorder = env.call(t, "readFile", base+"/files?path=notes/input.txt", "")
	fileTestAssertSuccess(t, recorder, api.TypeFileRead)
	read := fileTestDecode[api.FileReadResponse](t, recorder)
	if read.Path != "/home/user/notes/input.txt" || read.Encoding != api.FileEncodingUTF8 ||
		read.Size != 6 || read.Content != "hello\n" {
		t.Errorf("read response = %+v", read)
	}
	if env.node.count(fileTestRead) != 1 {
		t.Errorf("ReadFile calls = %d, want 1", env.node.count(fileTestRead))
	}

	// Dot-dot segments that clean inside the roots are accepted.
	recorder = env.call(t, "readFile", base+"/files?path=a/../notes/input.txt", "")
	fileTestAssertSuccess(t, recorder, api.TypeFileRead)
	if env.node.readReq.GetPath() != "/home/user/notes/input.txt" {
		t.Errorf("read path = %q, want the cleaned path", env.node.readReq.GetPath())
	}
}

func TestFilesBase64AndNonUTF8(t *testing.T) {
	env := newFileTestEnv(t)
	base := "/api/v1/sandboxes/" + fileTestSandboxID
	raw := []byte{0xff, 0xfe, 0x00, 'a', 0x80}
	body := fmt.Sprintf(`{"path":"raw.bin","content":%q,"encoding":"base64"}`,
		base64.StdEncoding.EncodeToString(raw))
	recorder := env.call(t, "writeFile", base+"/files", body)
	fileTestAssertSuccess(t, recorder, api.TypeFileWritten)
	written := fileTestDecode[api.FileWriteResponse](t, recorder)
	if written.Encoding != api.FileEncodingBase64 || written.Size != int64(len(raw)) {
		t.Errorf("write response = %+v", written)
	}
	if got := env.readDisk("/home/user/raw.bin"); !bytes.Equal(got, raw) {
		t.Errorf("disk = %x, want %x", got, raw)
	}

	recorder = env.call(t, "readFile", base+"/files?path=raw.bin&encoding=base64", "")
	fileTestAssertSuccess(t, recorder, api.TypeFileRead)
	read := fileTestDecode[api.FileReadResponse](t, recorder)
	decoded, err := base64.StdEncoding.DecodeString(read.Content)
	if err != nil {
		t.Fatalf("response base64: %v", err)
	}
	if !bytes.Equal(decoded, raw) || read.Encoding != api.FileEncodingBase64 {
		t.Errorf("read response = %+v", read)
	}

	recorder = env.call(t, "readFile", base+"/files?path=raw.bin", "")
	fileTestAssertError(t, recorder, http.StatusBadRequest, "invalid_query")
	if !strings.Contains(recorder.Body.String(), "use encoding=base64") {
		t.Errorf("non-UTF-8 answer does not name the base64 escape: %s", recorder.Body.String())
	}
}

func TestFilesReadSplitRuneChunks(t *testing.T) {
	env := newFileTestEnv(t)
	content := []byte("aé😀b")
	info := &nodev1.FileInfo{
		Path:             "/home/user/runes.txt",
		Name:             "runes.txt",
		Type:             nodev1.FileType_FILE_TYPE_REGULAR,
		Size:             int64(len(content)),
		Mode:             0o644,
		ModifiedAtUnixMs: -5000,
	}
	env.node.setReadHook(func(_ *nodev1.ReadFileRequest, stream grpc.ServerStreamingServer[nodev1.ReadFileResponse]) error {
		if err := stream.Send(&nodev1.ReadFileResponse{Part: &nodev1.ReadFileResponse_Info{Info: info}}); err != nil {
			return err
		}
		for offset, sizes := 0, []int{1, 1, 2, 1, 3}; offset < len(content); sizes = sizes[1:] {
			end := min(offset+sizes[0], len(content))
			if err := stream.Send(&nodev1.ReadFileResponse{Part: &nodev1.ReadFileResponse_Data{Data: content[offset:end]}}); err != nil {
				return err
			}
			offset = end
		}
		return nil
	})
	base := "/api/v1/sandboxes/" + fileTestSandboxID
	recorder := env.call(t, "readFile", base+"/files?path=runes.txt", "")
	fileTestAssertSuccess(t, recorder, api.TypeFileRead)
	read := fileTestDecode[api.FileReadResponse](t, recorder)
	if read.Content != string(content) || read.Size != int64(len(content)) {
		t.Errorf("split-rune read = %+v", read)
	}

	recorder = env.call(t, "readFile", base+"/files?path=runes.txt&encoding=base64", "")
	fileTestAssertSuccess(t, recorder, api.TypeFileRead)
	read = fileTestDecode[api.FileReadResponse](t, recorder)
	decoded, err := base64.StdEncoding.DecodeString(read.Content)
	if err != nil || !bytes.Equal(decoded, content) {
		t.Errorf("split-rune base64 = %+v err %v", read, err)
	}
}

func TestFilesInlineReadLimits(t *testing.T) {
	if testing.Short() {
		t.Skip("allocates 5 MiB fixtures")
	}
	env := newFileTestEnv(t)
	base := "/api/v1/sandboxes/" + fileTestSandboxID

	exact := bytes.Repeat([]byte{'a'}, int(api.FileInlineLimitBytes))
	env.writeDisk("/home/user/exact.bin", exact)
	recorder := env.call(t, "readFile", base+"/files?path=exact.bin", "")
	fileTestAssertSuccess(t, recorder, api.TypeFileRead)
	read := fileTestDecode[api.FileReadResponse](t, recorder)
	if read.Size != api.FileInlineLimitBytes || len(read.Content) != len(exact) {
		t.Errorf("exact read size = %d content %d", read.Size, len(read.Content))
	}

	over := bytes.Repeat([]byte{'b'}, int(api.FileInlineLimitBytes)+1)
	env.writeDisk("/home/user/over.bin", over)
	readsBefore, statsBefore := env.node.count(fileTestRead), env.node.count(fileTestStat)
	recorder = env.call(t, "readFile", base+"/files?path=over.bin", "")
	body := fileTestAssertError(t, recorder, http.StatusRequestEntityTooLarge, "file_too_large")
	if body.Error.Details["maxBytes"] != float64(api.FileInlineLimitBytes) {
		t.Errorf("maxBytes = %v", body.Error.Details["maxBytes"])
	}
	if body.Error.Details["sizeBytes"] != float64(api.FileInlineLimitBytes+1) {
		t.Errorf("sizeBytes = %v, want the statted file size", body.Error.Details["sizeBytes"])
	}
	if env.node.count(fileTestRead)-readsBefore != 1 || env.node.count(fileTestStat)-statsBefore != 1 {
		t.Errorf("calls = read:%d stat:%d, want one each",
			env.node.count(fileTestRead)-readsBefore, env.node.count(fileTestStat)-statsBefore)
	}
}

func TestFilesInlineWriteLimits(t *testing.T) {
	if testing.Short() {
		t.Skip("allocates 5 MiB fixtures")
	}
	env := newFileTestEnv(t)
	base := "/api/v1/sandboxes/" + fileTestSandboxID

	exactText := strings.Repeat("c", int(api.FileInlineLimitBytes))
	body := fmt.Sprintf(`{"path":"exact.txt","content":%q}`, exactText)
	recorder := env.call(t, "writeFile", base+"/files", body)
	fileTestAssertSuccess(t, recorder, api.TypeFileWritten)
	if got := env.readDisk("/home/user/exact.txt"); len(got) != len(exactText) {
		t.Errorf("exact write disk size = %d", len(got))
	}
	env.node.mu.Lock()
	chunks := append([]int(nil), env.node.putChunks...)
	env.node.mu.Unlock()
	total := 0
	for _, size := range chunks {
		if size > api.FileChunkBytes {
			t.Errorf("PutFile chunk %d exceeds %d", size, api.FileChunkBytes)
		}
		total += size
	}
	if total != len(exactText) {
		t.Errorf("PutFile bytes = %d, want %d", total, len(exactText))
	}

	before := env.factoryCalls.Load()
	overText := strings.Repeat("d", int(api.FileInlineLimitBytes)+1)
	body = fmt.Sprintf(`{"path":"over.txt","content":%q}`, overText)
	recorder = env.call(t, "writeFile", base+"/files", body)
	errorBody := fileTestAssertError(t, recorder, http.StatusRequestEntityTooLarge, "file_too_large")
	if errorBody.Error.Details["sizeBytes"] != float64(api.FileInlineLimitBytes+1) {
		t.Errorf("sizeBytes = %v", errorBody.Error.Details["sizeBytes"])
	}
	if env.factoryCalls.Load() != before {
		t.Error("oversized write reached the node")
	}
	if env.node.count(fileTestWrite) != 1 {
		t.Errorf("PutFile calls = %d, want only the exact write", env.node.count(fileTestWrite))
	}

	raw := bytes.Repeat([]byte{0x00}, int(api.FileInlineLimitBytes))
	body = fmt.Sprintf(`{"path":"exact.bin","content":%q,"encoding":"base64"}`,
		base64.StdEncoding.EncodeToString(raw))
	recorder = env.call(t, "writeFile", base+"/files", body)
	fileTestAssertSuccess(t, recorder, api.TypeFileWritten)
	if got := env.readDisk("/home/user/exact.bin"); !bytes.Equal(got, raw) {
		t.Errorf("base64 exact write size = %d", len(got))
	}
}

func TestFilesReadStreamContract(t *testing.T) {
	base := "/api/v1/sandboxes/" + fileTestSandboxID
	regular := func(path, name string) *nodev1.FileInfo {
		return &nodev1.FileInfo{Path: path, Name: name, Type: nodev1.FileType_FILE_TYPE_REGULAR, Size: 1, Mode: 0o644}
	}
	tests := []struct {
		name    string
		path    string
		hook    func(grpc.ServerStreamingServer[nodev1.ReadFileResponse]) error
		status  int
		apiCode string
	}{
		{
			name: "data before info",
			path: "a.txt",
			hook: func(stream grpc.ServerStreamingServer[nodev1.ReadFileResponse]) error {
				return fileTestSendReadData(stream, []byte("x"))
			},
			status: http.StatusBadGateway, apiCode: "sandbox_direct_failed",
		},
		{
			name:   "missing info",
			path:   "a.txt",
			hook:   func(grpc.ServerStreamingServer[nodev1.ReadFileResponse]) error { return nil },
			status: http.StatusBadGateway, apiCode: "sandbox_direct_failed",
		},
		{
			name: "info twice",
			path: "a.txt",
			hook: func(stream grpc.ServerStreamingServer[nodev1.ReadFileResponse]) error {
				if err := fileTestSendReadInfo(stream, regular("/home/user/a.txt", "a.txt")); err != nil {
					return err
				}
				return fileTestSendReadInfo(stream, regular("/home/user/a.txt", "a.txt"))
			},
			status: http.StatusBadGateway, apiCode: "sandbox_direct_failed",
		},
		{
			name: "chunk bounds",
			path: "a.txt",
			hook: func(stream grpc.ServerStreamingServer[nodev1.ReadFileResponse]) error {
				if err := fileTestSendReadInfo(stream, regular("/home/user/a.txt", "a.txt")); err != nil {
					return err
				}
				return fileTestSendReadData(stream, bytes.Repeat([]byte{'x'}, api.FileChunkBytes+1))
			},
			status: http.StatusBadGateway, apiCode: "sandbox_direct_failed",
		},
		{
			name: "unknown part",
			path: "a.txt",
			hook: func(stream grpc.ServerStreamingServer[nodev1.ReadFileResponse]) error {
				return fileTestSendReadMessage(stream, &nodev1.ReadFileResponse{})
			},
			status: http.StatusBadGateway, apiCode: "sandbox_direct_failed",
		},
		{
			name: "path mismatch",
			path: "a.txt",
			hook: func(stream grpc.ServerStreamingServer[nodev1.ReadFileResponse]) error {
				return fileTestSendReadInfo(stream, regular("/home/user/other.txt", "other.txt"))
			},
			status: http.StatusBadGateway, apiCode: "sandbox_direct_failed",
		},
		{
			name: "wrong type",
			path: "a.txt",
			hook: func(stream grpc.ServerStreamingServer[nodev1.ReadFileResponse]) error {
				return fileTestSendReadInfo(stream, &nodev1.FileInfo{
					Path: "/home/user/a.txt", Name: "a.txt",
					Type: nodev1.FileType_FILE_TYPE_DIRECTORY, Mode: 0o755,
				})
			},
			status: http.StatusBadRequest, apiCode: "invalid_path",
		},
		{
			name: "unknown file type",
			path: "a.txt",
			hook: func(stream grpc.ServerStreamingServer[nodev1.ReadFileResponse]) error {
				return fileTestSendReadInfo(stream, &nodev1.FileInfo{Path: "/home/user/a.txt", Name: "a.txt"})
			},
			status: http.StatusBadGateway, apiCode: "sandbox_direct_failed",
		},
		{
			name: "negative size",
			path: "a.txt",
			hook: func(stream grpc.ServerStreamingServer[nodev1.ReadFileResponse]) error {
				info := regular("/home/user/a.txt", "a.txt")
				info.Size = -1
				return fileTestSendReadInfo(stream, info)
			},
			status: http.StatusBadGateway, apiCode: "sandbox_direct_failed",
		},
		{
			name: "mode overflow",
			path: "a.txt",
			hook: func(stream grpc.ServerStreamingServer[nodev1.ReadFileResponse]) error {
				info := regular("/home/user/a.txt", "a.txt")
				info.Mode = 0o10000
				return fileTestSendReadInfo(stream, info)
			},
			status: http.StatusBadGateway, apiCode: "sandbox_direct_failed",
		},
		{
			name: "path outside roots",
			path: "a.txt",
			hook: func(stream grpc.ServerStreamingServer[nodev1.ReadFileResponse]) error {
				return fileTestSendReadInfo(stream, regular("/etc/passwd", "passwd"))
			},
			status: http.StatusBadGateway, apiCode: "sandbox_direct_failed",
		},
		{
			name: "name mismatch",
			path: "a.txt",
			hook: func(stream grpc.ServerStreamingServer[nodev1.ReadFileResponse]) error {
				return fileTestSendReadInfo(stream, regular("/home/user/a.txt", "b.txt"))
			},
			status: http.StatusBadGateway, apiCode: "sandbox_direct_failed",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			env := newFileTestEnv(t)
			hook := test.hook
			env.node.setReadHook(func(_ *nodev1.ReadFileRequest, stream grpc.ServerStreamingServer[nodev1.ReadFileResponse]) error {
				return hook(stream)
			})
			recorder := env.call(t, "readFile", base+"/files?path="+test.path, "")
			fileTestAssertError(t, recorder, test.status, test.apiCode)
		})
	}
}

func fileTestSendReadMessage(stream grpc.ServerStreamingServer[nodev1.ReadFileResponse], message *nodev1.ReadFileResponse) error {
	return stream.Send(message)
}

func fileTestSendReadInfo(stream grpc.ServerStreamingServer[nodev1.ReadFileResponse], info *nodev1.FileInfo) error {
	return fileTestSendReadMessage(stream, &nodev1.ReadFileResponse{Part: &nodev1.ReadFileResponse_Info{Info: info}})
}

func fileTestSendReadData(stream grpc.ServerStreamingServer[nodev1.ReadFileResponse], data []byte) error {
	return fileTestSendReadMessage(stream, &nodev1.ReadFileResponse{Part: &nodev1.ReadFileResponse_Data{Data: data}})
}

func TestFilesReadBufferOverflow(t *testing.T) {
	env := newFileTestEnv(t)
	base := "/api/v1/sandboxes/" + fileTestSandboxID
	env.node.setReadHook(func(_ *nodev1.ReadFileRequest, stream grpc.ServerStreamingServer[nodev1.ReadFileResponse]) error {
		info := &nodev1.FileInfo{
			Path: "/home/user/grow.bin", Name: "grow.bin",
			Type: nodev1.FileType_FILE_TYPE_REGULAR, Size: 1, Mode: 0o644,
		}
		if err := stream.Send(&nodev1.ReadFileResponse{Part: &nodev1.ReadFileResponse_Info{Info: info}}); err != nil {
			return err
		}
		chunk := bytes.Repeat([]byte{'g'}, api.FileChunkBytes)
		for range 6 {
			if err := stream.Send(&nodev1.ReadFileResponse{Part: &nodev1.ReadFileResponse_Data{Data: chunk}}); err != nil {
				return nil // the handler canceled at the cap
			}
		}
		return nil
	})
	recorder := env.call(t, "readFile", base+"/files?path=grow.bin", "")
	body := fileTestAssertError(t, recorder, http.StatusRequestEntityTooLarge, "file_too_large")
	if _, ok := body.Error.Details["sizeBytes"]; ok {
		t.Errorf("unknown actual size must be omitted, got %v", body.Error.Details["sizeBytes"])
	}
	if body.Error.Details["maxBytes"] != float64(api.FileInlineLimitBytes) {
		t.Errorf("maxBytes = %v", body.Error.Details["maxBytes"])
	}
}

func TestFilesReadOutOfRange(t *testing.T) {
	base := "/api/v1/sandboxes/" + fileTestSandboxID
	t.Run("known size", func(t *testing.T) {
		env := newFileTestEnv(t)
		env.writeDisk("/home/user/big.bin", bytes.Repeat([]byte{'z'}, int(api.FileInlineLimitBytes)+1))
		env.node.setReadHook(func(_ *nodev1.ReadFileRequest, _ grpc.ServerStreamingServer[nodev1.ReadFileResponse]) error {
			return status.Error(codes.OutOfRange, "native secret text")
		})
		recorder := env.call(t, "readFile", base+"/files?path=big.bin", "")
		body := fileTestAssertError(t, recorder, http.StatusRequestEntityTooLarge, "file_too_large")
		if body.Error.Details["sizeBytes"] != float64(api.FileInlineLimitBytes+1) {
			t.Errorf("sizeBytes = %v", body.Error.Details["sizeBytes"])
		}
		if strings.Contains(recorder.Body.String(), "native secret text") {
			t.Error("native node text reached the wire")
		}
	})
	t.Run("unknown size", func(t *testing.T) {
		env := newFileTestEnv(t)
		env.node.setReadHook(func(_ *nodev1.ReadFileRequest, _ grpc.ServerStreamingServer[nodev1.ReadFileResponse]) error {
			return status.Error(codes.OutOfRange, "native secret text")
		})
		recorder := env.call(t, "readFile", base+"/files?path=missing.bin", "")
		body := fileTestAssertError(t, recorder, http.StatusRequestEntityTooLarge, "file_too_large")
		if _, ok := body.Error.Details["sizeBytes"]; ok {
			t.Errorf("size must be omitted when unknown, got %v", body.Error.Details["sizeBytes"])
		}
	})
}

func TestFilesReadCancellation(t *testing.T) {
	env := newFileTestEnv(t)
	base := "/api/v1/sandboxes/" + fileTestSandboxID
	started := make(chan struct{})
	canceled := make(chan struct{})
	env.node.setReadHook(func(_ *nodev1.ReadFileRequest, stream grpc.ServerStreamingServer[nodev1.ReadFileResponse]) error {
		close(started)
		<-stream.Context().Done()
		close(canceled)
		return status.FromContextError(stream.Context().Err()).Err()
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	request := fileTestRequest(http.MethodGet, base+"/files?path=a.txt", "", fileTestIdentity(), fileTestSandboxID).
		WithContext(auth.WithIdentity(ctx, auth.Identity{Owner: fileTestOwner}))
	writer := newFileTestResponseWriter()
	done := make(chan struct{})
	go func() {
		defer close(done)
		env.handlers.ReadFile(writer, request)
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		cancel()
		fileTestJoin(t, done)
		t.Fatal("read RPC never started")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("read handler did not return after cancellation")
	}
	select {
	case <-canceled:
	case <-time.After(5 * time.Second):
		t.Fatal("node stream was not canceled")
	}
	if writer.wrote {
		t.Errorf("canceled read wrote status %d body %q", writer.status, writer.body.String())
	}
}

func TestFilesWriteContract(t *testing.T) {
	base := "/api/v1/sandboxes/" + fileTestSandboxID
	tests := []struct {
		name string
		hook func(grpc.ClientStreamingServer[nodev1.PutFileRequest, nodev1.PutFileResponse]) error
	}{
		{
			name: "size mismatch",
			hook: func(stream grpc.ClientStreamingServer[nodev1.PutFileRequest, nodev1.PutFileResponse]) error {
				header, data, err := fileTestDrainPut(stream)
				if err != nil {
					return err
				}
				return stream.SendAndClose(&nodev1.PutFileResponse{Path: header.GetPath(), Size: int64(len(data)) + 1})
			},
		},
		{
			name: "path mismatch",
			hook: func(stream grpc.ClientStreamingServer[nodev1.PutFileRequest, nodev1.PutFileResponse]) error {
				_, data, err := fileTestDrainPut(stream)
				if err != nil {
					return err
				}
				return stream.SendAndClose(&nodev1.PutFileResponse{Path: "/home/user/elsewhere", Size: int64(len(data))})
			},
		},
		{
			name: "node failure",
			hook: func(grpc.ClientStreamingServer[nodev1.PutFileRequest, nodev1.PutFileResponse]) error {
				return status.Error(codes.Internal, "native secret text")
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			env := newFileTestEnv(t)
			hook := test.hook
			env.node.setPutHook(hook)
			recorder := env.call(t, "writeFile", base+"/files", `{"path":"a.txt","content":"hello"}`)
			body := fileTestAssertError(t, recorder, http.StatusBadGateway, "sandbox_direct_failed")
			if strings.Contains(recorder.Body.String(), "native secret text") {
				t.Error("native node text reached the wire")
			}
			if retryable, ok := body.Error.Details["retryable"].(bool); !ok || !retryable {
				t.Errorf("ambiguous write retryable = %v, want true", body.Error.Details["retryable"])
			}
		})
	}
}

// fileTestDrainPut reads one PutFile stream into its header and bytes.
func fileTestDrainPut(stream grpc.ClientStreamingServer[nodev1.PutFileRequest, nodev1.PutFileResponse]) (*nodev1.PutFileRequest_Header, []byte, error) {
	first, err := stream.Recv()
	if err != nil {
		return nil, nil, err
	}
	header := first.GetHeader()
	if header == nil {
		return nil, nil, status.Error(codes.InvalidArgument, "header missing")
	}
	var data []byte
	for {
		message, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return header, data, nil
		}
		if err != nil {
			return header, data, err
		}
		data = append(data, message.GetData()...)
	}
}

func TestFilesWriteEmptyAndChunks(t *testing.T) {
	env := newFileTestEnv(t)
	base := "/api/v1/sandboxes/" + fileTestSandboxID
	recorder := env.call(t, "writeFile", base+"/files", `{"path":"empty.txt","content":""}`)
	fileTestAssertSuccess(t, recorder, api.TypeFileWritten)
	written := fileTestDecode[api.FileWriteResponse](t, recorder)
	if written.Size != 0 {
		t.Errorf("empty write size = %d", written.Size)
	}
	if got, err := os.ReadFile(env.disk("/home/user/empty.txt")); err != nil || len(got) != 0 {
		t.Errorf("empty disk file = %q err %v", got, err)
	}

	content := strings.Repeat("x", (5<<19)+1)
	recorder = env.call(t, "writeFile", base+"/files",
		fmt.Sprintf(`{"path":"chunks.txt","content":%q}`, content))
	fileTestAssertSuccess(t, recorder, api.TypeFileWritten)
	env.node.mu.Lock()
	chunks := append([]int(nil), env.node.putChunks...)
	env.node.mu.Unlock()
	want := []int{1 << 20, 1 << 20, (5 << 19) + 1 - (2 << 20)}
	if len(chunks) != len(want) {
		t.Fatalf("chunks = %v, want %v", chunks, want)
	}
	for i := range want {
		if chunks[i] != want[i] {
			t.Errorf("chunk %d = %d, want %d", i, chunks[i], want[i])
		}
	}
}

func TestFilesStartingSandboxThenReady(t *testing.T) {
	env := newFileTestEnv(t)
	env.writeDisk("/home/user/ready.txt", []byte("ok"))
	env.lookup.mu.Lock()
	env.lookup.states = []api.SandboxState{api.StateProvisioning, api.StateProvisioning, api.StateReady}
	env.lookup.mu.Unlock()
	base := "/api/v1/sandboxes/" + fileTestSandboxID
	recorder := env.call(t, "readFile", base+"/files?path=ready.txt", "")
	fileTestAssertSuccess(t, recorder, api.TypeFileRead)
	env.lookup.mu.Lock()
	calls := env.lookup.calls
	env.lookup.mu.Unlock()
	if calls < 3 {
		t.Errorf("lookup calls = %d, want polling until ready", calls)
	}
}

func TestFilesOwnershipBeforeRPC(t *testing.T) {
	env := newFileTestEnv(t)
	env.lookup.mu.Lock()
	env.lookup.notFound = true
	env.lookup.mu.Unlock()
	base := "/api/v1/sandboxes/" + fileTestSandboxID
	recorder := env.call(t, "readFile", base+"/files?path=a.txt", "")
	fileTestAssertError(t, recorder, http.StatusNotFound, "not_found")
	if env.factoryCalls.Load() != 0 {
		t.Errorf("node factory reached %d times", env.factoryCalls.Load())
	}
	for call, count := range env.node.counts() {
		if count != 0 {
			t.Errorf("RPC %s reached %d times for a foreign sandbox", call, count)
		}
	}
}

func TestFilesDelete(t *testing.T) {
	env := newFileTestEnv(t)
	base := "/api/v1/sandboxes/" + fileTestSandboxID
	env.writeDisk("/home/user/a.txt", []byte("a"))
	recorder := env.call(t, "deleteFile", base+"/files?path=a.txt", "")
	fileTestAssertSuccess(t, recorder, api.TypeFileDeleted)
	deleted := fileTestDecode[api.FileDeleteResponse](t, recorder)
	if deleted.Path != "/home/user/a.txt" {
		t.Errorf("delete path = %q", deleted.Path)
	}
	if _, err := os.Stat(env.disk("/home/user/a.txt")); !os.IsNotExist(err) {
		t.Errorf("file still exists: %v", err)
	}

	recorder = env.call(t, "deleteFile", base+"/files?path=missing.txt", "")
	fileTestAssertError(t, recorder, http.StatusNotFound, "not_found")

	env.writeDisk("/home/user/dir/keep.txt", []byte("k"))
	recorder = env.call(t, "deleteFile", base+"/files?path=dir", "")
	fileTestAssertError(t, recorder, http.StatusBadRequest, "invalid_path")
	if _, err := os.Stat(env.disk("/home/user/dir/keep.txt")); err != nil {
		t.Errorf("non-recursive delete removed the directory: %v", err)
	}
	recorder = env.call(t, "deleteFile", base+"/files?path=dir&recursive=true", "")
	fileTestAssertSuccess(t, recorder, api.TypeFileDeleted)
	if _, err := os.Stat(env.disk("/home/user/dir")); !os.IsNotExist(err) {
		t.Errorf("recursive delete left the directory: %v", err)
	}

	env.writeDisk("/home/user/target.txt", []byte("t"))
	link := env.disk("/home/user/link.txt")
	if err := os.Symlink("target.txt", link); err != nil {
		t.Fatal(err)
	}
	recorder = env.call(t, "deleteFile", base+"/files?path=link.txt", "")
	fileTestAssertSuccess(t, recorder, api.TypeFileDeleted)
	if _, err := os.Lstat(link); !os.IsNotExist(err) {
		t.Errorf("symlink still exists: %v", err)
	}
	if _, err := os.Stat(env.disk("/home/user/target.txt")); err != nil {
		t.Errorf("symlink delete removed the target: %v", err)
	}

	before := env.node.count(fileTestDelete)
	recorder = env.call(t, "deleteFile", base+"/files?path=%2Ftmp", "")
	fileTestAssertError(t, recorder, http.StatusBadRequest, "invalid_path")
	if env.node.count(fileTestDelete) != before {
		t.Error("deleting a root reached the node")
	}
}

func TestFilesList(t *testing.T) {
	env := newFileTestEnv(t)
	base := "/api/v1/sandboxes/" + fileTestSandboxID
	env.writeDisk("/home/user/b.txt", []byte("bb"))
	env.writeDisk("/home/user/a.txt", []byte("a"))
	if err := os.MkdirAll(env.disk("/home/user/dir"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("a.txt", env.disk("/home/user/link.txt")); err != nil {
		t.Fatal(err)
	}
	recorder := env.call(t, "listFiles", base+"/files/list", "")
	fileTestAssertSuccess(t, recorder, api.TypeFileList)
	list := fileTestDecode[api.FileListResponse](t, recorder)
	if list.Path != "/home/user" || list.Truncated {
		t.Errorf("list = %+v", list)
	}
	var names []string
	for _, entry := range list.Files {
		names = append(names, entry.Name)
	}
	if strings.Join(names, ",") != "a.txt,b.txt,dir,link.txt" {
		t.Errorf("names = %v", names)
	}
	for _, entry := range list.Files {
		switch entry.Name {
		case "a.txt":
			if entry.IsDir || entry.IsSymlink || entry.Size != 1 || entry.Mode != "0644" || entry.ModifiedAt.IsZero() {
				t.Errorf("a.txt entry = %+v", entry)
			}
		case "dir":
			if !entry.IsDir || entry.IsSymlink {
				t.Errorf("dir entry = %+v", entry)
			}
		case "link.txt":
			if !entry.IsSymlink || entry.Path != "/home/user/link.txt" {
				t.Errorf("link entry = %+v", entry)
			}
		}
	}
	env.node.mu.Lock()
	limit := env.node.listReq.Limit
	env.node.mu.Unlock()
	if limit != api.FileListEntries {
		t.Errorf("ListFiles limit = %d, want %d", limit, api.FileListEntries)
	}

	recorder = env.call(t, "listFiles", base+"/files/list?path=dir", "")
	fileTestAssertSuccess(t, recorder, api.TypeFileList)
	list = fileTestDecode[api.FileListResponse](t, recorder)
	if list.Path != "/home/user/dir" || len(list.Files) != 0 {
		t.Errorf("empty dir list = %+v", list)
	}
	if !strings.Contains(recorder.Body.String(), `"files":[]`) {
		t.Errorf("empty list is not an array: %s", recorder.Body.String())
	}

	env.writeDisk("/home/user/file.txt", []byte("f"))
	recorder = env.call(t, "listFiles", base+"/files/list?path=file.txt", "")
	fileTestAssertError(t, recorder, http.StatusBadRequest, "invalid_path")
	recorder = env.call(t, "listFiles", base+"/files/list?path=missing", "")
	fileTestAssertError(t, recorder, http.StatusNotFound, "not_found")
}

func TestFilesListTruncation(t *testing.T) {
	env := newFileTestEnv(t)
	base := "/api/v1/sandboxes/" + fileTestSandboxID
	dir := env.disk("/home/user/big")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	for i := range 10001 {
		name := filepath.Join(dir, fmt.Sprintf("f%05d", i))
		if err := os.WriteFile(name, nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	recorder := env.call(t, "listFiles", base+"/files/list?path=big", "")
	fileTestAssertSuccess(t, recorder, api.TypeFileList)
	list := fileTestDecode[api.FileListResponse](t, recorder)
	if len(list.Files) != int(api.FileListEntries) || !list.Truncated {
		t.Errorf("entries = %d truncated = %v, want %d true", len(list.Files), list.Truncated, api.FileListEntries)
	}
}

func TestFilesGetFileInfo(t *testing.T) {
	env := newFileTestEnv(t)
	base := "/api/v1/sandboxes/" + fileTestSandboxID
	env.writeDisk("/home/user/a.txt", []byte("abc"))
	if err := os.MkdirAll(env.disk("/home/user/dir"), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("a.txt", env.disk("/home/user/link.txt")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("nowhere", env.disk("/home/user/dangling")); err != nil {
		t.Fatal(err)
	}

	recorder := env.call(t, "getFileInfo", base+"/files/info?path=a.txt", "")
	fileTestAssertSuccess(t, recorder, api.TypeFileInfo)
	fileInfo := fileTestDecode[api.FileInfoResponse](t, recorder)
	if fileInfo.Name != "a.txt" || fileInfo.Path != "/home/user/a.txt" || fileInfo.IsDir ||
		fileInfo.IsSymlink || fileInfo.Size != 3 || fileInfo.Mode != "0644" {
		t.Errorf("file info = %+v", fileInfo)
	}

	recorder = env.call(t, "getFileInfo", base+"/files/info?path=dir", "")
	fileTestAssertSuccess(t, recorder, api.TypeFileInfo)
	dirInfo := fileTestDecode[api.FileInfoResponse](t, recorder)
	if !dirInfo.IsDir || dirInfo.Mode != "0750" {
		t.Errorf("dir info = %+v", dirInfo)
	}

	recorder = env.call(t, "getFileInfo", base+"/files/info?path=link.txt", "")
	fileTestAssertSuccess(t, recorder, api.TypeFileInfo)
	linkInfo := fileTestDecode[api.FileInfoResponse](t, recorder)
	if !linkInfo.IsSymlink || linkInfo.IsDir || linkInfo.Size != 3 {
		t.Errorf("link info = %+v", linkInfo)
	}

	recorder = env.call(t, "getFileInfo", base+"/files/info?path=dangling", "")
	fileTestAssertSuccess(t, recorder, api.TypeFileInfo)
	dangling := fileTestDecode[api.FileInfoResponse](t, recorder)
	if !dangling.IsSymlink || dangling.IsDir {
		t.Errorf("dangling info = %+v", dangling)
	}

	recorder = env.call(t, "getFileInfo", base+"/files/info?path=missing", "")
	fileTestAssertError(t, recorder, http.StatusNotFound, "not_found")
}

func TestFilesCreateFolder(t *testing.T) {
	env := newFileTestEnv(t)
	base := "/api/v1/sandboxes/" + fileTestSandboxID
	recorder := env.call(t, "createFolder", base+"/files/folder", `{"path":"a/b/c"}`)
	fileTestAssertSuccess(t, recorder, api.TypeFolderCreated)
	created := fileTestDecode[api.FileInfoResponse](t, recorder)
	if !created.IsDir || created.Path != "/home/user/a/b/c" || created.Mode != "0755" {
		t.Errorf("folder = %+v", created)
	}
	if info, err := os.Stat(env.disk("/home/user/a/b/c")); err != nil || !info.IsDir() {
		t.Errorf("folder on disk: %v", err)
	}

	recorder = env.call(t, "createFolder", base+"/files/folder", `{"path":"private","mode":"700"}`)
	fileTestAssertSuccess(t, recorder, api.TypeFolderCreated)
	private := fileTestDecode[api.FileInfoResponse](t, recorder)
	if private.Mode != "0700" {
		t.Errorf("private mode = %q", private.Mode)
	}
	info, err := os.Stat(env.disk("/home/user/private"))
	if err != nil {
		t.Fatalf("stat private: %v", err)
	}
	if info.Mode().Perm() != 0o700 {
		t.Errorf("private disk mode = %v", info.Mode())
	}

	recorder = env.call(t, "createFolder", base+"/files/folder", `{"path":"private"}`)
	fileTestAssertSuccess(t, recorder, api.TypeFolderCreated)

	env.writeDisk("/home/user/file.txt", []byte("f"))
	recorder = env.call(t, "createFolder", base+"/files/folder", `{"path":"file.txt"}`)
	body := fileTestAssertError(t, recorder, http.StatusBadRequest, "invalid_path")
	if body.Message != "path exists and is not a directory" {
		t.Errorf("message = %q", body.Message)
	}
}

func TestFilesMove(t *testing.T) {
	env := newFileTestEnv(t)
	base := "/api/v1/sandboxes/" + fileTestSandboxID
	env.writeDisk("/home/user/a.txt", []byte("a"))
	recorder := env.call(t, "moveFile", base+"/files/move", `{"source":"a.txt","destination":"dir/b.txt"}`)
	fileTestAssertSuccess(t, recorder, api.TypeFileMoved)
	moved := fileTestDecode[api.FileMoveResponse](t, recorder)
	if moved.Path != "/home/user/a.txt" || moved.Destination != "/home/user/dir/b.txt" {
		t.Errorf("move = %+v", moved)
	}
	if got := string(env.readDisk("/home/user/dir/b.txt")); got != "a" {
		t.Errorf("moved content = %q", got)
	}

	env.writeDisk("/home/user/home.txt", []byte("h"))
	recorder = env.call(t, "moveFile", base+"/files/move", `{"source":"home.txt","destination":"/tmp/moved.txt"}`)
	fileTestAssertSuccess(t, recorder, api.TypeFileMoved)
	if got := string(env.readDisk("/tmp/moved.txt")); got != "h" {
		t.Errorf("cross-root content = %q", got)
	}

	env.writeDisk("/home/user/source.txt", []byte("new"))
	env.writeDisk("/home/user/exists.txt", []byte("old"))
	recorder = env.call(t, "moveFile", base+"/files/move", `{"source":"source.txt","destination":"exists.txt"}`)
	body := fileTestAssertError(t, recorder, http.StatusBadRequest, "invalid_path")
	if body.Message != "destination exists; set overwrite" {
		t.Errorf("message = %q", body.Message)
	}
	recorder = env.call(t, "moveFile", base+"/files/move", `{"source":"source.txt","destination":"exists.txt","overwrite":true}`)
	fileTestAssertSuccess(t, recorder, api.TypeFileMoved)
	if got := string(env.readDisk("/home/user/exists.txt")); got != "new" {
		t.Errorf("overwrite content = %q", got)
	}

	env.writeDisk("/home/user/full/keep.txt", []byte("k"))
	recorder = env.call(t, "moveFile", base+"/files/move", `{"source":"exists.txt","destination":"full","overwrite":true}`)
	fileTestAssertError(t, recorder, http.StatusBadRequest, "invalid_path")

	recorder = env.call(t, "moveFile", base+"/files/move", `{"source":"missing.txt","destination":"b.txt"}`)
	fileTestAssertError(t, recorder, http.StatusNotFound, "not_found")

	env.writeDisk("/home/user/target.txt", []byte("t"))
	if err := os.Symlink("target.txt", env.disk("/home/user/link")); err != nil {
		t.Fatal(err)
	}
	recorder = env.call(t, "moveFile", base+"/files/move", `{"source":"link","destination":"link2"}`)
	fileTestAssertSuccess(t, recorder, api.TypeFileMoved)
	info, err := os.Lstat(env.disk("/home/user/link2"))
	if err != nil || info.Mode()&os.ModeSymlink == 0 {
		t.Errorf("moved link = %v err %v", info, err)
	}
	if got := string(env.readDisk("/home/user/target.txt")); got != "t" {
		t.Errorf("target after link move = %q", got)
	}
}

func TestFilesSetFilePermissions(t *testing.T) {
	env := newFileTestEnv(t)
	base := "/api/v1/sandboxes/" + fileTestSandboxID
	env.writeDisk("/home/user/script.sh", []byte("#!/bin/sh\n"))
	recorder := env.call(t, "setFilePermissions", base+"/files/permissions", `{"path":"script.sh","mode":"600"}`)
	fileTestAssertSuccess(t, recorder, api.TypeFilePermissionsSet)
	entry := fileTestDecode[api.FileInfoResponse](t, recorder)
	if entry.Mode != "0600" {
		t.Errorf("response mode = %q", entry.Mode)
	}
	info, err := os.Stat(env.disk("/home/user/script.sh"))
	if err != nil {
		t.Fatalf("stat script: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Errorf("disk mode = %v", info.Mode())
	}

	recorder = env.call(t, "setFilePermissions", base+"/files/permissions", `{"path":"script.sh","mode":"4755"}`)
	fileTestAssertSuccess(t, recorder, api.TypeFilePermissionsSet)
	info, err = os.Stat(env.disk("/home/user/script.sh"))
	if err != nil {
		t.Fatalf("stat script: %v", err)
	}
	if info.Mode().Perm() != 0o755 || info.Mode()&os.ModeSetuid == 0 {
		t.Errorf("setuid mode = %v", info.Mode())
	}

	recorder = env.call(t, "setFilePermissions", base+"/files/permissions", `{"path":"missing","mode":"644"}`)
	fileTestAssertError(t, recorder, http.StatusNotFound, "not_found")
}

func TestFilesWrongTypeAndEscapingSymlink(t *testing.T) {
	env := newFileTestEnv(t)
	base := "/api/v1/sandboxes/" + fileTestSandboxID
	if err := os.MkdirAll(env.disk("/home/user/dir"), 0o755); err != nil {
		t.Fatal(err)
	}

	recorder := env.call(t, "readFile", base+"/files?path=dir", "")
	fileTestAssertError(t, recorder, http.StatusBadRequest, "invalid_path")

	recorder = env.call(t, "writeFile", base+"/files", `{"path":"/home/user","content":"x"}`)
	fileTestAssertError(t, recorder, http.StatusBadRequest, "invalid_path")

	escape := env.disk("/home/user/escape")
	if err := os.Symlink("/etc/passwd", escape); err != nil {
		t.Fatal(err)
	}
	recorder = env.call(t, "readFile", base+"/files?path=escape", "")
	fileTestAssertError(t, recorder, http.StatusBadRequest, "invalid_path")

	recorder = env.call(t, "getFileInfo", base+"/files/info?path=escape", "")
	fileTestAssertSuccess(t, recorder, api.TypeFileInfo)
	info := fileTestDecode[api.FileInfoResponse](t, recorder)
	if !info.IsSymlink || info.IsDir {
		t.Errorf("escaping symlink info = %+v, want a symlink", info)
	}

	recorder = env.call(t, "deleteFile", base+"/files?path=escape", "")
	fileTestAssertSuccess(t, recorder, api.TypeFileDeleted)
	if _, err := os.Lstat(escape); !os.IsNotExist(err) {
		t.Errorf("escaping symlink still exists: %v", err)
	}
}

func TestFilesMetadataPrecision(t *testing.T) {
	env := newFileTestEnv(t)
	base := "/api/v1/sandboxes/" + fileTestSandboxID
	env.node.mu.Lock()
	env.node.statHook = func(*nodev1.StatFileRequest) (*nodev1.FileInfo, error) {
		return &nodev1.FileInfo{
			Path:             "/home/user/old.txt",
			Name:             "old.txt",
			Type:             nodev1.FileType_FILE_TYPE_REGULAR,
			Size:             0,
			Mode:             0o644,
			ModifiedAtUnixMs: -60000,
		}, nil
	}
	env.node.mu.Unlock()
	recorder := env.call(t, "getFileInfo", base+"/files/info?path=old.txt", "")
	fileTestAssertSuccess(t, recorder, api.TypeFileInfo)
	entry := fileTestDecode[api.FileInfoResponse](t, recorder)
	want := time.UnixMilli(-60000).UTC()
	if !entry.ModifiedAt.Equal(want) || entry.ModifiedAt.Year() != 1969 {
		t.Errorf("negative modifiedAt = %v, want %v", entry.ModifiedAt, want)
	}

	env.node.mu.Lock()
	env.node.statHook = func(*nodev1.StatFileRequest) (*nodev1.FileInfo, error) {
		return &nodev1.FileInfo{
			Path: "/home/user/epoch.txt", Name: "epoch.txt",
			Type: nodev1.FileType_FILE_TYPE_REGULAR, Mode: 0o644,
		}, nil
	}
	env.node.mu.Unlock()
	recorder = env.call(t, "getFileInfo", base+"/files/info?path=epoch.txt", "")
	fileTestAssertSuccess(t, recorder, api.TypeFileInfo)
	entry = fileTestDecode[api.FileInfoResponse](t, recorder)
	if !entry.ModifiedAt.Equal(time.UnixMilli(0).UTC()) {
		t.Errorf("zero modifiedAt = %v, want the epoch", entry.ModifiedAt)
	}

	bad := []*nodev1.FileInfo{
		{Path: "/home/user/bad", Name: "bad", Size: -1, Mode: 0o644, Type: nodev1.FileType_FILE_TYPE_REGULAR},
		{Path: "/home/user/bad", Name: "bad", Mode: 0o10000, Type: nodev1.FileType_FILE_TYPE_REGULAR},
		{Path: "/home/user/bad", Name: "bad", Mode: 0o644},
		{Path: "/etc/passwd", Name: "passwd", Mode: 0o644, Type: nodev1.FileType_FILE_TYPE_REGULAR},
		{Path: "/home/user/bad", Name: "other", Mode: 0o644, Type: nodev1.FileType_FILE_TYPE_REGULAR},
	}
	for i, info := range bad {
		env.node.mu.Lock()
		env.node.statHook = func(*nodev1.StatFileRequest) (*nodev1.FileInfo, error) { return info, nil }
		env.node.mu.Unlock()
		recorder = env.call(t, "getFileInfo", base+"/files/info?path=bad", "")
		fileTestAssertError(t, recorder, http.StatusBadGateway, "sandbox_direct_failed")
		if i == 0 && strings.Contains(recorder.Body.String(), "-1") {
			t.Error("native metadata reached the wire")
		}
	}
}

// fileTestRunLight runs one operation against a stub client.
func fileTestRunLight(t *testing.T, client nodev1.NodeClient, op string) *httptest.ResponseRecorder {
	t.Helper()
	handlers := newFileTestLight(t, fileTestReadyLookup(nil), fileTestStubFactory(client))
	operation := fileTestOperationFor(op)
	request := fileTestRequest(operation.method, operation.target, operation.body, fileTestIdentity(), fileTestSandboxID)
	return fileTestServe(fileTestHandler(t, handlers, op), request)
}

// TestFilesUnaryResponseValidation requires every unary handler to accept only
// the exact response it requested. Nil or wrong-path replies are 502, never an
// invented success.
func TestFilesUnaryResponseValidation(t *testing.T) {
	entries := func(paths ...string) []*nodev1.FileInfo {
		out := make([]*nodev1.FileInfo, 0, len(paths))
		for _, p := range paths {
			out = append(out, fileTestInfo(p, nodev1.FileType_FILE_TYPE_REGULAR))
		}
		return out
	}
	many := make([]*nodev1.FileInfo, 0, 10001)
	for i := range 10001 {
		many = append(many, fileTestInfo(fmt.Sprintf("/home/user/f%05d", i), nodev1.FileType_FILE_TYPE_REGULAR))
	}
	tests := []struct {
		name   string
		op     string
		client *fileTestStubClient
		status int
	}{
		{"delete nil", "deleteFile", &fileTestStubClient{deleteFn: func(context.Context, *nodev1.DeleteFileRequest) (*nodev1.DeleteFileResponse, error) {
			return nil, nil
		}}, http.StatusBadGateway},
		{"delete wrong path", "deleteFile", &fileTestStubClient{deleteFn: func(context.Context, *nodev1.DeleteFileRequest) (*nodev1.DeleteFileResponse, error) {
			return &nodev1.DeleteFileResponse{Path: "/home/user/other"}, nil
		}}, http.StatusBadGateway},
		{"delete ok", "deleteFile", &fileTestStubClient{deleteFn: func(context.Context, *nodev1.DeleteFileRequest) (*nodev1.DeleteFileResponse, error) {
			return &nodev1.DeleteFileResponse{Path: "/home/user/note.txt"}, nil
		}}, http.StatusOK},

		{"move nil", "moveFile", &fileTestStubClient{moveFn: func(context.Context, *nodev1.MoveFileRequest) (*nodev1.MoveFileResponse, error) {
			return nil, nil
		}}, http.StatusBadGateway},
		{"move wrong source", "moveFile", &fileTestStubClient{moveFn: func(context.Context, *nodev1.MoveFileRequest) (*nodev1.MoveFileResponse, error) {
			return &nodev1.MoveFileResponse{Source: "/home/user/other", Destination: "/home/user/b.txt"}, nil
		}}, http.StatusBadGateway},
		{"move wrong destination", "moveFile", &fileTestStubClient{moveFn: func(context.Context, *nodev1.MoveFileRequest) (*nodev1.MoveFileResponse, error) {
			return &nodev1.MoveFileResponse{Source: "/home/user/a.txt", Destination: "/home/user/other"}, nil
		}}, http.StatusBadGateway},
		{"move ok", "moveFile", &fileTestStubClient{moveFn: func(context.Context, *nodev1.MoveFileRequest) (*nodev1.MoveFileResponse, error) {
			return &nodev1.MoveFileResponse{Source: "/home/user/a.txt", Destination: "/home/user/b.txt"}, nil
		}}, http.StatusOK},

		{"list nil", "listFiles", &fileTestStubClient{listFn: func(context.Context, *nodev1.ListFilesRequest) (*nodev1.ListFilesResponse, error) {
			return nil, nil
		}}, http.StatusBadGateway},
		{"list wrong path", "listFiles", &fileTestStubClient{listFn: func(context.Context, *nodev1.ListFilesRequest) (*nodev1.ListFilesResponse, error) {
			return &nodev1.ListFilesResponse{Path: "/tmp"}, nil
		}}, http.StatusBadGateway},
		{"list too many", "listFiles", &fileTestStubClient{listFn: func(context.Context, *nodev1.ListFilesRequest) (*nodev1.ListFilesResponse, error) {
			return &nodev1.ListFilesResponse{Path: "/home/user", Entries: many}, nil
		}}, http.StatusBadGateway},
		{"list nested", "listFiles", &fileTestStubClient{listFn: func(context.Context, *nodev1.ListFilesRequest) (*nodev1.ListFilesResponse, error) {
			return &nodev1.ListFilesResponse{Path: "/home/user", Entries: entries("/home/user/dir/a")}, nil
		}}, http.StatusBadGateway},
		{"list duplicate", "listFiles", &fileTestStubClient{listFn: func(context.Context, *nodev1.ListFilesRequest) (*nodev1.ListFilesResponse, error) {
			return &nodev1.ListFilesResponse{Path: "/home/user", Entries: entries("/home/user/a", "/home/user/a")}, nil
		}}, http.StatusBadGateway},
		{"list descending", "listFiles", &fileTestStubClient{listFn: func(context.Context, *nodev1.ListFilesRequest) (*nodev1.ListFilesResponse, error) {
			return &nodev1.ListFilesResponse{Path: "/home/user", Entries: entries("/home/user/b", "/home/user/a")}, nil
		}}, http.StatusBadGateway},
		{"list invalid metadata", "listFiles", &fileTestStubClient{listFn: func(context.Context, *nodev1.ListFilesRequest) (*nodev1.ListFilesResponse, error) {
			return &nodev1.ListFilesResponse{Path: "/home/user", Entries: []*nodev1.FileInfo{{Path: "/home/user/a", Name: "a"}}}, nil
		}}, http.StatusBadGateway},
		{"list ok", "listFiles", &fileTestStubClient{listFn: func(context.Context, *nodev1.ListFilesRequest) (*nodev1.ListFilesResponse, error) {
			return &nodev1.ListFilesResponse{Path: "/home/user", Entries: entries("/home/user/a", "/home/user/b")}, nil
		}}, http.StatusOK},

		{"info nil", "getFileInfo", &fileTestStubClient{statFn: func(context.Context, *nodev1.StatFileRequest) (*nodev1.FileInfo, error) {
			return nil, nil
		}}, http.StatusBadGateway},
		{"info wrong path", "getFileInfo", &fileTestStubClient{statFn: func(context.Context, *nodev1.StatFileRequest) (*nodev1.FileInfo, error) {
			return fileTestInfo("/home/user/other", nodev1.FileType_FILE_TYPE_REGULAR), nil
		}}, http.StatusBadGateway},
		{"info ok", "getFileInfo", &fileTestStubClient{statFn: func(context.Context, *nodev1.StatFileRequest) (*nodev1.FileInfo, error) {
			return fileTestInfo("/home/user/note.txt", nodev1.FileType_FILE_TYPE_REGULAR), nil
		}}, http.StatusOK},

		{"folder nil", "createFolder", &fileTestStubClient{mkdirFn: func(context.Context, *nodev1.MakeDirRequest) (*nodev1.FileInfo, error) {
			return nil, nil
		}}, http.StatusBadGateway},
		{"folder wrong path", "createFolder", &fileTestStubClient{mkdirFn: func(context.Context, *nodev1.MakeDirRequest) (*nodev1.FileInfo, error) {
			return fileTestInfo("/home/user/other", nodev1.FileType_FILE_TYPE_DIRECTORY), nil
		}}, http.StatusBadGateway},
		{"folder wrong type", "createFolder", &fileTestStubClient{mkdirFn: func(context.Context, *nodev1.MakeDirRequest) (*nodev1.FileInfo, error) {
			return fileTestInfo("/home/user/dir", nodev1.FileType_FILE_TYPE_REGULAR), nil
		}}, http.StatusBadGateway},
		{"folder ok", "createFolder", &fileTestStubClient{mkdirFn: func(context.Context, *nodev1.MakeDirRequest) (*nodev1.FileInfo, error) {
			return fileTestInfo("/home/user/dir", nodev1.FileType_FILE_TYPE_DIRECTORY), nil
		}}, http.StatusOK},

		{"permissions nil", "setFilePermissions", &fileTestStubClient{chmodFn: func(context.Context, *nodev1.ChmodFileRequest) (*nodev1.FileInfo, error) {
			return nil, nil
		}}, http.StatusBadGateway},
		{"permissions wrong path", "setFilePermissions", &fileTestStubClient{chmodFn: func(context.Context, *nodev1.ChmodFileRequest) (*nodev1.FileInfo, error) {
			return fileTestInfo("/home/user/other", nodev1.FileType_FILE_TYPE_REGULAR), nil
		}}, http.StatusBadGateway},
		{"permissions ok", "setFilePermissions", &fileTestStubClient{chmodFn: func(context.Context, *nodev1.ChmodFileRequest) (*nodev1.FileInfo, error) {
			return fileTestInfo("/home/user/a.txt", nodev1.FileType_FILE_TYPE_REGULAR), nil
		}}, http.StatusOK},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recorder := fileTestRunLight(t, test.client, test.op)
			if test.status == http.StatusOK {
				fileTestAssertSuccess(t, recorder, map[string]string{
					"deleteFile":         api.TypeFileDeleted,
					"moveFile":           api.TypeFileMoved,
					"listFiles":          api.TypeFileList,
					"getFileInfo":        api.TypeFileInfo,
					"createFolder":       api.TypeFolderCreated,
					"setFilePermissions": api.TypeFilePermissionsSet,
				}[test.op])
				return
			}
			fileTestAssertError(t, recorder, test.status, "sandbox_direct_failed")
		})
	}
}

// TestFilesWriteSendProtocol covers Send returning io.EOF while the native
// final status is only available from CloseAndRecv, plus cancellation of the
// child RPC context on every exit.
func TestFilesWriteSendProtocol(t *testing.T) {
	tests := []struct {
		name       string
		failAt     int
		sendErr    error
		closeErr   error
		closeResp  *nodev1.PutFileResponse
		wantStatus int
		wantCode   string
		wantSends  int
		wantClose  int
	}{
		{
			name: "header EOF final invalid path", failAt: 1, sendErr: io.EOF,
			closeErr:   status.Error(codes.InvalidArgument, "native secret"),
			wantStatus: http.StatusBadRequest, wantCode: "invalid_path",
			wantSends: 1, wantClose: 1,
		},
		{
			name: "body EOF final invalid path", failAt: 2, sendErr: io.EOF,
			closeErr:   status.Error(codes.InvalidArgument, "native secret"),
			wantStatus: http.StatusBadRequest, wantCode: "invalid_path",
			wantSends: 2, wantClose: 1,
		},
		{
			name: "body EOF early success is protocol failure", failAt: 2, sendErr: io.EOF,
			closeResp:  &nodev1.PutFileResponse{Path: "/home/user/a.txt", Size: 5},
			wantStatus: http.StatusBadGateway, wantCode: "sandbox_direct_failed",
			wantSends: 2, wantClose: 1,
		},
		{
			name: "body non-EOF failure", failAt: 2, sendErr: status.Error(codes.Internal, "native secret"),
			wantStatus: http.StatusBadGateway, wantCode: "sandbox_direct_failed",
			wantSends: 2, wantClose: 0,
		},
		{
			name: "body resource exhausted is not 503", failAt: 2, sendErr: status.Error(codes.ResourceExhausted, "native secret"),
			wantStatus: http.StatusBadGateway, wantCode: "sandbox_direct_failed",
			wantSends: 2, wantClose: 0,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stream := &fileTestPutStream{
				failAt: test.failAt, sendErr: test.sendErr,
				closeErr: test.closeErr, closeResponse: test.closeResp,
			}
			var captured context.Context
			client := &fileTestStubClient{putFileFn: func(ctx context.Context) (grpc.ClientStreamingClient[nodev1.PutFileRequest, nodev1.PutFileResponse], error) {
				captured = ctx
				return stream, nil
			}}
			recorder := fileTestRunLight(t, client, "writeFile")
			body := fileTestAssertError(t, recorder, test.wantStatus, test.wantCode)
			if stream.sends != test.wantSends {
				t.Errorf("Send calls = %d, want %d", stream.sends, test.wantSends)
			}
			if stream.closeCalls != test.wantClose {
				t.Errorf("CloseAndRecv calls = %d, want %d", stream.closeCalls, test.wantClose)
			}
			if captured == nil {
				t.Fatal("PutFile did not receive a context")
			}
			if captured.Err() == nil {
				t.Error("child RPC context was not canceled after the handler returned")
			}
			if strings.Contains(recorder.Body.String(), "native secret") {
				t.Error("native node text reached the wire")
			}
			if test.wantStatus == http.StatusBadGateway {
				if retryable, ok := body.Error.Details["retryable"].(bool); !ok || !retryable {
					t.Errorf("ambiguous write retryable = %v, want true", body.Error.Details["retryable"])
				}
			}
		})
	}
}

// TestFilesCancelAfterLookupStopsBeforeDial cancels during a successful lookup
// and requires the handler to stop before dialing a node for every operation.
func TestFilesCancelAfterLookupStopsBeforeDial(t *testing.T) {
	for _, operation := range fileTestOperations() {
		t.Run(operation.op, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var factoryCalls atomic.Int32
			lookup := fileTestLookupFunc(func(context.Context, string, string) (api.CommandTarget, error) {
				cancel()
				return api.CommandTarget{NodeID: fileTestNodeID, State: api.StateReady}, nil
			})
			factory := func(string) (nodev1.NodeClient, func() error, error) {
				factoryCalls.Add(1)
				return &fileTestStubClient{}, func() error { return nil }, nil
			}
			handlers := newFileTestLight(t, lookup, factory)
			request := fileTestRequest(operation.method, operation.target, operation.body, fileTestIdentity(), fileTestSandboxID).
				WithContext(auth.WithIdentity(ctx, auth.Identity{Owner: fileTestOwner}))
			writer := newFileTestResponseWriter()
			fileTestHandler(t, handlers, operation.op)(writer, request)
			if factoryCalls.Load() != 0 {
				t.Errorf("factory reached %d times after cancellation", factoryCalls.Load())
			}
			if writer.wrote {
				t.Errorf("canceled handler wrote status %d body %q", writer.status, writer.body.String())
			}
		})
	}
}

// fileTestOperationStub returns a stub with the operation's method wired to fn.
func fileTestOperationStub(op string, fn func(context.Context) (any, error)) *fileTestStubClient {
	client := &fileTestStubClient{}
	wrapped := func(ctx context.Context) (any, error) { return fn(ctx) }
	switch op {
	case "readFile":
		client.readFn = func(ctx context.Context, _ *nodev1.ReadFileRequest) (grpc.ServerStreamingClient[nodev1.ReadFileResponse], error) {
			value, err := wrapped(ctx)
			if err != nil {
				return nil, err
			}
			return value.(grpc.ServerStreamingClient[nodev1.ReadFileResponse]), nil
		}
	case "writeFile":
		client.putFileFn = func(ctx context.Context) (grpc.ClientStreamingClient[nodev1.PutFileRequest, nodev1.PutFileResponse], error) {
			value, err := wrapped(ctx)
			if err != nil {
				return nil, err
			}
			return value.(grpc.ClientStreamingClient[nodev1.PutFileRequest, nodev1.PutFileResponse]), nil
		}
	case "deleteFile":
		client.deleteFn = func(ctx context.Context, _ *nodev1.DeleteFileRequest) (*nodev1.DeleteFileResponse, error) {
			value, err := wrapped(ctx)
			if err != nil {
				return nil, err
			}
			return value.(*nodev1.DeleteFileResponse), nil
		}
	case "listFiles":
		client.listFn = func(ctx context.Context, _ *nodev1.ListFilesRequest) (*nodev1.ListFilesResponse, error) {
			value, err := wrapped(ctx)
			if err != nil {
				return nil, err
			}
			return value.(*nodev1.ListFilesResponse), nil
		}
	case "getFileInfo":
		client.statFn = func(ctx context.Context, _ *nodev1.StatFileRequest) (*nodev1.FileInfo, error) {
			value, err := wrapped(ctx)
			if err != nil {
				return nil, err
			}
			return value.(*nodev1.FileInfo), nil
		}
	case "createFolder":
		client.mkdirFn = func(ctx context.Context, _ *nodev1.MakeDirRequest) (*nodev1.FileInfo, error) {
			value, err := wrapped(ctx)
			if err != nil {
				return nil, err
			}
			return value.(*nodev1.FileInfo), nil
		}
	case "moveFile":
		client.moveFn = func(ctx context.Context, _ *nodev1.MoveFileRequest) (*nodev1.MoveFileResponse, error) {
			value, err := wrapped(ctx)
			if err != nil {
				return nil, err
			}
			return value.(*nodev1.MoveFileResponse), nil
		}
	case "setFilePermissions":
		client.chmodFn = func(ctx context.Context, _ *nodev1.ChmodFileRequest) (*nodev1.FileInfo, error) {
			value, err := wrapped(ctx)
			if err != nil {
				return nil, err
			}
			return value.(*nodev1.FileInfo), nil
		}
	}
	return client
}

// fileTestSuccessValue returns a valid native response of op.
func fileTestSuccessValue(op string) any {
	switch op {
	case "readFile":
		return &fileTestReadStreamClient{messages: []*nodev1.ReadFileResponse{
			{Part: &nodev1.ReadFileResponse_Info{Info: fileTestInfo("/home/user/note.txt", nodev1.FileType_FILE_TYPE_REGULAR)}},
			{Part: &nodev1.ReadFileResponse_Data{Data: []byte("x")}},
		}}
	case "writeFile":
		return &fileTestPutStream{closeResponse: &nodev1.PutFileResponse{Path: "/home/user/note.txt", Size: 1}}
	case "deleteFile":
		return &nodev1.DeleteFileResponse{Path: "/home/user/note.txt"}
	case "listFiles":
		return &nodev1.ListFilesResponse{Path: "/home/user"}
	case "getFileInfo":
		return fileTestInfo("/home/user/note.txt", nodev1.FileType_FILE_TYPE_REGULAR)
	case "createFolder":
		return fileTestInfo("/home/user/dir", nodev1.FileType_FILE_TYPE_DIRECTORY)
	case "moveFile":
		return &nodev1.MoveFileResponse{Source: "/home/user/a.txt", Destination: "/home/user/b.txt"}
	case "setFilePermissions":
		return fileTestInfo("/home/user/a.txt", nodev1.FileType_FILE_TYPE_REGULAR)
	}
	panic("unknown file operation " + op)
}

// TestFilesCancelInFactoryStopsBeforeRPC cancels inside a successful node
// factory and requires the handler to skip the node RPC.
func TestFilesCancelInFactoryStopsBeforeRPC(t *testing.T) {
	for _, operation := range fileTestOperations() {
		t.Run(operation.op, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var rpcCalls atomic.Int32
			client := fileTestOperationStub(operation.op, func(context.Context) (any, error) {
				rpcCalls.Add(1)
				return fileTestSuccessValue(operation.op), nil
			})
			factory := func(string) (nodev1.NodeClient, func() error, error) {
				cancel()
				return client, func() error { return nil }, nil
			}
			handlers := newFileTestLight(t, fileTestReadyLookup(nil), factory)
			request := fileTestRequest(operation.method, operation.target, operation.body, fileTestIdentity(), fileTestSandboxID).
				WithContext(auth.WithIdentity(ctx, auth.Identity{Owner: fileTestOwner}))
			writer := newFileTestResponseWriter()
			fileTestHandler(t, handlers, operation.op)(writer, request)
			if rpcCalls.Load() != 0 {
				t.Errorf("node RPC reached %d times after factory cancellation", rpcCalls.Load())
			}
			if writer.wrote {
				t.Errorf("canceled handler wrote status %d body %q", writer.status, writer.body.String())
			}
		})
	}
}

// TestFilesCancelDuringRPCSuppressesResponse cancels after a successful node
// RPC but before the response and requires all eight handlers to write nothing.
func TestFilesCancelDuringRPCSuppressesResponse(t *testing.T) {
	for _, operation := range fileTestOperations() {
		t.Run(operation.op, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			client := fileTestOperationStub(operation.op, func(context.Context) (any, error) {
				cancel()
				return fileTestSuccessValue(operation.op), nil
			})
			handlers := newFileTestLight(t, fileTestReadyLookup(nil), fileTestStubFactory(client))
			request := fileTestRequest(operation.method, operation.target, operation.body, fileTestIdentity(), fileTestSandboxID).
				WithContext(auth.WithIdentity(ctx, auth.Identity{Owner: fileTestOwner}))
			writer := newFileTestResponseWriter()
			fileTestHandler(t, handlers, operation.op)(writer, request)
			if writer.wrote {
				t.Errorf("canceled handler wrote status %d body %q", writer.status, writer.body.String())
			}
		})
	}
}

// TestFilesCreateFolderZeroMode covers the approved explicit-0000 protocol:
// Stat first; an existing path gets a plain MakeDir (mode kept), a missing one
// gets MakeDir then Chmod 0, and a Chmod failure is a request error without a
// cleanup or a false success.
func TestFilesCreateFolderZeroMode(t *testing.T) {
	base := "/api/v1/sandboxes/" + fileTestSandboxID
	t.Run("new directory final 0000", func(t *testing.T) {
		env := newFileTestEnv(t)
		recorder := env.call(t, "createFolder", base+"/files/folder", `{"path":"zero","mode":"0000"}`)
		fileTestAssertSuccess(t, recorder, api.TypeFolderCreated)
		entry := fileTestDecode[api.FileInfoResponse](t, recorder)
		if entry.Mode != "0000" || !entry.IsDir || entry.Path != "/home/user/zero" {
			t.Errorf("zero folder = %+v", entry)
		}
		info, err := os.Stat(env.disk("/home/user/zero"))
		if err != nil {
			t.Fatalf("stat zero: %v", err)
		}
		if info.Mode().Perm() != 0 {
			t.Errorf("zero disk mode = %v", info.Mode())
		}
		if got := env.node.count(fileTestStat); got != 1 {
			t.Errorf("StatFile calls = %d, want 1", got)
		}
		if got := env.node.count(fileTestMkdir); got != 1 {
			t.Errorf("MakeDir calls = %d, want 1", got)
		}
		if got := env.node.count(fileTestChmod); got != 1 {
			t.Errorf("ChmodFile calls = %d, want 1", got)
		}
	})
	t.Run("existing directory keeps mode", func(t *testing.T) {
		env := newFileTestEnv(t)
		if err := os.MkdirAll(env.disk("/home/user/stable"), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(env.disk("/home/user/stable"), 0o750); err != nil {
			t.Fatal(err)
		}
		recorder := env.call(t, "createFolder", base+"/files/folder", `{"path":"stable","mode":"0000"}`)
		fileTestAssertSuccess(t, recorder, api.TypeFolderCreated)
		entry := fileTestDecode[api.FileInfoResponse](t, recorder)
		if entry.Mode != "0750" || !entry.IsDir {
			t.Errorf("stable folder = %+v, want the existing 0750 mode", entry)
		}
		if got := env.node.count(fileTestStat); got != 1 {
			t.Errorf("StatFile calls = %d, want 1", got)
		}
		if got := env.node.count(fileTestMkdir); got != 1 {
			t.Errorf("MakeDir calls = %d, want 1", got)
		}
		if got := env.node.count(fileTestChmod); got != 0 {
			t.Errorf("ChmodFile calls = %d, want 0", got)
		}
	})
	t.Run("existing non-directory 400", func(t *testing.T) {
		env := newFileTestEnv(t)
		env.writeDisk("/home/user/file.txt", []byte("f"))
		recorder := env.call(t, "createFolder", base+"/files/folder", `{"path":"file.txt","mode":"0000"}`)
		body := fileTestAssertError(t, recorder, http.StatusBadRequest, "invalid_path")
		if body.Message != "path exists and is not a directory" {
			t.Errorf("message = %q", body.Message)
		}
		if got := env.node.count(fileTestStat); got != 1 {
			t.Errorf("StatFile calls = %d, want 1", got)
		}
		if got := env.node.count(fileTestChmod); got != 0 {
			t.Errorf("ChmodFile calls = %d, want 0", got)
		}
	})
	t.Run("chmod failure after mkdir is a request error", func(t *testing.T) {
		env := newFileTestEnv(t)
		env.node.chmodHook = func(*nodev1.ChmodFileRequest) (*nodev1.FileInfo, error) {
			return nil, status.Error(codes.Internal, "native secret")
		}
		recorder := env.call(t, "createFolder", base+"/files/folder", `{"path":"half","mode":"0000"}`)
		fileTestAssertError(t, recorder, http.StatusBadGateway, "sandbox_direct_failed")
		if env.node.count(fileTestDelete) != 0 {
			t.Errorf("DeleteFile calls = %d, want 0 (no cleanup)", env.node.count(fileTestDelete))
		}
		info, err := os.Stat(env.disk("/home/user/half"))
		if err != nil || !info.IsDir() {
			t.Errorf("created directory missing after chmod failure: %v", err)
		}
		if info.Mode().Perm() != 0o755 {
			t.Errorf("half-created mode = %v, want the mkdir default 0755", info.Mode())
		}
	})
}

// TestFilesCreateFolderZeroModeCancellation cancels at each zero-mode phase
// and requires the handler to stop before the next RPC or response.
func TestFilesCreateFolderZeroModeCancellation(t *testing.T) {
	operation := fileTestOperationFor("createFolder")
	existing := fileTestInfo("/home/user/zero", nodev1.FileType_FILE_TYPE_DIRECTORY)
	tests := []struct {
		name      string
		stat      func(context.Context, context.CancelFunc) (*nodev1.FileInfo, error)
		mkdir     func(context.Context, context.CancelFunc) (*nodev1.FileInfo, error)
		chmod     func(context.Context, context.CancelFunc) (*nodev1.FileInfo, error)
		wantMkdir int
		wantChmod int
	}{
		{
			name: "cancel during stat",
			stat: func(_ context.Context, cancel context.CancelFunc) (*nodev1.FileInfo, error) {
				cancel()
				return existing, nil
			},
			wantMkdir: 0, wantChmod: 0,
		},
		{
			name: "cancel during mkdir",
			stat: func(context.Context, context.CancelFunc) (*nodev1.FileInfo, error) {
				return nil, status.Error(codes.NotFound, "missing")
			},
			mkdir: func(_ context.Context, cancel context.CancelFunc) (*nodev1.FileInfo, error) {
				cancel()
				return existing, nil
			},
			wantMkdir: 1, wantChmod: 0,
		},
		{
			name: "cancel during chmod",
			stat: func(context.Context, context.CancelFunc) (*nodev1.FileInfo, error) {
				return nil, status.Error(codes.NotFound, "missing")
			},
			mkdir: func(context.Context, context.CancelFunc) (*nodev1.FileInfo, error) {
				return existing, nil
			},
			chmod: func(_ context.Context, cancel context.CancelFunc) (*nodev1.FileInfo, error) {
				cancel()
				return existing, nil
			},
			wantMkdir: 1, wantChmod: 1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var mkdirCalls, chmodCalls atomic.Int32
			client := &fileTestStubClient{}
			if test.stat != nil {
				client.statFn = func(ctx context.Context, _ *nodev1.StatFileRequest) (*nodev1.FileInfo, error) {
					return test.stat(ctx, cancel)
				}
			}
			if test.mkdir != nil {
				client.mkdirFn = func(ctx context.Context, _ *nodev1.MakeDirRequest) (*nodev1.FileInfo, error) {
					mkdirCalls.Add(1)
					return test.mkdir(ctx, cancel)
				}
			}
			if test.chmod != nil {
				client.chmodFn = func(ctx context.Context, _ *nodev1.ChmodFileRequest) (*nodev1.FileInfo, error) {
					chmodCalls.Add(1)
					return test.chmod(ctx, cancel)
				}
			}
			handlers := newFileTestLight(t, fileTestReadyLookup(nil), fileTestStubFactory(client))
			request := fileTestRequest(operation.method, operation.target, `{"path":"zero","mode":"0000"}`, fileTestIdentity(), fileTestSandboxID).
				WithContext(auth.WithIdentity(ctx, auth.Identity{Owner: fileTestOwner}))
			writer := newFileTestResponseWriter()
			handlers.CreateFolder(writer, request)
			if int(mkdirCalls.Load()) != test.wantMkdir {
				t.Errorf("MakeDir calls = %d, want %d", mkdirCalls.Load(), test.wantMkdir)
			}
			if int(chmodCalls.Load()) != test.wantChmod {
				t.Errorf("ChmodFile calls = %d, want %d", chmodCalls.Load(), test.wantChmod)
			}
			if writer.wrote {
				t.Errorf("canceled handler wrote status %d body %q", writer.status, writer.body.String())
			}
		})
	}
}

// TestFilesZeroPaddedModes accepts arbitrarily zero-padded octal modes within
// 07777.
func TestFilesZeroPaddedModes(t *testing.T) {
	env := newFileTestEnv(t)
	base := "/api/v1/sandboxes/" + fileTestSandboxID
	recorder := env.call(t, "createFolder", base+"/files/folder", `{"path":"padded","mode":"0000000755"}`)
	fileTestAssertSuccess(t, recorder, api.TypeFolderCreated)
	folder := fileTestDecode[api.FileInfoResponse](t, recorder)
	if folder.Mode != "0755" {
		t.Errorf("padded folder mode = %q", folder.Mode)
	}
	env.writeDisk("/home/user/padded.txt", []byte("x"))
	recorder = env.call(t, "setFilePermissions", base+"/files/permissions", `{"path":"padded.txt","mode":"0000000644"}`)
	fileTestAssertSuccess(t, recorder, api.TypeFilePermissionsSet)
	file := fileTestDecode[api.FileInfoResponse](t, recorder)
	if file.Mode != "0644" {
		t.Errorf("padded file mode = %q", file.Mode)
	}
}

// TestFilesReadSizeSnapshotMismatch proves the read bound uses the actual
// streamed bytes, not the node's size snapshot.
func TestFilesReadSizeSnapshotMismatch(t *testing.T) {
	base := "/api/v1/sandboxes/" + fileTestSandboxID
	tests := []struct {
		name     string
		infoSize int64
		data     []byte
	}{
		{"grown after info", 1, []byte("abcdef")},
		{"shrunk after info", 1000, []byte("xyz")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			env := newFileTestEnv(t)
			info := fileTestInfo("/home/user/snapshot.bin", nodev1.FileType_FILE_TYPE_REGULAR)
			info.Size = test.infoSize
			env.node.setReadHook(func(_ *nodev1.ReadFileRequest, stream grpc.ServerStreamingServer[nodev1.ReadFileResponse]) error {
				if err := fileTestSendReadInfo(stream, info); err != nil {
					return err
				}
				return fileTestSendReadData(stream, test.data)
			})
			recorder := env.call(t, "readFile", base+"/files?path=snapshot.bin", "")
			fileTestAssertSuccess(t, recorder, api.TypeFileRead)
			read := fileTestDecode[api.FileReadResponse](t, recorder)
			if read.Content != string(test.data) || read.Size != int64(len(test.data)) {
				t.Errorf("read = %+v, want the actual %d bytes", read, len(test.data))
			}
		})
	}
}

// TestFilesWriteEscapedLimit proves the raw JSON bound accommodates a decoded
// 5 MiB file whose text is escaped to the six-byte worst case.
func TestFilesWriteEscapedLimit(t *testing.T) {
	if testing.Short() {
		t.Skip("allocates a ~30 MiB JSON body")
	}
	env := newFileTestEnv(t)
	base := "/api/v1/sandboxes/" + fileTestSandboxID
	content := string(bytes.Repeat([]byte{0}, int(api.FileInlineLimitBytes)))
	raw, err := json.Marshal(api.FileWriteRequest{Path: "escaped.bin", Content: content})
	if err != nil {
		t.Fatalf("marshal escaped request: %v", err)
	}
	if int64(len(raw)) <= 6*api.FileInlineLimitBytes {
		t.Fatalf("escaped body = %d bytes, want more than 6x the inline limit", len(raw))
	}
	request := httptest.NewRequest(http.MethodPut, base+"/files", bytes.NewReader(raw))
	request.Header.Set("Content-Type", "application/json")
	request = request.WithContext(auth.WithIdentity(t.Context(), auth.Identity{Owner: fileTestOwner}))
	request.SetPathValue(api.ParamSandboxID, fileTestSandboxID)
	recorder := httptest.NewRecorder()
	env.handlers.WriteFile(recorder, request)
	fileTestAssertSuccess(t, recorder, api.TypeFileWritten)
	stored := env.readDisk("/home/user/escaped.bin")
	if len(stored) != len(content) {
		t.Fatalf("stored size = %d, want %d", len(stored), len(content))
	}
	if !bytes.Equal(stored, []byte(content)) {
		t.Error("stored escaped content differs from the decoded bytes")
	}
}

// TestFilesReadOutOfRangeInitialCall covers OUT_OF_RANGE returned by the
// initial ReadFile call, before any stream message.
func TestFilesReadOutOfRangeInitialCall(t *testing.T) {
	tests := []struct {
		name     string
		stat     *nodev1.FileInfo
		wantSize bool
	}{
		{
			name:     "valid regular oversized stat",
			stat:     &nodev1.FileInfo{Path: "/home/user/note.txt", Name: "note.txt", Type: nodev1.FileType_FILE_TYPE_REGULAR, Size: api.FileInlineLimitBytes + 5, Mode: 0o644},
			wantSize: true,
		},
		{
			name: "wrong path stat",
			stat: &nodev1.FileInfo{Path: "/home/user/other", Name: "other", Type: nodev1.FileType_FILE_TYPE_REGULAR, Size: api.FileInlineLimitBytes + 5, Mode: 0o644},
		},
		{
			name: "directory stat",
			stat: &nodev1.FileInfo{Path: "/home/user/note.txt", Name: "note.txt", Type: nodev1.FileType_FILE_TYPE_DIRECTORY, Size: api.FileInlineLimitBytes + 5, Mode: 0o755},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &fileTestStubClient{
				readFn: func(context.Context, *nodev1.ReadFileRequest) (grpc.ServerStreamingClient[nodev1.ReadFileResponse], error) {
					return nil, status.Error(codes.OutOfRange, "native secret text")
				},
				statFn: func(context.Context, *nodev1.StatFileRequest) (*nodev1.FileInfo, error) {
					return test.stat, nil
				},
			}
			recorder := fileTestRunLight(t, client, "readFile")
			body := fileTestAssertError(t, recorder, http.StatusRequestEntityTooLarge, "file_too_large")
			size, hasSize := body.Error.Details["sizeBytes"]
			if test.wantSize != hasSize {
				t.Errorf("sizeBytes present = %v, want %v (%v)", hasSize, test.wantSize, size)
			}
			if test.wantSize && size != float64(test.stat.GetSize()) {
				t.Errorf("sizeBytes = %v, want %d", size, test.stat.GetSize())
			}
			if strings.Contains(recorder.Body.String(), "native secret text") {
				t.Error("native node text reached the wire")
			}
		})
	}
}

// TestFilesWriteNoFalseSuccess proves a node that completes PutFile without
// the requested bytes never produces a success response or a partial file.
func TestFilesWriteNoFalseSuccess(t *testing.T) {
	env := newFileTestEnv(t)
	base := "/api/v1/sandboxes/" + fileTestSandboxID
	env.node.setPutHook(func(stream grpc.ClientStreamingServer[nodev1.PutFileRequest, nodev1.PutFileResponse]) error {
		header, _, err := fileTestDrainPut(stream)
		if err != nil {
			return err
		}
		return stream.SendAndClose(&nodev1.PutFileResponse{Path: header.GetPath(), Size: 0})
	})
	recorder := env.call(t, "writeFile", base+"/files", `{"path":"a.txt","content":"hello"}`)
	fileTestAssertError(t, recorder, http.StatusBadGateway, "sandbox_direct_failed")
	if env.node.count(fileTestWrite) != 1 {
		t.Errorf("PutFile calls = %d, want 1", env.node.count(fileTestWrite))
	}
	if _, err := os.Stat(env.disk("/home/user/a.txt")); !os.IsNotExist(err) {
		t.Errorf("false success left a file: %v", err)
	}
}

// TestFilesCreateFolderZeroModeContracts covers malformed native zero-mode
// responses; every one is an ambiguous 502, never a success.
func TestFilesCreateFolderZeroModeContracts(t *testing.T) {
	existing := fileTestInfo("/home/user/zero", nodev1.FileType_FILE_TYPE_DIRECTORY)
	missing := func(context.Context, *nodev1.StatFileRequest) (*nodev1.FileInfo, error) {
		return nil, status.Error(codes.NotFound, "missing")
	}
	tests := []struct {
		name  string
		stat  func(context.Context, *nodev1.StatFileRequest) (*nodev1.FileInfo, error)
		mkdir func(context.Context, *nodev1.MakeDirRequest) (*nodev1.FileInfo, error)
		chmod func(context.Context, *nodev1.ChmodFileRequest) (*nodev1.FileInfo, error)
	}{
		{
			name: "stat wrong path",
			stat: func(context.Context, *nodev1.StatFileRequest) (*nodev1.FileInfo, error) {
				return fileTestInfo("/home/user/other", nodev1.FileType_FILE_TYPE_DIRECTORY), nil
			},
		},
		{
			name: "stat invalid metadata",
			stat: func(context.Context, *nodev1.StatFileRequest) (*nodev1.FileInfo, error) {
				return &nodev1.FileInfo{Path: "/home/user/zero", Name: "zero"}, nil
			},
		},
		{
			name: "stat internal",
			stat: func(context.Context, *nodev1.StatFileRequest) (*nodev1.FileInfo, error) {
				return nil, status.Error(codes.Internal, "native")
			},
		},
		{
			name: "existing mkdir wrong path",
			stat: func(context.Context, *nodev1.StatFileRequest) (*nodev1.FileInfo, error) { return existing, nil },
			mkdir: func(context.Context, *nodev1.MakeDirRequest) (*nodev1.FileInfo, error) {
				return fileTestInfo("/home/user/other", nodev1.FileType_FILE_TYPE_DIRECTORY), nil
			},
		},
		{
			name: "existing mkdir wrong type",
			stat: func(context.Context, *nodev1.StatFileRequest) (*nodev1.FileInfo, error) { return existing, nil },
			mkdir: func(context.Context, *nodev1.MakeDirRequest) (*nodev1.FileInfo, error) {
				return fileTestInfo("/home/user/zero", nodev1.FileType_FILE_TYPE_REGULAR), nil
			},
		},
		{
			name: "existing mkdir invalid metadata",
			stat: func(context.Context, *nodev1.StatFileRequest) (*nodev1.FileInfo, error) { return existing, nil },
			mkdir: func(context.Context, *nodev1.MakeDirRequest) (*nodev1.FileInfo, error) {
				return &nodev1.FileInfo{Path: "/home/user/zero", Name: "zero", Type: nodev1.FileType_FILE_TYPE_DIRECTORY, Size: -1}, nil
			},
		},
		{
			name:  "missing mkdir nil",
			stat:  missing,
			mkdir: func(context.Context, *nodev1.MakeDirRequest) (*nodev1.FileInfo, error) { return nil, nil },
		},
		{
			name: "missing mkdir wrong type",
			stat: missing,
			mkdir: func(context.Context, *nodev1.MakeDirRequest) (*nodev1.FileInfo, error) {
				return fileTestInfo("/home/user/zero", nodev1.FileType_FILE_TYPE_REGULAR), nil
			},
		},
		{
			name: "missing mkdir error",
			stat: missing,
			mkdir: func(context.Context, *nodev1.MakeDirRequest) (*nodev1.FileInfo, error) {
				return nil, status.Error(codes.Internal, "native")
			},
		},
		{
			name: "chmod wrong path",
			stat: missing,
			mkdir: func(context.Context, *nodev1.MakeDirRequest) (*nodev1.FileInfo, error) {
				return existing, nil
			},
			chmod: func(context.Context, *nodev1.ChmodFileRequest) (*nodev1.FileInfo, error) {
				return fileTestInfo("/home/user/other", nodev1.FileType_FILE_TYPE_DIRECTORY), nil
			},
		},
		{
			name: "chmod invalid metadata",
			stat: missing,
			mkdir: func(context.Context, *nodev1.MakeDirRequest) (*nodev1.FileInfo, error) {
				return existing, nil
			},
			chmod: func(context.Context, *nodev1.ChmodFileRequest) (*nodev1.FileInfo, error) {
				return &nodev1.FileInfo{Path: "/home/user/zero", Name: "zero", Type: nodev1.FileType_FILE_TYPE_DIRECTORY, Size: -1}, nil
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &fileTestStubClient{statFn: test.stat, mkdirFn: test.mkdir, chmodFn: test.chmod}
			handlers := newFileTestLight(t, fileTestReadyLookup(nil), fileTestStubFactory(client))
			operation := fileTestOperationFor("createFolder")
			request := fileTestRequest(operation.method, operation.target, `{"path":"zero","mode":"0000"}`, fileTestIdentity(), fileTestSandboxID)
			recorder := fileTestServe(handlers.CreateFolder, request)
			fileTestAssertError(t, recorder, http.StatusBadGateway, "sandbox_direct_failed")
		})
	}
}

// TestFilesNilNodeClient treats a factory that returns no client as an
// internal configuration failure, never a success or a native call.
func TestFilesNilNodeClient(t *testing.T) {
	factory := func(string) (nodev1.NodeClient, func() error, error) {
		return nil, func() error { return nil }, nil
	}
	handlers := newFileTestLight(t, fileTestReadyLookup(nil), factory)
	operation := fileTestOperationFor("readFile")
	request := fileTestRequest(operation.method, operation.target, "", fileTestIdentity(), fileTestSandboxID)
	recorder := fileTestServe(handlers.ReadFile, request)
	fileTestAssertError(t, recorder, http.StatusInternalServerError, "http_500")
}

// TestFilesFactoryErrorIsAmbiguousNodeFailure maps a factory failure to 502
// with the retryable flag of the operation.
func TestFilesFactoryErrorIsAmbiguousNodeFailure(t *testing.T) {
	factory := func(string) (nodev1.NodeClient, func() error, error) {
		return nil, nil, errors.New("factory broke")
	}
	handlers := newFileTestLight(t, fileTestReadyLookup(nil), factory)
	operation := fileTestOperationFor("writeFile")
	request := fileTestRequest(operation.method, operation.target, operation.body, fileTestIdentity(), fileTestSandboxID)
	recorder := fileTestServe(handlers.WriteFile, request)
	body := fileTestAssertError(t, recorder, http.StatusBadGateway, "sandbox_direct_failed")
	if retryable, ok := body.Error.Details["retryable"].(bool); !ok || !retryable {
		t.Errorf("write factory failure retryable = %v, want true", body.Error.Details["retryable"])
	}
}

// fileTestCountingReadStream replays messages and counts Recv calls.
type fileTestCountingReadStream struct {
	grpc.ServerStreamingClient[nodev1.ReadFileResponse]
	messages  []*nodev1.ReadFileResponse
	index     int
	recvCalls int
	onRecv    func(*fileTestCountingReadStream)
}

func (s *fileTestCountingReadStream) Recv() (*nodev1.ReadFileResponse, error) {
	s.recvCalls++
	if s.onRecv != nil {
		s.onRecv(s)
	}
	if s.index >= len(s.messages) {
		return nil, io.EOF
	}
	message := s.messages[s.index]
	s.index++
	return message, nil
}

// TestFilesNilStreams treats a nil stream returned with a nil error as a
// protocol fault, never a panic.
func TestFilesNilStreams(t *testing.T) {
	t.Run("read", func(t *testing.T) {
		client := &fileTestStubClient{readFn: func(context.Context, *nodev1.ReadFileRequest) (grpc.ServerStreamingClient[nodev1.ReadFileResponse], error) {
			return nil, nil
		}}
		recorder := fileTestRunLight(t, client, "readFile")
		fileTestAssertError(t, recorder, http.StatusBadGateway, "sandbox_direct_failed")
	})
	t.Run("write", func(t *testing.T) {
		client := &fileTestStubClient{putFileFn: func(context.Context) (grpc.ClientStreamingClient[nodev1.PutFileRequest, nodev1.PutFileResponse], error) {
			return nil, nil
		}}
		recorder := fileTestRunLight(t, client, "writeFile")
		fileTestAssertError(t, recorder, http.StatusBadGateway, "sandbox_direct_failed")
	})
}

// TestFilesReadTypedNilOneof treats typed-nil oneof wrappers as protocol
// faults.
func TestFilesReadTypedNilOneof(t *testing.T) {
	tests := []struct {
		name     string
		messages []*nodev1.ReadFileResponse
	}{
		{
			name:     "typed nil info",
			messages: []*nodev1.ReadFileResponse{{Part: (*nodev1.ReadFileResponse_Info)(nil)}},
		},
		{
			name: "typed nil data",
			messages: []*nodev1.ReadFileResponse{
				{Part: &nodev1.ReadFileResponse_Info{Info: fileTestInfo("/home/user/note.txt", nodev1.FileType_FILE_TYPE_REGULAR)}},
				{Part: (*nodev1.ReadFileResponse_Data)(nil)},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &fileTestStubClient{readFn: func(context.Context, *nodev1.ReadFileRequest) (grpc.ServerStreamingClient[nodev1.ReadFileResponse], error) {
				return &fileTestCountingReadStream{messages: test.messages}, nil
			}}
			recorder := fileTestRunLight(t, client, "readFile")
			fileTestAssertError(t, recorder, http.StatusBadGateway, "sandbox_direct_failed")
		})
	}
}

// TestFilesEntryCancellation cancels before the handler starts and requires
// no lookup, no node and no response for all eight operations.
func TestFilesEntryCancellation(t *testing.T) {
	for _, operation := range fileTestOperations() {
		t.Run(operation.op, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			var lookupCalls, factoryCalls atomic.Int32
			lookup := fileTestLookupFunc(func(context.Context, string, string) (api.CommandTarget, error) {
				lookupCalls.Add(1)
				return api.CommandTarget{NodeID: fileTestNodeID, State: api.StateReady}, nil
			})
			factory := func(string) (nodev1.NodeClient, func() error, error) {
				factoryCalls.Add(1)
				return &fileTestStubClient{}, func() error { return nil }, nil
			}
			handlers := newFileTestLight(t, lookup, factory)
			request := fileTestRequest(operation.method, operation.target, operation.body, fileTestIdentity(), fileTestSandboxID).
				WithContext(auth.WithIdentity(ctx, auth.Identity{Owner: fileTestOwner}))
			writer := newFileTestResponseWriter()
			fileTestHandler(t, handlers, operation.op)(writer, request)
			if lookupCalls.Load() != 0 {
				t.Errorf("lookup calls = %d, want 0", lookupCalls.Load())
			}
			if factoryCalls.Load() != 0 {
				t.Errorf("factory calls = %d, want 0", factoryCalls.Load())
			}
			if writer.wrote {
				t.Errorf("canceled handler wrote status %d body %q", writer.status, writer.body.String())
			}
		})
	}
}

// TestFilesCancelOnOpen cancels while the streaming RPC opens and requires no
// first Recv or Send.
func TestFilesCancelOnOpen(t *testing.T) {
	t.Run("read", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		stream := &fileTestCountingReadStream{}
		client := &fileTestStubClient{readFn: func(context.Context, *nodev1.ReadFileRequest) (grpc.ServerStreamingClient[nodev1.ReadFileResponse], error) {
			cancel()
			return stream, nil
		}}
		handlers := newFileTestLight(t, fileTestReadyLookup(nil), fileTestStubFactory(client))
		operation := fileTestOperationFor("readFile")
		request := fileTestRequest(operation.method, operation.target, "", fileTestIdentity(), fileTestSandboxID).
			WithContext(auth.WithIdentity(ctx, auth.Identity{Owner: fileTestOwner}))
		writer := newFileTestResponseWriter()
		handlers.ReadFile(writer, request)
		if stream.recvCalls != 0 {
			t.Errorf("Recv calls = %d, want 0", stream.recvCalls)
		}
		if writer.wrote {
			t.Errorf("canceled handler wrote status %d body %q", writer.status, writer.body.String())
		}
	})
	t.Run("write", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		stream := &fileTestPutStream{}
		client := &fileTestStubClient{putFileFn: func(context.Context) (grpc.ClientStreamingClient[nodev1.PutFileRequest, nodev1.PutFileResponse], error) {
			cancel()
			return stream, nil
		}}
		handlers := newFileTestLight(t, fileTestReadyLookup(nil), fileTestStubFactory(client))
		operation := fileTestOperationFor("writeFile")
		request := fileTestRequest(operation.method, operation.target, operation.body, fileTestIdentity(), fileTestSandboxID).
			WithContext(auth.WithIdentity(ctx, auth.Identity{Owner: fileTestOwner}))
		writer := newFileTestResponseWriter()
		handlers.WriteFile(writer, request)
		if stream.sends != 0 {
			t.Errorf("Send calls = %d, want 0", stream.sends)
		}
		if writer.wrote {
			t.Errorf("canceled handler wrote status %d body %q", writer.status, writer.body.String())
		}
	})
}

// TestFilesCancelDuringReadData cancels after the first received message and
// requires the next Recv to be skipped and no response written.
func TestFilesCancelDuringReadData(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	stream := &fileTestCountingReadStream{
		messages: []*nodev1.ReadFileResponse{
			{Part: &nodev1.ReadFileResponse_Info{Info: fileTestInfo("/home/user/note.txt", nodev1.FileType_FILE_TYPE_REGULAR)}},
			{Part: &nodev1.ReadFileResponse_Data{Data: []byte("x")}},
		},
	}
	stream.onRecv = func(s *fileTestCountingReadStream) {
		if s.recvCalls == 1 {
			cancel()
		}
	}
	client := &fileTestStubClient{readFn: func(context.Context, *nodev1.ReadFileRequest) (grpc.ServerStreamingClient[nodev1.ReadFileResponse], error) {
		return stream, nil
	}}
	handlers := newFileTestLight(t, fileTestReadyLookup(nil), fileTestStubFactory(client))
	operation := fileTestOperationFor("readFile")
	request := fileTestRequest(operation.method, operation.target, "", fileTestIdentity(), fileTestSandboxID).
		WithContext(auth.WithIdentity(ctx, auth.Identity{Owner: fileTestOwner}))
	writer := newFileTestResponseWriter()
	handlers.ReadFile(writer, request)
	if stream.recvCalls != 1 {
		t.Errorf("Recv calls = %d, want 1", stream.recvCalls)
	}
	if writer.wrote {
		t.Errorf("canceled handler wrote status %d body %q", writer.status, writer.body.String())
	}
}

// TestFilesZeroModeMetadataGuards covers malformed zero-mode metadata: a bad
// MakeDir record must not reach Chmod, and a non-directory Chmod record must
// not become a folder success.
func TestFilesZeroModeMetadataGuards(t *testing.T) {
	operation := fileTestOperationFor("createFolder")
	t.Run("malformed mkdir metadata skips chmod", func(t *testing.T) {
		var chmodCalls atomic.Int32
		client := &fileTestStubClient{
			statFn: func(context.Context, *nodev1.StatFileRequest) (*nodev1.FileInfo, error) {
				return nil, status.Error(codes.NotFound, "missing")
			},
			mkdirFn: func(context.Context, *nodev1.MakeDirRequest) (*nodev1.FileInfo, error) {
				return &nodev1.FileInfo{
					Path: "/home/user/zero", Name: "zero",
					Type: nodev1.FileType_FILE_TYPE_DIRECTORY, Size: -1, Mode: 0o755,
				}, nil
			},
			chmodFn: func(context.Context, *nodev1.ChmodFileRequest) (*nodev1.FileInfo, error) {
				chmodCalls.Add(1)
				return fileTestInfo("/home/user/zero", nodev1.FileType_FILE_TYPE_DIRECTORY), nil
			},
		}
		handlers := newFileTestLight(t, fileTestReadyLookup(nil), fileTestStubFactory(client))
		request := fileTestRequest(operation.method, operation.target, `{"path":"zero","mode":"0000"}`, fileTestIdentity(), fileTestSandboxID)
		recorder := fileTestServe(handlers.CreateFolder, request)
		fileTestAssertError(t, recorder, http.StatusBadGateway, "sandbox_direct_failed")
		if chmodCalls.Load() != 0 {
			t.Errorf("ChmodFile calls = %d, want 0", chmodCalls.Load())
		}
	})
	t.Run("non-directory chmod result", func(t *testing.T) {
		client := &fileTestStubClient{
			statFn: func(context.Context, *nodev1.StatFileRequest) (*nodev1.FileInfo, error) {
				return nil, status.Error(codes.NotFound, "missing")
			},
			mkdirFn: func(context.Context, *nodev1.MakeDirRequest) (*nodev1.FileInfo, error) {
				return fileTestInfo("/home/user/zero", nodev1.FileType_FILE_TYPE_DIRECTORY), nil
			},
			chmodFn: func(context.Context, *nodev1.ChmodFileRequest) (*nodev1.FileInfo, error) {
				return fileTestInfo("/home/user/zero", nodev1.FileType_FILE_TYPE_REGULAR), nil
			},
		}
		handlers := newFileTestLight(t, fileTestReadyLookup(nil), fileTestStubFactory(client))
		request := fileTestRequest(operation.method, operation.target, `{"path":"zero","mode":"0000"}`, fileTestIdentity(), fileTestSandboxID)
		recorder := fileTestServe(handlers.CreateFolder, request)
		fileTestAssertError(t, recorder, http.StatusBadGateway, "sandbox_direct_failed")
	})
}
