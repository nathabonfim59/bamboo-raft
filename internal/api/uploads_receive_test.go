package api_test

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
	"github.com/nathabonfim59/bamboo-raft/internal/apierr"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

func uploadsTestNewStream() *uploadsTestPutStream {
	return &uploadsTestPutStream{
		send: func(*nodev1.PutFileRequest) error { return nil },
		recv: func() (*nodev1.PutFileResponse, error) {
			return &nodev1.PutFileResponse{}, nil
		},
		closeSend: make(chan struct{}),
		terminal:  make(chan struct{}),
	}
}

func uploadsTestAssertClosed(t *testing.T, ch <-chan struct{}, worker string) {
	t.Helper()
	if ch == nil {
		return
	}
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Errorf("%s did not exit", worker)
	}
}

func uploadsTestStreamFactory(stream *uploadsTestPutStream) api.NodeClientFactory {
	client := &uploadsTestNodeClient{putFile: func(ctx context.Context) (grpc.ClientStreamingClient[nodev1.PutFileRequest, nodev1.PutFileResponse], error) {
		stream.ctx = ctx
		return stream, nil
	}}
	return uploadsTestFactory(client)
}

func uploadsTestDecodeUploaded(t *testing.T, recorder *httptest.ResponseRecorder) api.UploadedFile {
	t.Helper()
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", recorder.Code)
	}
	var response struct {
		OK   bool   `json:"ok"`
		Type string `json:"type"`
		api.UploadedFile
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		// Never print the body or decoder text: a regression could put a
		// token or secret there before the leak checks run.
		t.Fatalf("decode upload response failed (%T)", err)
	}
	if !response.OK || response.Type != api.TypeFileUploaded {
		t.Fatalf("envelope = ok:%v type:%q", response.OK, response.Type)
	}
	return response.UploadedFile
}

func TestUploadReceiveRealStack(t *testing.T) {
	env := uploadsTestStartEnv(t)
	clock := newUploadsTestClock()
	lookup := uploadsTestReadyLookup()
	handlers := uploadsTestHandlers(t, lookup, env.factory(), clock)
	token := uploadsTestIssueToken(t, handlers, uploadsTestOwner, uploadsTestSandboxID, "uploads/large.bin")

	data := make([]byte, 5<<20+12345)
	for i := range data {
		data[i] = byte(i*31 + 7)
	}
	reader := &uploadsTestReader{data: data, chunk: 64 << 10}
	recorder := uploadsTestReceive(t, handlers, http.MethodPut, token, reader)
	response := uploadsTestDecodeUploaded(t, recorder)
	if response.Path != "/home/user/uploads/large.bin" || response.Size != int64(len(data)) {
		t.Errorf("response = path:%q size:%d", response.Path, response.Size)
	}
	if !reader.completed {
		t.Error("handler stopped reading a live upload")
	}
	stored, ok := uploadsTestReadGuestFile(t, env, uploadsTestSandboxID, response.Path)
	if !ok {
		t.Fatal("uploaded file is missing")
	}
	if !bytes.Equal(stored, data) {
		t.Fatalf("stored file differs: got %d bytes, want %d", len(stored), len(data))
	}

	// A token is reusable for the same target until it expires, and the
	// replacement is atomic at the node.
	replacement := []byte("second upload")
	recorder = uploadsTestReceive(t, handlers, http.MethodPut, token, bytes.NewReader(replacement))
	response = uploadsTestDecodeUploaded(t, recorder)
	if response.Size != int64(len(replacement)) {
		t.Errorf("replacement size = %d", response.Size)
	}
	stored, ok = uploadsTestReadGuestFile(t, env, uploadsTestSandboxID, response.Path)
	if !ok || !bytes.Equal(stored, replacement) {
		t.Fatal("replacement upload was not stored")
	}
	if calls := lookup.snapshot(); len(calls) != 3 || calls[2].Owner != uploadsTestOwner || calls[2].SandboxID != uploadsTestSandboxID {
		t.Errorf("redemption lookup calls = %+v", calls)
	}
}

func TestUploadReceiveEmptyFile(t *testing.T) {
	env := uploadsTestStartEnv(t)
	handlers := uploadsTestHandlers(t, uploadsTestReadyLookup(), env.factory(), newUploadsTestClock())
	token := uploadsTestIssueToken(t, handlers, uploadsTestOwner, uploadsTestSandboxID, "/tmp/empty.bin")
	recorder := uploadsTestReceive(t, handlers, http.MethodPut, token, strings.NewReader(""))
	response := uploadsTestDecodeUploaded(t, recorder)
	if response.Path != "/tmp/empty.bin" || response.Size != 0 {
		t.Errorf("response = path:%q size:%d", response.Path, response.Size)
	}
	stored, ok := uploadsTestReadGuestFile(t, env, uploadsTestSandboxID, response.Path)
	if !ok || len(stored) != 0 {
		t.Fatal("empty file was not created")
	}
}

func TestUploadReceiveFinalChunkEOF(t *testing.T) {
	env := uploadsTestStartEnv(t)
	handlers := uploadsTestHandlers(t, uploadsTestReadyLookup(), env.factory(), newUploadsTestClock())
	token := uploadsTestIssueToken(t, handlers, uploadsTestOwner, uploadsTestSandboxID, "exact.bin")
	// One Read returns a full 1 MiB chunk together with io.EOF.
	data := bytes.Repeat([]byte{0x5a}, 1<<20)
	reader := &uploadsTestReader{data: data, chunk: len(data)}
	recorder := uploadsTestReceive(t, handlers, http.MethodPut, token, reader)
	response := uploadsTestDecodeUploaded(t, recorder)
	if response.Size != int64(len(data)) {
		t.Errorf("size = %d, want %d", response.Size, len(data))
	}
	stored, ok := uploadsTestReadGuestFile(t, env, uploadsTestSandboxID, response.Path)
	if !ok || !bytes.Equal(stored, data) {
		t.Fatal("final n>0+EOF chunk was not preserved")
	}
}

func TestUploadReceiveEarlyNodeRejection(t *testing.T) {
	tests := []struct {
		name       string
		body       string
		closeErr   error
		wantStatus int
		wantCode   string
	}{
		{"empty body rejected", "", status.Error(codes.InvalidArgument, "bad path"), http.StatusBadRequest, apierr.CodeInvalidPath},
		{"not found", "", status.Error(codes.NotFound, "sandbox gone"), http.StatusNotFound, apierr.CodeNotFound},
		{"not ready", "data", status.Error(codes.FailedPrecondition, "not ready"), http.StatusConflict, apierr.CodeSandboxNotReady},
		{"capacity", "data", status.Error(codes.ResourceExhausted, "full"), http.StatusServiceUnavailable, apierr.CodeOutOfCapacity},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			clock := newUploadsTestClock()
			stream := uploadsTestNewStream()
			stream.early = true
			stream.recv = func() (*nodev1.PutFileResponse, error) { return nil, test.closeErr }
			handlers := uploadsTestHandlers(t, uploadsTestReadyLookup(), uploadsTestStreamFactory(stream), clock)
			token := uploadsTestIssueToken(t, handlers, uploadsTestOwner, uploadsTestSandboxID, "rejected.bin")
			recorder := uploadsTestReceive(t, handlers, http.MethodPut, token, strings.NewReader(test.body))
			envelope := uploadsTestTargets(t, recorder, test.wantStatus, test.wantCode)
			if test.wantStatus == http.StatusServiceUnavailable && envelope.Error.Details["retryAfterSeconds"] != float64(5) {
				t.Errorf("retryAfterSeconds = %v", envelope.Error.Details["retryAfterSeconds"])
			}
			if stream.recvCalls() == 0 {
				t.Error("CloseAndRecv was not called")
			}
			uploadsTestAssertNoToken(t, recorder, token)
		})
	}
}

func TestUploadReceiveSendFailureStopsReading(t *testing.T) {
	clock := newUploadsTestClock()
	stream := uploadsTestNewStream()
	sends := 0
	stream.send = func(request *nodev1.PutFileRequest) error {
		if request.GetHeader() != nil {
			return nil
		}
		sends++
		if sends == 1 {
			return nil
		}
		// Send reports io.EOF without the real status; CloseAndRecv is
		// authoritative.
		return io.EOF
	}
	stream.recv = func() (*nodev1.PutFileResponse, error) {
		return nil, status.Error(codes.FailedPrecondition, "node rejected the upload")
	}
	handlers := uploadsTestHandlers(t, uploadsTestReadyLookup(), uploadsTestStreamFactory(stream), clock)
	token := uploadsTestIssueToken(t, handlers, uploadsTestOwner, uploadsTestSandboxID, "partial.bin")

	reader := &uploadsTestReader{data: make([]byte, 1<<20), chunk: 1024}
	recorder := uploadsTestReceive(t, handlers, http.MethodPut, token, reader)
	uploadsTestTargets(t, recorder, http.StatusConflict, apierr.CodeSandboxNotReady)
	if stream.recvCalls() == 0 {
		t.Error("CloseAndRecv was not called after the Send failure")
	}
	if reader.reads > 2 {
		t.Errorf("handler kept reading after the node failed: %d reads", reader.reads)
	}
	if reader.completed {
		t.Error("handler read the whole body after the node failed")
	}
}

func TestUploadReceiveBodyReadFailure(t *testing.T) {
	stream := uploadsTestNewStream()
	handlers := uploadsTestHandlers(t, uploadsTestReadyLookup(), uploadsTestStreamFactory(stream), newUploadsTestClock())
	token := uploadsTestIssueToken(t, handlers, uploadsTestOwner, uploadsTestSandboxID, "broken.bin")
	reader := &uploadsTestErrorReader{err: errors.New("client body failed")}
	recorder := uploadsTestReceive(t, handlers, http.MethodPut, token, reader)
	uploadsTestTargets(t, recorder, http.StatusBadRequest, apierr.CodeInvalidJSON)
	if stream.recvCalls() == 0 {
		t.Error("body failure did not join the node RPC")
	}
	select {
	case <-stream.ctx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("body failure did not cancel the node RPC")
	}
}

func TestUploadReceiveCancellation(t *testing.T) {
	stream := uploadsTestNewStream()
	stream.recv = func() (*nodev1.PutFileResponse, error) { return nil, context.Canceled }
	handlers := uploadsTestHandlers(t, uploadsTestReadyLookup(), uploadsTestStreamFactory(stream), newUploadsTestClock())
	token := uploadsTestIssueToken(t, handlers, uploadsTestOwner, uploadsTestSandboxID, "cancel.bin")

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	reader := &uploadsTestBlockingReader{ctx: ctx, started: make(chan struct{}), closed: make(chan struct{}), readDone: make(chan struct{})}
	request := uploadsTestReceiveRequest(http.MethodPut, token, reader).WithContext(ctx)
	request.Body = reader
	recorder := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		handlers.Receive(recorder, request)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		_ = reader.Close()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Errorf("canceled upload did not exit after cleanup")
		}
	})
	select {
	case <-reader.started:
	case <-time.After(5 * time.Second):
		t.Fatal("upload never started reading")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("canceled upload did not return")
	}
	if recorder.Body.Len() != 0 {
		t.Error("canceled upload wrote a response body")
	}
	select {
	case <-reader.closed:
	default:
		t.Error("canceled upload did not close the request body")
	}
	if stream.recvCalls() == 0 {
		t.Error("canceled upload did not join the node RPC")
	}
	select {
	case <-stream.ctx.Done():
	default:
		t.Error("canceled upload did not cancel the node RPC context")
	}
	uploadsTestAssertClosed(t, reader.readDone, "body pump")
}

func TestUploadReceiveNodeClientFailures(t *testing.T) {
	tests := []struct {
		name       string
		factory    api.NodeClientFactory
		wantStatus int
		wantCode   string
	}{
		{
			name: "dial error",
			factory: func(string) (nodev1.NodeClient, func() error, error) {
				return nil, nil, errors.New("gateway unavailable")
			},
			wantStatus: http.StatusBadGateway,
			wantCode:   apierr.CodeSandboxDirectFailed,
		},
		{
			name: "nil client",
			factory: func(string) (nodev1.NodeClient, func() error, error) {
				return nil, func() error { return nil }, nil
			},
			wantStatus: http.StatusInternalServerError,
			wantCode:   apierr.CodeInternal,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handlers := uploadsTestHandlers(t, uploadsTestReadyLookup(), test.factory, newUploadsTestClock())
			token := uploadsTestIssueToken(t, handlers, uploadsTestOwner, uploadsTestSandboxID, "x")
			recorder := uploadsTestReceive(t, handlers, http.MethodPut, token, strings.NewReader("data"))
			uploadsTestTargets(t, recorder, test.wantStatus, test.wantCode)
			uploadsTestAssertNoToken(t, recorder, token)
		})
	}
}

func TestUploadReceiveNodeResultMismatch(t *testing.T) {
	tests := []struct {
		name     string
		response *nodev1.PutFileResponse
	}{
		{"wrong path", &nodev1.PutFileResponse{Path: "/home/user/other.bin", Size: 4}},
		{"wrong size", &nodev1.PutFileResponse{Path: "/home/user/x", Size: 99}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stream := uploadsTestNewStream()
			stream.recv = func() (*nodev1.PutFileResponse, error) { return test.response, nil }
			handlers := uploadsTestHandlers(t, uploadsTestReadyLookup(), uploadsTestStreamFactory(stream), newUploadsTestClock())
			token := uploadsTestIssueToken(t, handlers, uploadsTestOwner, uploadsTestSandboxID, "x")
			recorder := uploadsTestReceive(t, handlers, http.MethodPut, token, strings.NewReader("data"))
			uploadsTestTargets(t, recorder, http.StatusBadGateway, apierr.CodeSandboxDirectFailed)
		})
	}
}

func TestUploadReceiveSendFailureWithoutRecvStatus(t *testing.T) {
	stream := uploadsTestNewStream()
	stream.send = func(request *nodev1.PutFileRequest) error {
		if request.GetHeader() != nil {
			return status.Error(codes.InvalidArgument, "bad header")
		}
		return nil
	}
	// The node claims success even though the client failed to send the
	// header. That is a protocol violation, never a successful upload.
	stream.recv = func() (*nodev1.PutFileResponse, error) {
		return &nodev1.PutFileResponse{Path: "/home/user/x", Size: 0}, nil
	}
	handlers := uploadsTestHandlers(t, uploadsTestReadyLookup(), uploadsTestStreamFactory(stream), newUploadsTestClock())
	token := uploadsTestIssueToken(t, handlers, uploadsTestOwner, uploadsTestSandboxID, "x")
	recorder := uploadsTestReceive(t, handlers, http.MethodPut, token, strings.NewReader(""))
	uploadsTestTargets(t, recorder, http.StatusBadGateway, apierr.CodeSandboxDirectFailed)
	if stream.recvCalls() == 0 {
		t.Error("header Send failure did not observe the final receive")
	}
}

func TestUploadReceiveClientClose(t *testing.T) {
	tests := []struct {
		name    string
		closeFn func() error
	}{
		{"nil close", nil},
		{"close error", func() error { return errors.New("close failed") }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stream := uploadsTestNewStream()
			stream.recv = func() (*nodev1.PutFileResponse, error) {
				return &nodev1.PutFileResponse{Path: "/home/user/x", Size: 0}, nil
			}
			client := &uploadsTestNodeClient{putFile: func(ctx context.Context) (grpc.ClientStreamingClient[nodev1.PutFileRequest, nodev1.PutFileResponse], error) {
				stream.ctx = ctx
				return stream, nil
			}}
			factory := func(string) (nodev1.NodeClient, func() error, error) {
				return client, test.closeFn, nil
			}
			handlers := uploadsTestHandlers(t, uploadsTestReadyLookup(), factory, newUploadsTestClock())
			token := uploadsTestIssueToken(t, handlers, uploadsTestOwner, uploadsTestSandboxID, "x")
			recorder := uploadsTestReceive(t, handlers, http.MethodPut, token, strings.NewReader(""))
			uploadsTestDecodeUploaded(t, recorder)
		})
	}
}

func TestUploadReceiveTokenOwnerAndTargetIsolation(t *testing.T) {
	env := uploadsTestStartEnv(t)
	env.createVM(t, uploadsTestOtherSandboxID)
	clock := newUploadsTestClock()

	// The Authorization header is irrelevant; the token's owner is used.
	lookup := uploadsTestReadyLookup()
	handlers := uploadsTestHandlers(t, lookup, env.factory(), clock)
	token := uploadsTestIssueToken(t, handlers, uploadsTestOwner, uploadsTestSandboxID, "owned.bin")
	request := uploadsTestReceiveRequest(http.MethodPut, token, strings.NewReader("owned"))
	request.Header.Set("Authorization", "Bearer unrelated")
	recorder := httptest.NewRecorder()
	handlers.Receive(recorder, request)
	uploadsTestDecodeUploaded(t, recorder)
	calls := lookup.snapshot()
	if len(calls) != 2 || calls[1].Owner != uploadsTestOwner || calls[1].SandboxID != uploadsTestSandboxID {
		t.Errorf("receiver did not use the token owner/target: %+v", calls)
	}

	// A token for another sandbox writes only that sandbox.
	otherToken := uploadsTestIssueToken(t, handlers, uploadsTestOtherOwner, uploadsTestOtherSandboxID, "other.bin")
	recorder = uploadsTestReceive(t, handlers, http.MethodPut, otherToken, strings.NewReader("other"))
	uploadsTestDecodeUploaded(t, recorder)
	otherData, ok := uploadsTestReadGuestFile(t, env, uploadsTestOtherSandboxID, "/home/user/other.bin")
	if !ok || string(otherData) != "other" {
		t.Fatal("other-sandbox upload was not written to its target")
	}
	if _, ok := uploadsTestReadGuestFile(t, env, uploadsTestSandboxID, "/home/user/other.bin"); ok {
		t.Fatal("other-sandbox token wrote into the first sandbox")
	}

	// Distinct handler instances holding the same key redeem each other's
	// tokens without process-local state.
	secondLookup := uploadsTestReadyLookup()
	second := uploadsTestHandlers(t, secondLookup, env.factory(), clock)
	recorder = uploadsTestReceive(t, second, http.MethodPut, token, strings.NewReader("cross"))
	uploadsTestDecodeUploaded(t, recorder)
	if calls := secondLookup.snapshot(); len(calls) != 1 || calls[0].Owner != uploadsTestOwner {
		t.Errorf("second instance lookups = %+v", calls)
	}
	stored, ok := uploadsTestReadGuestFile(t, env, uploadsTestSandboxID, "/home/user/owned.bin")
	if !ok || string(stored) != "cross" {
		t.Fatal("second instance did not redeem the first instance's token")
	}

	// A different installation key refuses the token before touching state.
	otherKeyLookup := uploadsTestReadyLookup()
	otherKey := uploadsTestHandlers(t, otherKeyLookup, env.factory(), clock, func(options *api.UploadsOptions) {
		options.SigningKey = []byte("another-installation-signing-key-0123456789")
	})
	recorder = uploadsTestReceive(t, otherKey, http.MethodPut, token, strings.NewReader("wrong key"))
	uploadsTestTargets(t, recorder, http.StatusNotFound, apierr.CodeNotFound)
	if otherKeyLookup.count() != 0 {
		t.Error("a token from another key reached the lookup")
	}
}

func TestUploadReceiveStaleTarget(t *testing.T) {
	tests := []struct {
		name       string
		stale      func(t *testing.T, env *uploadsTestEnv, lookup *uploadsTestLookup)
		wantStatus int
		wantCode   string
	}{
		{
			name: "state changed",
			stale: func(_ *testing.T, _ *uploadsTestEnv, lookup *uploadsTestLookup) {
				lookup.set(api.CommandTarget{NodeID: uploadsTestNodeID, State: api.StateArchived}, nil)
			},
			wantStatus: http.StatusConflict,
			wantCode:   apierr.CodeSandboxNotReady,
		},
		{
			name: "deleted before redemption",
			stale: func(_ *testing.T, _ *uploadsTestEnv, lookup *uploadsTestLookup) {
				lookup.set(api.CommandTarget{}, api.ErrCommandTargetNotFound)
			},
			wantStatus: http.StatusNotFound,
			wantCode:   apierr.CodeNotFound,
		},
		{
			name: "node deleted before redemption",
			stale: func(t *testing.T, env *uploadsTestEnv, _ *uploadsTestLookup) {
				if _, err := env.link.Client.DeleteVM(t.Context(), &nodev1.DeleteVMRequest{SandboxId: uploadsTestSandboxID}); err != nil {
					t.Fatalf("DeleteVM: %v", err)
				}
			},
			wantStatus: http.StatusNotFound,
			wantCode:   apierr.CodeNotFound,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			env := uploadsTestStartEnv(t)
			clock := newUploadsTestClock()
			lookup := uploadsTestReadyLookup()
			handlers := uploadsTestHandlers(t, lookup, env.factory(), clock)
			token := uploadsTestIssueToken(t, handlers, uploadsTestOwner, uploadsTestSandboxID, "stale.bin")
			test.stale(t, env, lookup)
			recorder := uploadsTestReceive(t, handlers, http.MethodPut, token, strings.NewReader("must not land"))
			uploadsTestTargets(t, recorder, test.wantStatus, test.wantCode)
			uploadsTestAssertNoToken(t, recorder, token)
			if _, ok := uploadsTestReadGuestFile(t, env, uploadsTestSandboxID, "/home/user/stale.bin"); ok {
				t.Fatal("a stale token wrote a file")
			}
		})
	}
}

func TestUploadReceiverMethodGuard(t *testing.T) {
	lookup := uploadsTestReadyLookup()
	handlers := uploadsTestHandlers(t, lookup, uploadsTestFactory(&uploadsTestNodeClient{}), newUploadsTestClock())
	token := uploadsTestIssueToken(t, handlers, uploadsTestOwner, uploadsTestSandboxID, "x")
	for _, method := range []string{http.MethodGet, http.MethodPost, http.MethodDelete} {
		t.Run(method, func(t *testing.T) {
			recorder := uploadsTestReceive(t, handlers, method, token, strings.NewReader("data"))
			uploadsTestTargets(t, recorder, http.StatusMethodNotAllowed, apierr.CodeNotFound)
			uploadsTestAssertNoToken(t, recorder, token)
		})
	}
	// Method guard runs before the token, so even an empty token is refused
	// the same way.
	recorder := uploadsTestReceive(t, handlers, http.MethodGet, "", nil)
	uploadsTestTargets(t, recorder, http.StatusMethodNotAllowed, apierr.CodeNotFound)
}

func TestUploadReceiverMissingToken(t *testing.T) {
	lookup := uploadsTestReadyLookup()
	handlers := uploadsTestHandlers(t, lookup, uploadsTestFactory(&uploadsTestNodeClient{}), newUploadsTestClock())
	recorder := uploadsTestReceive(t, handlers, http.MethodPut, "", strings.NewReader("data"))
	uploadsTestTargets(t, recorder, http.StatusNotFound, apierr.CodeNotFound)
	if lookup.count() != 0 {
		t.Error("missing token reached the lookup")
	}
}

// uploadsTestErrorReader returns a non-EOF failure immediately.
type uploadsTestErrorReader struct {
	err   error
	reads int
	done  chan struct{}
}

func (r *uploadsTestErrorReader) Read([]byte) (int, error) {
	r.reads++
	if r.done != nil {
		close(r.done)
	}
	return 0, r.err
}

func (r *uploadsTestErrorReader) Close() error { return nil }

// uploadsTestBlockingReader blocks until the request context ends.
type uploadsTestBlockingReader struct {
	ctx       context.Context
	started   chan struct{}
	closed    chan struct{}
	readDone  chan struct{}
	once      sync.Once
	closeOnce sync.Once
}

func (r *uploadsTestBlockingReader) Read([]byte) (int, error) {
	r.once.Do(func() { close(r.started) })
	var err error
	select {
	case <-r.ctx.Done():
		err = r.ctx.Err()
	case <-r.closed:
		err = io.ErrClosedPipe
	}
	if r.readDone != nil {
		close(r.readDone)
	}
	return 0, err
}

func (r *uploadsTestBlockingReader) Close() error {
	r.closeOnce.Do(func() {
		if r.closed != nil {
			close(r.closed)
		}
	})
	return nil
}

// uploadsTestCloseRecorder records whether the handler closed a real HTTP
// request body and the first data read.
type uploadsTestCloseRecorder struct {
	io.ReadCloser
	readStarted  chan struct{}
	readOnce     sync.Once
	dataRead     chan struct{}
	dataOnce     sync.Once
	readDone     chan struct{}
	readDoneOnce sync.Once
	closed       atomic.Bool
}

func (r *uploadsTestCloseRecorder) Read(p []byte) (int, error) {
	r.readOnce.Do(func() { close(r.readStarted) })
	n, err := r.ReadCloser.Read(p)
	if n > 0 && r.dataRead != nil {
		r.dataOnce.Do(func() { close(r.dataRead) })
	}
	if err != nil && r.readDone != nil {
		r.readDoneOnce.Do(func() { close(r.readDone) })
	}
	return n, err
}

func (r *uploadsTestCloseRecorder) Close() error {
	r.closed.Store(true)
	return r.ReadCloser.Close()
}

// TestUploadReceiverNoAuthorization makes the bootstrap-free contract explicit.
func TestUploadReceiverNoAuthorization(t *testing.T) {
	env := uploadsTestStartEnv(t)
	handlers := uploadsTestHandlers(t, uploadsTestReadyLookup(), env.factory(), newUploadsTestClock())
	token := uploadsTestIssueToken(t, handlers, uploadsTestOwner, uploadsTestSandboxID, "anonymous.bin")
	recorder := uploadsTestReceive(t, handlers, http.MethodPut, token, strings.NewReader("anonymous"))
	uploadsTestDecodeUploaded(t, recorder)
}

func TestUploadReceiveNilBody(t *testing.T) {
	stream := uploadsTestNewStream()
	stream.recv = func() (*nodev1.PutFileResponse, error) {
		return &nodev1.PutFileResponse{Path: "/home/user/x", Size: 0}, nil
	}
	handlers := uploadsTestHandlers(t, uploadsTestReadyLookup(), uploadsTestStreamFactory(stream), newUploadsTestClock())
	token := uploadsTestIssueToken(t, handlers, uploadsTestOwner, uploadsTestSandboxID, "x")
	request := uploadsTestReceiveRequest(http.MethodPut, token, nil)
	request.Body = nil
	recorder := httptest.NewRecorder()
	handlers.Receive(recorder, request)
	uploadsTestDecodeUploaded(t, recorder)
}

func TestUploadLongControlPathIssueRedeem(t *testing.T) {
	const maxPath = 4096
	controlPath := "/home/user/" + strings.Repeat("\x01", maxPath-len("/home/user/"))
	stream := uploadsTestNewStream()
	stream.recv = func() (*nodev1.PutFileResponse, error) {
		return &nodev1.PutFileResponse{Path: controlPath, Size: 0}, nil
	}
	handlers := uploadsTestHandlers(t, uploadsTestReadyLookup(), uploadsTestStreamFactory(stream), newUploadsTestClock())
	token := uploadsTestIssueToken(t, handlers, uploadsTestOwner, uploadsTestSandboxID, controlPath)
	recorder := uploadsTestReceive(t, handlers, http.MethodPut, token, strings.NewReader(""))
	response := uploadsTestDecodeUploaded(t, recorder)
	if response.Path != controlPath {
		t.Error("control-character path did not round-trip through the signed token")
	}
}

func TestUploadPathLimitIssuance(t *testing.T) {
	tooLong := "/home/user/" + strings.Repeat("a", 4096)
	body, err := json.Marshal(map[string]string{"path": tooLong})
	if err != nil {
		t.Fatal(err)
	}
	handlers := uploadsTestHandlers(t, uploadsTestReadyLookup(), uploadsTestFactory(&uploadsTestNodeClient{}), newUploadsTestClock())
	recorder := uploadsTestIssue(t, handlers, uploadsTestOwner, uploadsTestSandboxID, string(body))
	uploadsTestTargets(t, recorder, http.StatusBadRequest, apierr.CodeInvalidPath)
	uploadsTestValidateCreateUploadURL(t, recorder)
}

func TestUploadReceiveNilClientCloses(t *testing.T) {
	closed := make(chan struct{})
	factory := func(string) (nodev1.NodeClient, func() error, error) {
		return nil, func() error { close(closed); return nil }, nil
	}
	handlers := uploadsTestHandlers(t, uploadsTestReadyLookup(), factory, newUploadsTestClock())
	token := uploadsTestIssueToken(t, handlers, uploadsTestOwner, uploadsTestSandboxID, "x")
	recorder := uploadsTestReceive(t, handlers, http.MethodPut, token, strings.NewReader("data"))
	uploadsTestTargets(t, recorder, http.StatusInternalServerError, apierr.CodeInternal)
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("nil client did not close its closer")
	}
}

func TestUploadReceiveClientErrorsDoNotLeak(t *testing.T) {
	const secret = "client-secret-material"
	tests := []struct {
		name       string
		factory    func(closed chan struct{}) api.NodeClientFactory
		wantStatus int
		wantCode   string
		wantClosed bool
	}{
		{
			name: "factory error",
			factory: func(chan struct{}) api.NodeClientFactory {
				return func(string) (nodev1.NodeClient, func() error, error) {
					return nil, nil, errors.New(secret)
				}
			},
			wantStatus: http.StatusBadGateway,
			wantCode:   apierr.CodeSandboxDirectFailed,
		},
		{
			name: "nil client close error",
			factory: func(closed chan struct{}) api.NodeClientFactory {
				return func(string) (nodev1.NodeClient, func() error, error) {
					return nil, func() error {
						close(closed)
						return errors.New(secret)
					}, nil
				}
			},
			wantStatus: http.StatusInternalServerError,
			wantCode:   apierr.CodeInternal,
			wantClosed: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var logs bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
			closed := make(chan struct{})
			handlers := uploadsTestHandlers(t, uploadsTestReadyLookup(), test.factory(closed), newUploadsTestClock(), func(options *api.UploadsOptions) {
				options.Logger = logger
			})
			token := uploadsTestIssueToken(t, handlers, uploadsTestOwner, uploadsTestSandboxID, "x")
			recorder := uploadsTestReceive(t, handlers, http.MethodPut, token, strings.NewReader("data"))
			uploadsTestTargets(t, recorder, test.wantStatus, test.wantCode)
			if bytes.Contains(recorder.Body.Bytes(), []byte(secret)) || strings.Contains(logs.String(), secret) {
				t.Fatal("client error text leaked into the response or logs")
			}
			if test.wantClosed {
				select {
				case <-closed:
				case <-time.After(5 * time.Second):
					t.Fatal("closer was not called")
				}
			}
		})
	}
}

func TestUploadReceiveRealHTTPRejectsHeldBody(t *testing.T) {
	tests := []struct {
		name       string
		code       codes.Code
		wantStatus int
	}{
		{"not found", codes.NotFound, http.StatusNotFound},
		{"canceled", codes.Canceled, http.StatusBadGateway},
		{"deadline exceeded", codes.DeadlineExceeded, http.StatusBadGateway},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			uploadsTestRealHTTPHeldBody(t, test.code, test.wantStatus)
		})
	}
}

func uploadsTestRealHTTPHeldBody(t *testing.T, nodeCode codes.Code, wantStatus int) {
	stream := uploadsTestNewStream()
	stream.early = true
	headerSent := make(chan struct{})
	var headerOnce sync.Once
	stream.send = func(request *nodev1.PutFileRequest) error {
		if request.GetHeader() != nil {
			headerOnce.Do(func() { close(headerSent) })
		}
		return nil
	}
	dataRead := make(chan struct{})
	stream.recv = func() (*nodev1.PutFileResponse, error) {
		<-headerSent
		<-dataRead
		return nil, status.Error(nodeCode, "sandbox gone")
	}
	handlers := uploadsTestHandlers(t, uploadsTestReadyLookup(), uploadsTestStreamFactory(stream), newUploadsTestClock())
	token := uploadsTestIssueToken(t, handlers, uploadsTestOwner, uploadsTestSandboxID, "held.bin")

	readStarted := make(chan struct{})
	body := &uploadsTestCloseRecorder{readStarted: readStarted, dataRead: dataRead, readDone: make(chan struct{})}
	handlerDone := make(chan uploadsTestHandlerResult, 1)
	handlerStarted := make(chan struct{})
	handlerFinished := make(chan struct{})
	mux := http.NewServeMux()
	mux.HandleFunc("PUT "+api.UploadReceiverPath, func(w http.ResponseWriter, r *http.Request) {
		body.ReadCloser = r.Body
		r.Body = body
		close(handlerStarted)
		tee := &uploadsTestTeeResponse{ResponseWriter: w}
		handlers.Receive(tee, r)
		handlerDone <- uploadsTestHandlerResult{closed: body.closed.Load(), status: tee.status}
		close(handlerFinished)
	})
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close) // registered first: runs last, after all joins.

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	pipeReader, pipeWriter := io.Pipe()
	request, err := http.NewRequestWithContext(ctx, http.MethodPut, server.URL+"/api/bbraft/uploads/"+token, pipeReader)
	if err != nil {
		cancel()
		_ = pipeWriter.Close()
		t.Fatal(err)
	}
	request.Close = true
	client := &http.Client{Transport: &http.Transport{}}
	clientResult := make(chan uploadsTestHTTPResult, 1)
	clientFinished := make(chan struct{})
	go func() {
		response, err := client.Do(request)
		if err != nil {
			clientResult <- uploadsTestHTTPResult{err: err}
		} else {
			_ = response.Body.Close()
			clientResult <- uploadsTestHTTPResult{status: response.StatusCode}
		}
		close(clientFinished)
	}()

	writeStarted := make(chan struct{})
	writeDone := make(chan struct{})
	t.Cleanup(func() {
		// LIFO-safe cleanup: cancel the request, close the pipe, then join
		// the handler, writer and client before server.Close runs.
		cancel()
		_ = pipeWriter.CloseWithError(io.ErrClosedPipe)
		select {
		case <-handlerStarted:
			select {
			case <-handlerFinished:
			case <-time.After(5 * time.Second):
				t.Errorf("handler did not exit after cleanup")
			}
		default:
		}
		select {
		case <-writeStarted:
			select {
			case <-writeDone:
			case <-time.After(5 * time.Second):
				t.Errorf("client body writer did not stop after cleanup")
			}
		default:
		}
		select {
		case <-clientFinished:
		case <-time.After(5 * time.Second):
			t.Errorf("client did not finish after cleanup")
		}
		client.CloseIdleConnections()
	})

	select {
	case <-readStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("server never started reading the body")
	}
	// One chunk is in flight and the request body stays open.
	close(writeStarted)
	go func() {
		_, _ = pipeWriter.Write([]byte("partial"))
		close(writeDone)
	}()
	select {
	case <-dataRead:
	case <-time.After(5 * time.Second):
		t.Fatal("server never read the in-flight chunk")
	}

	select {
	case done := <-handlerDone:
		if !done.closed {
			t.Fatal("handler did not close the real HTTP body before returning")
		}
		if done.status != wantStatus {
			t.Fatalf("handler status = %d, want %d (recv calls %d, last error %T)", done.status, wantStatus, stream.recvCalls(), stream.lastRecvError())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not return after the node rejected the upload")
	}
	if stream.recvCalls() == 0 {
		t.Error("final receive was not observed")
	}
	select {
	case <-stream.ctx.Done():
	default:
		t.Error("node stream context was not canceled")
	}
	uploadsTestAssertClosed(t, body.readDone, "body pump")
	select {
	case <-clientFinished:
	case <-time.After(5 * time.Second):
		t.Fatal("client did not finish")
	}
	select {
	case result := <-clientResult:
		if result.err != nil {
			t.Fatalf("client request failed (%T)", result.err)
		}
		if result.status != wantStatus {
			t.Errorf("status = %d, want %d", result.status, wantStatus)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("client result missing")
	}
}

func TestUploadReceiveNodeSuccessBeforeBodyEnds(t *testing.T) {
	stream := uploadsTestNewStream()
	stream.early = true
	stream.recv = func() (*nodev1.PutFileResponse, error) {
		return &nodev1.PutFileResponse{Path: "/home/user/x", Size: 0}, nil
	}
	handlers := uploadsTestHandlers(t, uploadsTestReadyLookup(), uploadsTestStreamFactory(stream), newUploadsTestClock())
	token := uploadsTestIssueToken(t, handlers, uploadsTestOwner, uploadsTestSandboxID, "x")
	reader := &uploadsTestHoldReader{started: make(chan struct{}), closed: make(chan struct{}), readDone: make(chan struct{})}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	request := uploadsTestReceiveRequest(http.MethodPut, token, reader).WithContext(ctx)
	request.Body = reader
	recorder := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		handlers.Receive(recorder, request)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		_ = reader.Close()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Errorf("handler did not exit after cleanup")
		}
	})
	select {
	case <-reader.started:
	case <-time.After(5 * time.Second):
		t.Fatal("upload never started reading")
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not stop after an early node success")
	}
	uploadsTestTargets(t, recorder, http.StatusBadGateway, apierr.CodeSandboxDirectFailed)
	select {
	case <-reader.closed:
	default:
		t.Error("handler did not interrupt the partial body")
	}
	uploadsTestAssertClosed(t, reader.readDone, "body pump")
}

// uploadsTestHoldReader blocks until Close.
type uploadsTestHoldReader struct {
	started   chan struct{}
	closed    chan struct{}
	readDone  chan struct{}
	once      sync.Once
	closeOnce sync.Once
}

func (r *uploadsTestHoldReader) Read([]byte) (int, error) {
	r.once.Do(func() { close(r.started) })
	<-r.closed
	if r.readDone != nil {
		close(r.readDone)
	}
	return 0, io.ErrClosedPipe
}

func (r *uploadsTestHoldReader) Close() error {
	r.closeOnce.Do(func() { close(r.closed) })
	return nil
}

func TestUploadReceiveSlowFinalResponseCommit(t *testing.T) {
	// A legitimate final commit may be slower than the cleanup budget. The
	// normal success path must await the caller context, not apply the 5s
	// cleanup timeout.
	const delay = 5*time.Second + 200*time.Millisecond
	stream := uploadsTestNewStream()
	recvReturned := make(chan struct{})
	stream.recv = func() (*nodev1.PutFileResponse, error) {
		time.Sleep(delay)
		close(recvReturned)
		return &nodev1.PutFileResponse{Path: "/home/user/x", Size: 0}, nil
	}
	handlers := uploadsTestHandlers(t, uploadsTestReadyLookup(), uploadsTestStreamFactory(stream), newUploadsTestClock())
	token := uploadsTestIssueToken(t, handlers, uploadsTestOwner, uploadsTestSandboxID, "x")
	started := time.Now()
	recorder := uploadsTestReceive(t, handlers, http.MethodPut, token, strings.NewReader(""))
	uploadsTestDecodeUploaded(t, recorder)
	if elapsed := time.Since(started); elapsed < delay {
		t.Errorf("handler returned before the slow final response (%s)", elapsed)
	}
	select {
	case <-recvReturned:
	default:
		t.Error("final receive worker did not exit")
	}
}

func TestUploadRelativePathBoundary(t *testing.T) {
	// A 4096-byte relative path becomes longer than 4096 once resolved
	// against /home/user, so it must be rejected at issuance rather than
	// producing a token that can never be redeemed.
	tooLong := strings.Repeat("a", 4096)
	body, err := json.Marshal(map[string]string{"path": tooLong})
	if err != nil {
		t.Fatal(err)
	}
	handlers := uploadsTestHandlers(t, uploadsTestReadyLookup(), uploadsTestFactory(&uploadsTestNodeClient{}), newUploadsTestClock())
	recorder := uploadsTestIssue(t, handlers, uploadsTestOwner, uploadsTestSandboxID, string(body))
	uploadsTestTargets(t, recorder, http.StatusBadRequest, apierr.CodeInvalidPath)
	uploadsTestValidateCreateUploadURL(t, recorder)

	// A long relative input that cleans to a short canonical path is valid.
	cleanPath := "/home/user/file"
	longClean := strings.Repeat("./", 2000) + "file"
	stream := uploadsTestNewStream()
	stream.recv = func() (*nodev1.PutFileResponse, error) {
		return &nodev1.PutFileResponse{Path: cleanPath, Size: 0}, nil
	}
	cleanHandlers := uploadsTestHandlers(t, uploadsTestReadyLookup(), uploadsTestStreamFactory(stream), newUploadsTestClock())
	token := uploadsTestIssueToken(t, cleanHandlers, uploadsTestOwner, uploadsTestSandboxID, longClean)
	recorder = uploadsTestReceive(t, cleanHandlers, http.MethodPut, token, strings.NewReader(""))
	uploadsTestDecodeUploaded(t, recorder)

	// Exactly 4096 canonical bytes is the inclusive boundary.
	boundaryPath := "/home/user/" + strings.Repeat("a", 4085)
	boundaryStream := uploadsTestNewStream()
	boundaryStream.recv = func() (*nodev1.PutFileResponse, error) {
		return &nodev1.PutFileResponse{Path: boundaryPath, Size: 0}, nil
	}
	boundaryHandlers := uploadsTestHandlers(t, uploadsTestReadyLookup(), uploadsTestStreamFactory(boundaryStream), newUploadsTestClock())
	token = uploadsTestIssueToken(t, boundaryHandlers, uploadsTestOwner, uploadsTestSandboxID, boundaryPath)
	recorder = uploadsTestReceive(t, boundaryHandlers, http.MethodPut, token, strings.NewReader(""))
	uploadsTestDecodeUploaded(t, recorder)
}

// uploadsTestGRPCPutFile is one real gRPC node PutFile implementation.
type uploadsTestGRPCPutFile func(grpc.ClientStreamingServer[nodev1.PutFileRequest, nodev1.PutFileResponse]) error

type uploadsTestGRPCNode struct {
	nodev1.UnimplementedNodeServer
	putFile uploadsTestGRPCPutFile
}

func (n *uploadsTestGRPCNode) PutFile(stream grpc.ClientStreamingServer[nodev1.PutFileRequest, nodev1.PutFileResponse]) error {
	return n.putFile(stream)
}

// uploadsTestStartGRPCNode serves one real gRPC node and returns a client
// factory plus a channel closed when the wrapped RecvMsg returns.
func uploadsTestStartGRPCNode(t *testing.T, putFile uploadsTestGRPCPutFile) (api.NodeClientFactory, <-chan struct{}) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	nodev1.RegisterNodeServer(server, &uploadsTestGRPCNode{putFile: putFile})
	serveDone := make(chan struct{})
	go func() {
		_ = server.Serve(listener)
		close(serveDone)
	}()
	// LIFO cleanup: the client closes before the server stops.
	t.Cleanup(func() {
		server.Stop()
		<-serveDone
	})
	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	recvDone := make(chan struct{})
	client := nodev1.NewNodeClient(conn)
	factory := func(string) (nodev1.NodeClient, func() error, error) {
		return &uploadsTestRecvSignalClient{NodeClient: client, done: recvDone}, func() error { return nil }, nil
	}
	return factory, recvDone
}

type uploadsTestRecvSignalClient struct {
	nodev1.NodeClient
	done chan struct{}
}

func (c *uploadsTestRecvSignalClient) PutFile(ctx context.Context, opts ...grpc.CallOption) (grpc.ClientStreamingClient[nodev1.PutFileRequest, nodev1.PutFileResponse], error) {
	stream, err := c.NodeClient.PutFile(ctx, opts...)
	if err != nil {
		return nil, err
	}
	return &uploadsTestRecvSignalStream{ClientStreamingClient: stream, done: c.done}, nil
}

type uploadsTestRecvSignalStream struct {
	grpc.ClientStreamingClient[nodev1.PutFileRequest, nodev1.PutFileResponse]
	done chan struct{}
	once sync.Once
}

func (s *uploadsTestRecvSignalStream) RecvMsg(message any) error {
	err := s.ClientStreamingClient.RecvMsg(message)
	s.once.Do(func() { close(s.done) })
	return err
}

func TestUploadReceiveRealGRPCCallerCancellation(t *testing.T) {
	factory, recvDone := uploadsTestStartGRPCNode(t, func(stream grpc.ClientStreamingServer[nodev1.PutFileRequest, nodev1.PutFileResponse]) error {
		if _, err := stream.Recv(); err != nil {
			return err
		}
		<-stream.Context().Done()
		return status.FromContextError(stream.Context().Err()).Err()
	})
	handlers := uploadsTestHandlers(t, uploadsTestReadyLookup(), factory, newUploadsTestClock())
	token := uploadsTestIssueToken(t, handlers, uploadsTestOwner, uploadsTestSandboxID, "grpc-cancel.bin")
	reader := &uploadsTestHoldReader{started: make(chan struct{}), closed: make(chan struct{}), readDone: make(chan struct{})}
	ctx, cancel := context.WithCancel(t.Context())
	request := uploadsTestReceiveRequest(http.MethodPut, token, reader).WithContext(ctx)
	request.Body = reader
	recorder := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		handlers.Receive(recorder, request)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		_ = reader.Close()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Errorf("handler did not exit after cleanup")
		}
	})
	select {
	case <-reader.started:
	case <-time.After(5 * time.Second):
		t.Fatal("upload never started reading")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("caller-canceled upload did not return")
	}
	if recorder.Body.Len() != 0 {
		t.Error("caller-canceled upload wrote a response")
	}
	uploadsTestAssertClosed(t, reader.readDone, "body pump")
	uploadsTestAssertClosed(t, recvDone, "gRPC receive")
}

func TestUploadReceiveRealGRPCNodeStatusCancellation(t *testing.T) {
	for _, code := range []codes.Code{codes.Canceled, codes.DeadlineExceeded} {
		t.Run(code.String(), func(t *testing.T) {
			factory, recvDone := uploadsTestStartGRPCNode(t, func(stream grpc.ClientStreamingServer[nodev1.PutFileRequest, nodev1.PutFileResponse]) error {
				if _, err := stream.Recv(); err != nil {
					return err
				}
				return status.Error(code, "node failure")
			})
			handlers := uploadsTestHandlers(t, uploadsTestReadyLookup(), factory, newUploadsTestClock())
			token := uploadsTestIssueToken(t, handlers, uploadsTestOwner, uploadsTestSandboxID, "grpc-status.bin")
			reader := &uploadsTestHoldReader{started: make(chan struct{}), closed: make(chan struct{}), readDone: make(chan struct{})}
			request := uploadsTestReceiveRequest(http.MethodPut, token, reader)
			request.Body = reader
			recorder := httptest.NewRecorder()
			done := make(chan struct{})
			go func() {
				handlers.Receive(recorder, request)
				close(done)
			}()
			t.Cleanup(func() {
				_ = reader.Close()
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Errorf("handler did not exit after cleanup")
				}
			})
			select {
			case <-reader.started:
			case <-time.After(5 * time.Second):
				t.Fatal("upload never started reading")
			}
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("handler did not return after the node status")
			}
			uploadsTestTargets(t, recorder, http.StatusBadGateway, apierr.CodeSandboxDirectFailed)
			uploadsTestAssertClosed(t, reader.readDone, "body pump")
			uploadsTestAssertClosed(t, recvDone, "gRPC receive")
		})
	}
}

func TestUploadReceiveBodyErrorNodeStatusCancellation(t *testing.T) {
	for _, code := range []codes.Code{codes.Canceled, codes.DeadlineExceeded} {
		t.Run(code.String(), func(t *testing.T) {
			bodyFailed := make(chan struct{})
			stream := uploadsTestNewStream()
			stream.early = true
			stream.recv = func() (*nodev1.PutFileResponse, error) {
				// The body-error branch closes bodyFailed by reading, then
				// cancels the stream context. Waiting on both makes the
				// branch order deterministic without a scheduler sleep.
				<-bodyFailed
				<-stream.ctx.Done()
				return nil, status.Error(code, "node failure")
			}
			handlers := uploadsTestHandlers(t, uploadsTestReadyLookup(), uploadsTestStreamFactory(stream), newUploadsTestClock())
			token := uploadsTestIssueToken(t, handlers, uploadsTestOwner, uploadsTestSandboxID, "body-error.bin")
			reader := &uploadsTestErrorReader{err: errors.New("body failed"), done: bodyFailed}
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			request := uploadsTestReceiveRequest(http.MethodPut, token, reader).WithContext(ctx)
			request.Body = reader
			recorder := httptest.NewRecorder()
			handlers.Receive(recorder, request)
			uploadsTestTargets(t, recorder, http.StatusBadRequest, apierr.CodeInvalidJSON)
			if stream.recvCalls() == 0 {
				t.Error("gRPC receive worker did not run")
			}
		})
	}
}

func TestUploadReceiveCallerCancelAfterBodyComplete(t *testing.T) {
	recvPending := make(chan struct{})
	recvCanceled := make(chan struct{})
	release := make(chan struct{})
	recvReturned := make(chan struct{})
	var releaseOnce sync.Once
	stream := uploadsTestNewStream()
	stream.recv = func() (*nodev1.PutFileResponse, error) {
		close(recvPending)
		<-stream.ctx.Done()
		close(recvCanceled)
		<-release
		close(recvReturned)
		return nil, stream.ctx.Err()
	}
	handlers := uploadsTestHandlers(t, uploadsTestReadyLookup(), uploadsTestStreamFactory(stream), newUploadsTestClock())
	token := uploadsTestIssueToken(t, handlers, uploadsTestOwner, uploadsTestSandboxID, "complete-cancel.bin")
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	request := uploadsTestReceiveRequest(http.MethodPut, token, strings.NewReader("")).WithContext(ctx)
	recorder := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		handlers.Receive(recorder, request)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		releaseOnce.Do(func() { close(release) })
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Errorf("handler did not exit after cleanup")
		}
	})
	// The empty body reaches EOF, CloseSend lets RecvMsg enter the fake, and
	// the handler waits on the caller context with no artificial timeout.
	select {
	case <-recvPending:
	case <-time.After(5 * time.Second):
		t.Fatal("receive never became pending")
	}
	cancel()
	select {
	case <-recvCanceled:
	case <-time.After(5 * time.Second):
		t.Fatal("receive did not observe caller cancellation")
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("caller-canceled commit did not return")
	}
	if recorder.Body.Len() != 0 {
		t.Error("caller-canceled commit wrote a response")
	}
	uploadsTestAssertClosed(t, recvReturned, "gRPC receive")
}

func TestUploadReceiveSendErrorGraceJoinsPendingRecv(t *testing.T) {
	const grace = 5 * time.Second
	recvPending := make(chan struct{})
	recvReturned := make(chan struct{})
	stream := uploadsTestNewStream()
	stream.early = true
	stream.recv = func() (*nodev1.PutFileResponse, error) {
		close(recvPending)
		<-stream.ctx.Done()
		close(recvReturned)
		return nil, stream.ctx.Err()
	}
	stream.send = func(*nodev1.PutFileRequest) error {
		return status.Error(codes.NotFound, "node gone")
	}
	handlers := uploadsTestHandlers(t, uploadsTestReadyLookup(), uploadsTestStreamFactory(stream), newUploadsTestClock())
	token := uploadsTestIssueToken(t, handlers, uploadsTestOwner, uploadsTestSandboxID, "send-error.bin")
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	request := uploadsTestReceiveRequest(http.MethodPut, token, strings.NewReader("")).WithContext(ctx)
	recorder := httptest.NewRecorder()
	started := time.Now()
	handlers.Receive(recorder, request)
	elapsed := time.Since(started)
	uploadsTestTargets(t, recorder, http.StatusNotFound, apierr.CodeNotFound)
	if elapsed < grace {
		t.Errorf("send-error path did not wait the full grace (%s)", elapsed)
	}
	select {
	case <-recvPending:
	default:
		t.Error("receive was not pending during the grace")
	}
	uploadsTestAssertClosed(t, recvReturned, "gRPC receive")
}

type uploadsTestHTTPResult struct {
	status int
	err    error
}

// uploadsTestHandlerResult is what the real-HTTP handler observed.
type uploadsTestHandlerResult struct {
	closed bool
	status int
}

// uploadsTestTeeResponse forwards writes to the real ResponseWriter while
// recording the status. Unwrap lets http.ResponseController set read deadlines
// on the real connection.
type uploadsTestTeeResponse struct {
	http.ResponseWriter
	status int
}

func (t *uploadsTestTeeResponse) WriteHeader(code int) {
	if t.status == 0 {
		t.status = code
	}
	t.ResponseWriter.WriteHeader(code)
}

func (t *uploadsTestTeeResponse) Write(p []byte) (int, error) {
	if t.status == 0 {
		t.status = http.StatusOK
	}
	return t.ResponseWriter.Write(p)
}

func (t *uploadsTestTeeResponse) Unwrap() http.ResponseWriter { return t.ResponseWriter }
