package api_test

import (
	"bytes"
	"context"
	"crypto/subtle"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
	"github.com/nathabonfim59/bamboo-raft/internal/auth"
	"github.com/nathabonfim59/bamboo-raft/internal/nodefake"
	"github.com/nathabonfim59/bamboo-raft/internal/nodelink"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
	"github.com/nathabonfim59/bamboo-raft/internal/server/nodegateway"
)

const (
	fileTestNodeID    = "node-files"
	fileTestSandboxID = "bx_abcdefgh"
	fileTestOwner     = "11111111-1111-4111-8111-111111111111"
	fileTestNodeToken = "file-test-node-token"
	fileTestGateway   = "file-test-gateway-token"
)

// fileTestRPC names one node RPC the file handlers may call.
type fileTestRPC string

// The recorded file RPCs.
const (
	fileTestRead   fileTestRPC = "ReadFile"
	fileTestStat   fileTestRPC = "StatFile"
	fileTestList   fileTestRPC = "ListFiles"
	fileTestDelete fileTestRPC = "DeleteFile"
	fileTestMkdir  fileTestRPC = "MakeDir"
	fileTestMove   fileTestRPC = "MoveFile"
	fileTestChmod  fileTestRPC = "ChmodFile"
	fileTestWrite  fileTestRPC = "PutFile"
)

// fileTestNode wraps nodefake to count and record the file RPCs. Hooks replace
// the native implementation for stream-contract and failure tests.
type fileTestNode struct {
	*nodefake.Node

	mu       sync.Mutex
	calls    map[fileTestRPC]int
	readReq  *nodev1.ReadFileRequest
	statReq  *nodev1.StatFileRequest
	listReq  *nodev1.ListFilesRequest
	delReq   *nodev1.DeleteFileRequest
	mkdirReq *nodev1.MakeDirRequest
	moveReq  *nodev1.MoveFileRequest
	chmodReq *nodev1.ChmodFileRequest

	putHeader *nodev1.PutFileRequest_Header
	putChunks []int
	putData   []byte

	readHook func(*nodev1.ReadFileRequest, grpc.ServerStreamingServer[nodev1.ReadFileResponse]) error
	putHook  func(grpc.ClientStreamingServer[nodev1.PutFileRequest, nodev1.PutFileResponse]) error
	statHook func(*nodev1.StatFileRequest) (*nodev1.FileInfo, error)

	deleteHook func(*nodev1.DeleteFileRequest) (*nodev1.DeleteFileResponse, error)
	moveHook   func(*nodev1.MoveFileRequest) (*nodev1.MoveFileResponse, error)
	listHook   func(*nodev1.ListFilesRequest) (*nodev1.ListFilesResponse, error)
	mkdirHook  func(*nodev1.MakeDirRequest) (*nodev1.FileInfo, error)
	chmodHook  func(*nodev1.ChmodFileRequest) (*nodev1.FileInfo, error)
}

func newFileTestNode(id, root string) *fileTestNode {
	return &fileTestNode{Node: nodefake.New(id, root), calls: map[fileTestRPC]int{}}
}

// count returns how many times call reached the node.
func (n *fileTestNode) count(call fileTestRPC) int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.calls[call]
}

// counts returns a copy of every counter.
func (n *fileTestNode) counts() map[fileTestRPC]int {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make(map[fileTestRPC]int, len(n.calls))
	for call, count := range n.calls {
		out[call] = count
	}
	return out
}

func (n *fileTestNode) setReadHook(hook func(*nodev1.ReadFileRequest, grpc.ServerStreamingServer[nodev1.ReadFileResponse]) error) {
	n.mu.Lock()
	n.readHook = hook
	n.mu.Unlock()
}

func (n *fileTestNode) setPutHook(hook func(grpc.ClientStreamingServer[nodev1.PutFileRequest, nodev1.PutFileResponse]) error) {
	n.mu.Lock()
	n.putHook = hook
	n.mu.Unlock()
}

func (n *fileTestNode) ReadFile(req *nodev1.ReadFileRequest, stream grpc.ServerStreamingServer[nodev1.ReadFileResponse]) error {
	n.mu.Lock()
	n.calls[fileTestRead]++
	n.readReq = req
	hook := n.readHook
	n.mu.Unlock()
	if hook != nil {
		return hook(req, stream)
	}
	return n.Node.ReadFile(req, stream)
}

func (n *fileTestNode) StatFile(ctx context.Context, req *nodev1.StatFileRequest) (*nodev1.FileInfo, error) {
	n.mu.Lock()
	n.calls[fileTestStat]++
	n.statReq = req
	hook := n.statHook
	n.mu.Unlock()
	if hook != nil {
		return hook(req)
	}
	return n.Node.StatFile(ctx, req)
}

func (n *fileTestNode) DeleteFile(ctx context.Context, req *nodev1.DeleteFileRequest) (*nodev1.DeleteFileResponse, error) {
	n.mu.Lock()
	n.calls[fileTestDelete]++
	n.delReq = req
	hook := n.deleteHook
	n.mu.Unlock()
	if hook != nil {
		return hook(req)
	}
	return n.Node.DeleteFile(ctx, req)
}

func (n *fileTestNode) MakeDir(ctx context.Context, req *nodev1.MakeDirRequest) (*nodev1.FileInfo, error) {
	n.mu.Lock()
	n.calls[fileTestMkdir]++
	n.mkdirReq = req
	hook := n.mkdirHook
	n.mu.Unlock()
	if hook != nil {
		return hook(req)
	}
	return n.Node.MakeDir(ctx, req)
}

func (n *fileTestNode) MoveFile(ctx context.Context, req *nodev1.MoveFileRequest) (*nodev1.MoveFileResponse, error) {
	n.mu.Lock()
	n.calls[fileTestMove]++
	n.moveReq = req
	hook := n.moveHook
	n.mu.Unlock()
	if hook != nil {
		return hook(req)
	}
	return n.Node.MoveFile(ctx, req)
}

func (n *fileTestNode) ChmodFile(ctx context.Context, req *nodev1.ChmodFileRequest) (*nodev1.FileInfo, error) {
	n.mu.Lock()
	n.calls[fileTestChmod]++
	n.chmodReq = req
	hook := n.chmodHook
	n.mu.Unlock()
	if hook != nil {
		return hook(req)
	}
	return n.Node.ChmodFile(ctx, req)
}

func (n *fileTestNode) ListFiles(ctx context.Context, req *nodev1.ListFilesRequest) (*nodev1.ListFilesResponse, error) {
	n.mu.Lock()
	n.calls[fileTestList]++
	n.listReq = req
	hook := n.listHook
	n.mu.Unlock()
	if hook != nil {
		return hook(req)
	}
	return n.Node.ListFiles(ctx, req)
}

func (n *fileTestNode) PutFile(stream grpc.ClientStreamingServer[nodev1.PutFileRequest, nodev1.PutFileResponse]) error {
	n.mu.Lock()
	n.calls[fileTestWrite]++
	hook := n.putHook
	n.mu.Unlock()
	if hook != nil {
		return hook(stream)
	}
	return n.Node.PutFile(&fileTestPutRecorder{ClientStreamingServer: stream, node: n})
}

// fileTestPutRecorder records the header and data chunks while nodefake
// consumes the stream.
type fileTestPutRecorder struct {
	grpc.ClientStreamingServer[nodev1.PutFileRequest, nodev1.PutFileResponse]
	node *fileTestNode
}

func (r *fileTestPutRecorder) Recv() (*nodev1.PutFileRequest, error) {
	message, err := r.ClientStreamingServer.Recv()
	if message != nil {
		r.node.mu.Lock()
		if header := message.GetHeader(); header != nil {
			r.node.putHeader = header
		}
		if data := message.GetData(); data != nil {
			r.node.putChunks = append(r.node.putChunks, len(data))
			r.node.putData = append(r.node.putData, data...)
		}
		r.node.mu.Unlock()
	}
	return message, err
}

// fileTestLookup is the scripted owner-scoped target lookup.
type fileTestLookup struct {
	mu        sync.Mutex
	owner     string
	sandboxID string
	nodeID    string
	states    []api.SandboxState
	notFound  bool
	err       error
	calls     int
	lastOwner string
	lastID    string
}

func (l *fileTestLookup) TargetLookup(_ context.Context, owner, sandboxID string) (api.CommandTarget, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls++
	l.lastOwner, l.lastID = owner, sandboxID
	if l.err != nil {
		return api.CommandTarget{}, l.err
	}
	if l.notFound || owner != l.owner || sandboxID != l.sandboxID {
		return api.CommandTarget{}, api.ErrCommandTargetNotFound
	}
	state := api.StateReady
	if len(l.states) > 0 {
		state = l.states[0]
		if len(l.states) > 1 {
			l.states = l.states[1:]
		}
	}
	return api.CommandTarget{NodeID: l.nodeID, State: state}, nil
}

// fileTestLookupFunc adapts a function to api.CommandTargetLookup.
type fileTestLookupFunc func(ctx context.Context, owner, sandboxID string) (api.CommandTarget, error)

func (f fileTestLookupFunc) TargetLookup(ctx context.Context, owner, sandboxID string) (api.CommandTarget, error) {
	return f(ctx, owner, sandboxID)
}

// fileTestEnv is the full nodefake → nodelink → gateway → handler stack.
type fileTestEnv struct {
	t            *testing.T
	root         string
	node         *fileTestNode
	lookup       *fileTestLookup
	handlers     api.FilesHandlers
	factoryCalls *atomic.Int32
}

// newFileTestEnv starts one READY nodefake behind the real hub and gateway and
// returns handlers bound to it.
func newFileTestEnv(t *testing.T, opts ...func(*api.FilesOptions)) *fileTestEnv {
	t.Helper()
	root := t.TempDir()
	node := newFileTestNode(fileTestNodeID, root)
	if _, err := node.CreateVM(t.Context(), &nodev1.CreateVMRequest{
		SandboxId: fileTestSandboxID,
		Resources: &nodev1.Resources{Vcpus: 1, MemoryMib: 128},
	}); err != nil {
		t.Fatalf("create fake VM: %v", err)
	}
	hub, hubAddr := fileTestStartHub(t)
	fileTestStartNode(t, hubAddr, node)
	gatewayAddr := fileTestStartGateway(t, hub)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if _, err := hub.Wait(ctx, fileTestNodeID); err != nil {
		t.Fatalf("node link: %v", err)
	}
	base := nodegateway.NewClientFactory(gatewayAddr, fileTestGateway, nil)
	var factoryCalls atomic.Int32
	factory := func(nodeID string) (nodev1.NodeClient, func() error, error) {
		factoryCalls.Add(1)
		client, err := base.Dial(nodeID)
		if err != nil {
			return nil, nil, err
		}
		return client, client.Close, nil
	}
	lookup := &fileTestLookup{owner: fileTestOwner, sandboxID: fileTestSandboxID, nodeID: fileTestNodeID}
	options := api.FilesOptions{
		Lookup:        lookup,
		Clients:       factory,
		ReadyDeadline: 2 * time.Second,
		ReadyPoll:     2 * time.Millisecond,
	}
	for _, opt := range opts {
		opt(&options)
	}
	handlers, err := api.NewFilesHandlers(options)
	if err != nil {
		t.Fatalf("new file handlers: %v", err)
	}
	return &fileTestEnv{t: t, root: root, node: node, lookup: lookup, handlers: handlers, factoryCalls: &factoryCalls}
}

// newFileTestLight returns handlers without a node for validation, state and
// error-table tests.
func newFileTestLight(t *testing.T, lookup api.CommandTargetLookup, factory api.NodeClientFactory, opts ...func(*api.FilesOptions)) api.FilesHandlers {
	t.Helper()
	options := api.FilesOptions{
		Lookup:        lookup,
		Clients:       factory,
		ReadyDeadline: 20 * time.Millisecond,
		ReadyPoll:     time.Millisecond,
	}
	for _, opt := range opts {
		opt(&options)
	}
	handlers, err := api.NewFilesHandlers(options)
	if err != nil {
		t.Fatalf("new file handlers: %v", err)
	}
	return handlers
}

// fileTestStartHub runs a nodelink hub and returns it with its address.
func fileTestStartHub(t *testing.T) (*nodelink.Hub, string) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	authenticate := func(_ context.Context, hello nodelink.Hello) error {
		if subtle.ConstantTimeCompare([]byte(hello.Token), []byte(fileTestNodeToken)) != 1 {
			return errors.New("bad node token")
		}
		return nil
	}
	hub := nodelink.NewHub(authenticate, fileTestQuietLogger())
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { hub.Serve(ctx, listener); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	return hub, listener.Addr().String()
}

// fileTestStartNode serves node through nodelink against the hub at addr.
func fileTestStartNode(t *testing.T, addr string, node nodev1.NodeServer) {
	t.Helper()
	server := grpc.NewServer(nodelink.ServerOptions()...)
	nodev1.RegisterNodeServer(server, node)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		nodelink.Serve(ctx, nodelink.DialConfig{
			Addr: addr, NodeID: fileTestNodeID, Token: fileTestNodeToken,
			Log: fileTestQuietLogger(), MinBackoff: 20 * time.Millisecond, MaxBackoff: 100 * time.Millisecond,
		}, server)
		close(done)
	}()
	t.Cleanup(func() { cancel(); <-done })
}

// fileTestStartGateway serves the node gateway on a loopback port.
func fileTestStartGateway(t *testing.T, hub *nodelink.Hub) string {
	t.Helper()
	gateway, err := nodegateway.New(hub, fileTestGateway)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		if err := gateway.Serve(ctx, listener); err != nil {
			t.Errorf("gateway Serve: %v", err)
		}
		close(done)
	}()
	t.Cleanup(func() { cancel(); <-done })
	return listener.Addr().String()
}

func fileTestQuietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// handler returns the handler of one operation.
func (e *fileTestEnv) handler(op string) http.HandlerFunc {
	e.t.Helper()
	switch op {
	case "readFile":
		return e.handlers.ReadFile
	case "writeFile":
		return e.handlers.WriteFile
	case "deleteFile":
		return e.handlers.DeleteFile
	case "listFiles":
		return e.handlers.ListFiles
	case "getFileInfo":
		return e.handlers.GetFileInfo
	case "createFolder":
		return e.handlers.CreateFolder
	case "moveFile":
		return e.handlers.MoveFile
	case "setFilePermissions":
		return e.handlers.SetFilePermissions
	}
	e.t.Fatalf("unknown file operation %q", op)
	return nil
}

// disk maps a canonical guest path to the fake VM's directory on the host.
func (e *fileTestEnv) disk(guestPath string) string {
	return filepath.Join(e.root, "vms", fileTestSandboxID, strings.TrimPrefix(guestPath, "/"))
}

// writeDisk writes a file directly into the fake VM's disk.
func (e *fileTestEnv) writeDisk(guestPath string, data []byte) {
	e.t.Helper()
	target := e.disk(guestPath)
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		e.t.Fatalf("mkdir disk: %v", err)
	}
	if err := os.WriteFile(target, data, 0o644); err != nil {
		e.t.Fatalf("write disk: %v", err)
	}
}

// readDisk reads a file directly from the fake VM's disk.
func (e *fileTestEnv) readDisk(guestPath string) []byte {
	e.t.Helper()
	data, err := os.ReadFile(e.disk(guestPath))
	if err != nil {
		e.t.Fatalf("read disk %s: %v", guestPath, err)
	}
	return data
}

// fileTestRequest builds one handler request with the authenticated identity
// (nil for an anonymous call) and the sandbox path value ("" to omit it).
func fileTestRequest(method, target, body string, identity *auth.Identity, sandboxID string) *http.Request {
	var reader io.Reader = http.NoBody
	if body != "" {
		reader = strings.NewReader(body)
	}
	request := httptest.NewRequest(method, target, reader)
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	if identity != nil {
		request = request.WithContext(auth.WithIdentity(request.Context(), *identity))
	}
	if sandboxID != "" {
		request.SetPathValue(api.ParamSandboxID, sandboxID)
	}
	return request
}

// fileTestServe runs handler and returns its recorder.
func fileTestServe(handler http.HandlerFunc, request *http.Request) *httptest.ResponseRecorder {
	recorder := httptest.NewRecorder()
	handler(recorder, request)
	return recorder
}

// fileTestResponseWriter records whether anything was written.
type fileTestResponseWriter struct {
	header http.Header
	status int
	body   bytes.Buffer
	wrote  bool
}

func newFileTestResponseWriter() *fileTestResponseWriter {
	return &fileTestResponseWriter{header: http.Header{}}
}

func (w *fileTestResponseWriter) Header() http.Header { return w.header }

func (w *fileTestResponseWriter) WriteHeader(status int) {
	w.wrote = true
	w.status = status
}

func (w *fileTestResponseWriter) Write(data []byte) (int, error) {
	w.wrote = true
	return w.body.Write(data)
}

// fileTestJoin waits for one handler goroutine, reporting rather than hanging
// when it does not return.
func fileTestJoin(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Error("handler did not return after cancellation")
	}
}

// fileTestIdentity is the authenticated identity used by most tests.
func fileTestIdentity() *auth.Identity {
	return &auth.Identity{Owner: fileTestOwner, KeyID: "key_test"}
}

// fileTestRPCError returns a client whose named file RPC returns err.
func fileTestRPCError(call fileTestRPC, err error) api.NodeClientFactory {
	return func(string) (nodev1.NodeClient, func() error, error) {
		return &fileTestFailingClient{call: call, err: err}, func() error { return nil }, nil
	}
}

// fileTestFailingClient overrides one file RPC of an otherwise unusable
// client.
type fileTestFailingClient struct {
	nodev1.NodeClient
	call fileTestRPC
	err  error
}

func (c *fileTestFailingClient) ReadFile(context.Context, *nodev1.ReadFileRequest, ...grpc.CallOption) (grpc.ServerStreamingClient[nodev1.ReadFileResponse], error) {
	if c.call == fileTestRead {
		return nil, c.err
	}
	return nil, status.Error(codes.Unimplemented, "not used")
}

func (c *fileTestFailingClient) StatFile(context.Context, *nodev1.StatFileRequest, ...grpc.CallOption) (*nodev1.FileInfo, error) {
	if c.call == fileTestStat {
		return nil, c.err
	}
	return nil, status.Error(codes.Unimplemented, "not used")
}

func (c *fileTestFailingClient) ListFiles(context.Context, *nodev1.ListFilesRequest, ...grpc.CallOption) (*nodev1.ListFilesResponse, error) {
	if c.call == fileTestList {
		return nil, c.err
	}
	return nil, status.Error(codes.Unimplemented, "not used")
}

func (c *fileTestFailingClient) DeleteFile(context.Context, *nodev1.DeleteFileRequest, ...grpc.CallOption) (*nodev1.DeleteFileResponse, error) {
	if c.call == fileTestDelete {
		return nil, c.err
	}
	return nil, status.Error(codes.Unimplemented, "not used")
}

func (c *fileTestFailingClient) MakeDir(context.Context, *nodev1.MakeDirRequest, ...grpc.CallOption) (*nodev1.FileInfo, error) {
	if c.call == fileTestMkdir {
		return nil, c.err
	}
	return nil, status.Error(codes.Unimplemented, "not used")
}

func (c *fileTestFailingClient) MoveFile(context.Context, *nodev1.MoveFileRequest, ...grpc.CallOption) (*nodev1.MoveFileResponse, error) {
	if c.call == fileTestMove {
		return nil, c.err
	}
	return nil, status.Error(codes.Unimplemented, "not used")
}

func (c *fileTestFailingClient) ChmodFile(context.Context, *nodev1.ChmodFileRequest, ...grpc.CallOption) (*nodev1.FileInfo, error) {
	if c.call == fileTestChmod {
		return nil, c.err
	}
	return nil, status.Error(codes.Unimplemented, "not used")
}

func (c *fileTestFailingClient) PutFile(context.Context, ...grpc.CallOption) (grpc.ClientStreamingClient[nodev1.PutFileRequest, nodev1.PutFileResponse], error) {
	if c.call == fileTestWrite {
		return nil, c.err
	}
	return nil, status.Error(codes.Unimplemented, "not used")
}

// fileTestStubClient overrides any subset of the file RPCs for deterministic
// response, protocol and cancellation tests.
type fileTestStubClient struct {
	nodev1.NodeClient
	readFn    func(context.Context, *nodev1.ReadFileRequest) (grpc.ServerStreamingClient[nodev1.ReadFileResponse], error)
	statFn    func(context.Context, *nodev1.StatFileRequest) (*nodev1.FileInfo, error)
	listFn    func(context.Context, *nodev1.ListFilesRequest) (*nodev1.ListFilesResponse, error)
	deleteFn  func(context.Context, *nodev1.DeleteFileRequest) (*nodev1.DeleteFileResponse, error)
	mkdirFn   func(context.Context, *nodev1.MakeDirRequest) (*nodev1.FileInfo, error)
	moveFn    func(context.Context, *nodev1.MoveFileRequest) (*nodev1.MoveFileResponse, error)
	chmodFn   func(context.Context, *nodev1.ChmodFileRequest) (*nodev1.FileInfo, error)
	putFileFn func(context.Context) (grpc.ClientStreamingClient[nodev1.PutFileRequest, nodev1.PutFileResponse], error)
}

func (c *fileTestStubClient) ReadFile(ctx context.Context, req *nodev1.ReadFileRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[nodev1.ReadFileResponse], error) {
	if c.readFn == nil {
		return nil, status.Error(codes.Unimplemented, "not used")
	}
	return c.readFn(ctx, req)
}

func (c *fileTestStubClient) StatFile(ctx context.Context, req *nodev1.StatFileRequest, _ ...grpc.CallOption) (*nodev1.FileInfo, error) {
	if c.statFn == nil {
		return nil, status.Error(codes.Unimplemented, "not used")
	}
	return c.statFn(ctx, req)
}

func (c *fileTestStubClient) ListFiles(ctx context.Context, req *nodev1.ListFilesRequest, _ ...grpc.CallOption) (*nodev1.ListFilesResponse, error) {
	if c.listFn == nil {
		return nil, status.Error(codes.Unimplemented, "not used")
	}
	return c.listFn(ctx, req)
}

func (c *fileTestStubClient) DeleteFile(ctx context.Context, req *nodev1.DeleteFileRequest, _ ...grpc.CallOption) (*nodev1.DeleteFileResponse, error) {
	if c.deleteFn == nil {
		return nil, status.Error(codes.Unimplemented, "not used")
	}
	return c.deleteFn(ctx, req)
}

func (c *fileTestStubClient) MakeDir(ctx context.Context, req *nodev1.MakeDirRequest, _ ...grpc.CallOption) (*nodev1.FileInfo, error) {
	if c.mkdirFn == nil {
		return nil, status.Error(codes.Unimplemented, "not used")
	}
	return c.mkdirFn(ctx, req)
}

func (c *fileTestStubClient) MoveFile(ctx context.Context, req *nodev1.MoveFileRequest, _ ...grpc.CallOption) (*nodev1.MoveFileResponse, error) {
	if c.moveFn == nil {
		return nil, status.Error(codes.Unimplemented, "not used")
	}
	return c.moveFn(ctx, req)
}

func (c *fileTestStubClient) ChmodFile(ctx context.Context, req *nodev1.ChmodFileRequest, _ ...grpc.CallOption) (*nodev1.FileInfo, error) {
	if c.chmodFn == nil {
		return nil, status.Error(codes.Unimplemented, "not used")
	}
	return c.chmodFn(ctx, req)
}

func (c *fileTestStubClient) PutFile(ctx context.Context, _ ...grpc.CallOption) (grpc.ClientStreamingClient[nodev1.PutFileRequest, nodev1.PutFileResponse], error) {
	if c.putFileFn == nil {
		return nil, status.Error(codes.Unimplemented, "not used")
	}
	return c.putFileFn(ctx)
}

// fileTestStubFactory returns a factory for client with a no-op close.
func fileTestStubFactory(client nodev1.NodeClient) api.NodeClientFactory {
	return func(string) (nodev1.NodeClient, func() error, error) {
		return client, func() error { return nil }, nil
	}
}

// fileTestInfo builds one valid node file record.
func fileTestInfo(guestPath string, typ nodev1.FileType) *nodev1.FileInfo {
	mode := uint32(0o644)
	size := int64(0)
	if typ == nodev1.FileType_FILE_TYPE_DIRECTORY {
		mode = 0o755
	}
	return &nodev1.FileInfo{
		Path: guestPath, Name: path.Base(guestPath), Type: typ, Size: size, Mode: mode, ModifiedAtUnixMs: 1,
	}
}

// fileTestReadStreamClient replays fixed ReadFile messages.
type fileTestReadStreamClient struct {
	grpc.ServerStreamingClient[nodev1.ReadFileResponse]
	messages []*nodev1.ReadFileResponse
	index    int
}

func (s *fileTestReadStreamClient) Recv() (*nodev1.ReadFileResponse, error) {
	if s.index >= len(s.messages) {
		return nil, io.EOF
	}
	message := s.messages[s.index]
	s.index++
	return message, nil
}

// fileTestPutStream records sends and the final receive. failAt is the 1-based
// Send call that fails; zero never fails.
type fileTestPutStream struct {
	grpc.ClientStreamingClient[nodev1.PutFileRequest, nodev1.PutFileResponse]
	sends         int
	failAt        int
	sendErr       error
	closeCalls    int
	closeResponse *nodev1.PutFileResponse
	closeErr      error
}

func (s *fileTestPutStream) Send(*nodev1.PutFileRequest) error {
	s.sends++
	if s.failAt > 0 && s.sends == s.failAt {
		return s.sendErr
	}
	return nil
}

func (s *fileTestPutStream) CloseAndRecv() (*nodev1.PutFileResponse, error) {
	s.closeCalls++
	return s.closeResponse, s.closeErr
}
