// Package nodegateway is the internal authenticated nodev1.Node proxy the
// nodehub role serves for the api and lifecycle roles.
//
// Every call carries two pieces of gRPC metadata: the internal gateway
// credential in authorization as "Bearer <token>", and the target node in
// bbraft-node-id (exactly one value). The gateway authenticates the digest
// of the token in constant time, resolves the node's nodelink.Hub link and
// forwards the frozen nodev1.Node RPC over it. It never dials a node, never
// holds node credentials, and keeps only the SHA-256 digest of its own
// token: the raw token reaches the client factory (ClientFactory) and
// nowhere else.
//
// Forwarding is lockstep, so cancellation travels both ways, memory stays
// bounded at one message and backpressure is preserved. The listener is
// loopback plaintext by default; off loopback the caller must supply TLS
// (config.Node enforces it; ServerCredentials loads the pair).
package nodegateway

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/nodelink"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// Metadata keys carried by every call to the gateway.
const (
	// MetadataNodeID selects the hub link to forward to. It must have
	// exactly one nonempty value: the node's registered id.
	MetadataNodeID = "bbraft-node-id"
	// MetadataAuthorization holds the internal credential, as
	// "Bearer <token>" (one value).
	MetadataAuthorization = "authorization"
)

// HubView is the hub surface nodegateway needs. *nodelink.Hub implements
// it; nodegateway never opens a connection itself.
type HubView interface {
	// Get returns the live link of a node.
	Get(nodeID string) (*nodelink.Link, bool)
	// Links returns the live links, sorted by node id.
	Links() []*nodelink.Link
}

// InventoryHub is the hub surface the inventory poller consumes: the read
// view plus change notification. Changes is required, not optional: a hub
// without it would silently fall back to interval-only polling and publish
// a late connection up to Interval late.
type InventoryHub interface {
	HubView
	// Changes returns a channel closed on the next link add, remove or
	// replacement. Subscribe before reading Links so no change is missed.
	Changes() <-chan struct{}
}

// Gateway implements nodev1.Node by forwarding every call over the hub link
// named by the bbraft-node-id metadata. Create it with New and serve it
// with Serve, which installs the authenticating interceptors.
type Gateway struct {
	nodev1.UnimplementedNodeServer

	hub    HubView
	digest [sha256.Size]byte
}

// New returns a gateway forwarding to hub's links and admitting calls
// signed with token. token must be nonempty; only its SHA-256 digest is
// retained, never the token itself.
func New(hub HubView, token string) (*Gateway, error) {
	if hub == nil {
		return nil, errors.New("nodegateway: New needs a hub")
	}
	if token == "" {
		return nil, errors.New("nodegateway: New needs a nonempty token")
	}
	return &Gateway{hub: hub, digest: sha256.Sum256([]byte(token))}, nil
}

// Serve serves the gateway on ln until ctx ends or ln fails. When ctx ends
// it stops immediately — in-flight RPCs, including long Exec streams, are
// cut — and returns nil. opts are appended after the authenticating
// interceptors, e.g. grpc.Creds(ServerCredentials(...)).
func (g *Gateway) Serve(ctx context.Context, ln net.Listener, opts ...grpc.ServerOption) error {
	srv := grpc.NewServer(append([]grpc.ServerOption{
		grpc.ChainUnaryInterceptor(g.authUnary),
		grpc.ChainStreamInterceptor(g.authStream),
	}, opts...)...)
	nodev1.RegisterNodeServer(srv, g)
	stop := context.AfterFunc(ctx, srv.Stop)
	defer stop()
	err := srv.Serve(ln)
	if ctx.Err() != nil {
		return nil
	}
	return err
}

// ServerCredentials loads a TLS credential for the gateway listener from a
// certificate/key pair (config.Node.GatewayTLSCertFile/KeyFile). ctx is
// checked before and after the file loads.
func ServerCredentials(ctx context.Context, certFile, keyFile string) (credentials.TransportCredentials, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("nodegateway: gateway TLS: %w", err)
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("nodegateway: load gateway TLS pair: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("nodegateway: gateway TLS: %w", err)
	}
	return credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{cert}}), nil
}

// authUnary admits unary calls, then hands the handler a context carrying
// the resolved link.
func (g *Gateway) authUnary(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	link, err := g.admit(ctx)
	if err != nil {
		return nil, err
	}
	return handler(context.WithValue(ctx, linkKey{}, link), req)
}

// authStream admits streaming calls the same way; the handler's stream
// context carries the resolved link.
func (g *Gateway) authStream(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	link, err := g.admit(ss.Context())
	if err != nil {
		return err
	}
	wrapped := &linkStream{ServerStream: ss, ctx: context.WithValue(ss.Context(), linkKey{}, link)}
	return handler(srv, wrapped)
}

// linkStream exposes the admitted link's context to stream handlers.
type linkStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *linkStream) Context() context.Context { return s.ctx }

// admit authenticates the call from its incoming metadata and returns the
// link named by bbraft-node-id. Errors are status errors that never echo
// the token.
func (g *Gateway) admit(ctx context.Context) (*nodelink.Link, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	token := ""
	if vals := md.Get(MetadataAuthorization); len(vals) == 1 {
		if raw, ok := strings.CutPrefix(vals[0], "Bearer "); ok {
			token = raw
		}
	}
	// Always run the same hash and compare, whatever arrived, so nothing
	// about the credential leaks through timing.
	digest := sha256.Sum256([]byte(token))
	if subtle.ConstantTimeCompare(digest[:], g.digest[:]) != 1 {
		return nil, status.Error(codes.Unauthenticated, "nodegateway: invalid or missing credential")
	}
	ids := md.Get(MetadataNodeID)
	if len(ids) != 1 || ids[0] == "" {
		return nil, status.Errorf(codes.InvalidArgument,
			"nodegateway: %s metadata must be exactly one nonempty node id", MetadataNodeID)
	}
	link, ok := g.hub.Get(ids[0])
	if !ok {
		return nil, status.Errorf(codes.Unavailable, "nodegateway: node %q is not connected", ids[0])
	}
	return link, nil
}

// linkKey keys the admitted link inside a handler context.
type linkKey struct{}

// linkOf returns the link an interceptor admitted for this call.
func linkOf(ctx context.Context) (*nodelink.Link, error) {
	link, ok := ctx.Value(linkKey{}).(*nodelink.Link)
	if !ok {
		return nil, status.Error(codes.Internal,
			"nodegateway: call reached a handler without admission; serve with Gateway.Serve")
	}
	return link, nil
}

// forward sends one unary RPC to the admitted link's client. call is an
// interface method expression such as nodev1.NodeClient.Info; the gateway
// adds no call options of its own, so the caller's context travels as is.
func (g *Gateway) forward[Req, Resp any](ctx context.Context, req Req,
	call func(nodev1.NodeClient, context.Context, Req, ...grpc.CallOption) (Resp, error),
) (Resp, error) {
	link, err := linkOf(ctx)
	if err != nil {
		var zero Resp
		return zero, err
	}
	return call(link.Client, ctx, req)
}

// Info forwards to the node.
func (g *Gateway) Info(ctx context.Context, req *nodev1.InfoRequest) (*nodev1.InfoResponse, error) {
	return g.forward(ctx, req, nodev1.NodeClient.Info)
}

// CreateVM forwards to the node and blocks with it until the VM is ready.
func (g *Gateway) CreateVM(ctx context.Context, req *nodev1.CreateVMRequest) (*nodev1.VM, error) {
	return g.forward(ctx, req, nodev1.NodeClient.CreateVM)
}

// GetVM forwards to the node.
func (g *Gateway) GetVM(ctx context.Context, req *nodev1.GetVMRequest) (*nodev1.VM, error) {
	return g.forward(ctx, req, nodev1.NodeClient.GetVM)
}

// ListVMs forwards to the node.
func (g *Gateway) ListVMs(ctx context.Context, req *nodev1.ListVMsRequest) (*nodev1.ListVMsResponse, error) {
	return g.forward(ctx, req, nodev1.NodeClient.ListVMs)
}

// DeleteVM forwards to the node.
func (g *Gateway) DeleteVM(ctx context.Context, req *nodev1.DeleteVMRequest) (*nodev1.DeleteVMResponse, error) {
	return g.forward(ctx, req, nodev1.NodeClient.DeleteVM)
}

// SaveSnapshot forwards to the node.
func (g *Gateway) SaveSnapshot(ctx context.Context, req *nodev1.SaveSnapshotRequest) (*nodev1.Snapshot, error) {
	return g.forward(ctx, req, nodev1.NodeClient.SaveSnapshot)
}

// DeleteSnapshot forwards to the node.
func (g *Gateway) DeleteSnapshot(ctx context.Context, req *nodev1.DeleteSnapshotRequest) (*nodev1.DeleteSnapshotResponse, error) {
	return g.forward(ctx, req, nodev1.NodeClient.DeleteSnapshot)
}

// Exec forwards the request and relays the node's events in received
// order. Recv and Send run in lockstep — one event buffered at a time —
// so memory stays bounded, backpressure is preserved and cancelling the
// caller's context cancels the node call (which kills the command's
// process group, per the frozen contract).
func (g *Gateway) Exec(req *nodev1.ExecRequest, stream nodev1.Node_ExecServer) error {
	return relay(req, stream, nodev1.NodeClient.Exec)
}

// relay forwards one server-streaming RPC and relays the node's messages in
// received order, in lockstep (one message buffered at a time), with Exec's
// backpressure and cancellation properties.
func relay[Req, Resp any](req Req, stream grpc.ServerStreamingServer[Resp],
	call func(nodev1.NodeClient, context.Context, Req, ...grpc.CallOption) (grpc.ServerStreamingClient[Resp], error),
) error {
	link, err := linkOf(stream.Context())
	if err != nil {
		return err
	}
	in, err := call(link.Client, stream.Context(), req)
	if err != nil {
		return err
	}
	for {
		msg, err := in.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := stream.Send(msg); err != nil {
			return err
		}
	}
}

// PutFile relays the caller's messages to the node one at a time and
// returns the node's response. As with Exec, the lockstep loop keeps memory
// bounded at one chunk, preserves backpressure and carries cancellation
// both ways.
func (g *Gateway) PutFile(stream nodev1.Node_PutFileServer) error {
	link, err := linkOf(stream.Context())
	if err != nil {
		return err
	}
	upload, err := link.Client.PutFile(stream.Context())
	if err != nil {
		return err
	}
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		if err := upload.Send(msg); err != nil {
			// An early node rejection (say INVALID_ARGUMENT on the
			// header) surfaces as io.EOF from Send; the real status
			// comes back from CloseAndRecv, and returning bare EOF
			// would lose it.
			if errors.Is(err, io.EOF) {
				if _, cerr := upload.CloseAndRecv(); cerr != nil {
					return cerr
				}
			}
			return err
		}
	}
	resp, err := upload.CloseAndRecv()
	if err != nil {
		return err
	}
	return stream.SendAndClose(resp)
}

// StopVM forwards to the node.
func (g *Gateway) StopVM(ctx context.Context, req *nodev1.StopVMRequest) (*nodev1.VM, error) {
	return g.forward(ctx, req, nodev1.NodeClient.StopVM)
}

// StartVM forwards to the node.
func (g *Gateway) StartVM(ctx context.Context, req *nodev1.StartVMRequest) (*nodev1.VM, error) {
	return g.forward(ctx, req, nodev1.NodeClient.StartVM)
}

// StartProcess forwards to the node.
func (g *Gateway) StartProcess(ctx context.Context, req *nodev1.StartProcessRequest) (*nodev1.Process, error) {
	return g.forward(ctx, req, nodev1.NodeClient.StartProcess)
}

// GetProcess forwards to the node.
func (g *Gateway) GetProcess(ctx context.Context, req *nodev1.GetProcessRequest) (*nodev1.ProcessOutput, error) {
	return g.forward(ctx, req, nodev1.NodeClient.GetProcess)
}

// KillProcess forwards to the node.
func (g *Gateway) KillProcess(ctx context.Context, req *nodev1.KillProcessRequest) (*nodev1.KillProcessResponse, error) {
	return g.forward(ctx, req, nodev1.NodeClient.KillProcess)
}

// StatFile forwards to the node.
func (g *Gateway) StatFile(ctx context.Context, req *nodev1.StatFileRequest) (*nodev1.FileInfo, error) {
	return g.forward(ctx, req, nodev1.NodeClient.StatFile)
}

// ListFiles forwards to the node.
func (g *Gateway) ListFiles(ctx context.Context, req *nodev1.ListFilesRequest) (*nodev1.ListFilesResponse, error) {
	return g.forward(ctx, req, nodev1.NodeClient.ListFiles)
}

// DeleteFile forwards to the node.
func (g *Gateway) DeleteFile(ctx context.Context, req *nodev1.DeleteFileRequest) (*nodev1.DeleteFileResponse, error) {
	return g.forward(ctx, req, nodev1.NodeClient.DeleteFile)
}

// MakeDir forwards to the node.
func (g *Gateway) MakeDir(ctx context.Context, req *nodev1.MakeDirRequest) (*nodev1.FileInfo, error) {
	return g.forward(ctx, req, nodev1.NodeClient.MakeDir)
}

// MoveFile forwards to the node.
func (g *Gateway) MoveFile(ctx context.Context, req *nodev1.MoveFileRequest) (*nodev1.MoveFileResponse, error) {
	return g.forward(ctx, req, nodev1.NodeClient.MoveFile)
}

// ChmodFile forwards to the node.
func (g *Gateway) ChmodFile(ctx context.Context, req *nodev1.ChmodFileRequest) (*nodev1.FileInfo, error) {
	return g.forward(ctx, req, nodev1.NodeClient.ChmodFile)
}

// AddSSHKey forwards to the node.
func (g *Gateway) AddSSHKey(ctx context.Context, req *nodev1.AddSSHKeyRequest) (*nodev1.SSHAccess, error) {
	return g.forward(ctx, req, nodev1.NodeClient.AddSSHKey)
}

// AttachProcess forwards the request and relays the node's messages like Exec.
func (g *Gateway) AttachProcess(req *nodev1.AttachProcessRequest, stream grpc.ServerStreamingServer[nodev1.ProcessEvent]) error {
	return relay(req, stream, nodev1.NodeClient.AttachProcess)
}

// ReadFile forwards the request and relays the node's messages like Exec.
func (g *Gateway) ReadFile(req *nodev1.ReadFileRequest, stream grpc.ServerStreamingServer[nodev1.ReadFileResponse]) error {
	return relay(req, stream, nodev1.NodeClient.ReadFile)
}
