package nodegateway

import (
	"context"
	"crypto/tls"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/nathabonfim59/bamboo-raft/internal/nodelink"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// ClientFactory dials the internal node gateway for the api and lifecycle
// roles. Clients it returns are bound to one node: every RPC carries the
// internal credential and the node's bbraft-node-id, so callers need no
// gateway knowledge beyond picking the node. The factory is the only place
// that holds the raw token — keep it out of logs.
type ClientFactory struct {
	addr  string
	token string
	creds credentials.TransportCredentials
}

// NewClientFactory returns a factory for the gateway at addr. token is the
// raw internal credential sent on every call. tlsConf nil means plaintext,
// which is only acceptable on loopback (config.Node enforces that on the
// server side); off loopback pass the TLS configuration matching the
// gateway's certificate.
func NewClientFactory(addr, token string, tlsConf *tls.Config) *ClientFactory {
	var creds credentials.TransportCredentials
	if tlsConf != nil {
		creds = credentials.NewTLS(tlsConf)
	}
	return &ClientFactory{addr: addr, token: token, creds: creds}
}

// Dial returns a client whose every RPC targets nodeID through the
// gateway. The connection is lazy: it opens on the first call, with the
// caller's context. Close releases it.
func (f *ClientFactory) Dial(nodeID string) (*Client, error) {
	creds := f.creds
	if creds == nil {
		creds = insecure.NewCredentials()
	}
	cc, err := grpc.NewClient(f.addr, grpc.WithTransportCredentials(creds),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(nodelink.MaxResponseBytes)))
	if err != nil {
		return nil, fmt.Errorf("nodegateway: client for %s: %w", f.addr, err)
	}
	return &Client{
		NodeClient: &boundClient{inner: nodev1.NewNodeClient(cc), nodeID: nodeID, token: f.token},
		cc:         cc,
	}, nil
}

// Client is one gateway connection bound to one node. Its nodev1.NodeClient
// methods forward through the gateway; Close closes the connection.
type Client struct {
	nodev1.NodeClient
	cc *grpc.ClientConn
}

// Close closes the connection to the gateway. In-flight RPCs are cut.
func (c *Client) Close() error { return c.cc.Close() }

// boundClient adds the credential and node id metadata to every call. The
// two gateway keys override any caller-set values of those keys; every
// other outgoing metadata key the caller set is preserved.
type boundClient struct {
	inner  nodev1.NodeClient
	nodeID string
	token  string
}

// bind returns ctx with this client's credential and node id on the
// outgoing metadata, keeping the caller's other keys.
func (b *boundClient) bind(ctx context.Context) context.Context {
	md, _ := metadata.FromOutgoingContext(ctx)
	md = md.Copy()
	md.Set(MetadataAuthorization, "Bearer "+b.token)
	md.Set(MetadataNodeID, b.nodeID)
	return metadata.NewOutgoingContext(ctx, md)
}

// Info forwards through the gateway with this client's binding.
func (b *boundClient) Info(ctx context.Context, in *nodev1.InfoRequest, opts ...grpc.CallOption) (*nodev1.InfoResponse, error) {
	return b.inner.Info(b.bind(ctx), in, opts...)
}

// CreateVM forwards through the gateway with this client's binding.
func (b *boundClient) CreateVM(ctx context.Context, in *nodev1.CreateVMRequest, opts ...grpc.CallOption) (*nodev1.VM, error) {
	return b.inner.CreateVM(b.bind(ctx), in, opts...)
}

// GetVM forwards through the gateway with this client's binding.
func (b *boundClient) GetVM(ctx context.Context, in *nodev1.GetVMRequest, opts ...grpc.CallOption) (*nodev1.VM, error) {
	return b.inner.GetVM(b.bind(ctx), in, opts...)
}

// ListVMs forwards through the gateway with this client's binding.
func (b *boundClient) ListVMs(ctx context.Context, in *nodev1.ListVMsRequest, opts ...grpc.CallOption) (*nodev1.ListVMsResponse, error) {
	return b.inner.ListVMs(b.bind(ctx), in, opts...)
}

// DeleteVM forwards through the gateway with this client's binding.
func (b *boundClient) DeleteVM(ctx context.Context, in *nodev1.DeleteVMRequest, opts ...grpc.CallOption) (*nodev1.DeleteVMResponse, error) {
	return b.inner.DeleteVM(b.bind(ctx), in, opts...)
}

// Exec forwards through the gateway with this client's binding.
func (b *boundClient) Exec(ctx context.Context, in *nodev1.ExecRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[nodev1.ExecEvent], error) {
	return b.inner.Exec(b.bind(ctx), in, opts...)
}

// PutFile forwards through the gateway with this client's binding.
func (b *boundClient) PutFile(ctx context.Context, opts ...grpc.CallOption) (grpc.ClientStreamingClient[nodev1.PutFileRequest, nodev1.PutFileResponse], error) {
	return b.inner.PutFile(b.bind(ctx), opts...)
}

// SaveSnapshot forwards through the gateway with this client's binding.
func (b *boundClient) SaveSnapshot(ctx context.Context, in *nodev1.SaveSnapshotRequest, opts ...grpc.CallOption) (*nodev1.Snapshot, error) {
	return b.inner.SaveSnapshot(b.bind(ctx), in, opts...)
}

// DeleteSnapshot forwards through the gateway with this client's binding.
func (b *boundClient) DeleteSnapshot(ctx context.Context, in *nodev1.DeleteSnapshotRequest, opts ...grpc.CallOption) (*nodev1.DeleteSnapshotResponse, error) {
	return b.inner.DeleteSnapshot(b.bind(ctx), in, opts...)
}

// StopVM forwards through the gateway with this client's binding.
func (b *boundClient) StopVM(ctx context.Context, in *nodev1.StopVMRequest, opts ...grpc.CallOption) (*nodev1.VM, error) {
	return b.inner.StopVM(b.bind(ctx), in, opts...)
}

// StartVM forwards through the gateway with this client's binding.
func (b *boundClient) StartVM(ctx context.Context, in *nodev1.StartVMRequest, opts ...grpc.CallOption) (*nodev1.VM, error) {
	return b.inner.StartVM(b.bind(ctx), in, opts...)
}

// StartProcess forwards through the gateway with this client's binding.
func (b *boundClient) StartProcess(ctx context.Context, in *nodev1.StartProcessRequest, opts ...grpc.CallOption) (*nodev1.Process, error) {
	return b.inner.StartProcess(b.bind(ctx), in, opts...)
}

// GetProcess forwards through the gateway with this client's binding.
func (b *boundClient) GetProcess(ctx context.Context, in *nodev1.GetProcessRequest, opts ...grpc.CallOption) (*nodev1.ProcessOutput, error) {
	return b.inner.GetProcess(b.bind(ctx), in, opts...)
}

// KillProcess forwards through the gateway with this client's binding.
func (b *boundClient) KillProcess(ctx context.Context, in *nodev1.KillProcessRequest, opts ...grpc.CallOption) (*nodev1.KillProcessResponse, error) {
	return b.inner.KillProcess(b.bind(ctx), in, opts...)
}

// StatFile forwards through the gateway with this client's binding.
func (b *boundClient) StatFile(ctx context.Context, in *nodev1.StatFileRequest, opts ...grpc.CallOption) (*nodev1.FileInfo, error) {
	return b.inner.StatFile(b.bind(ctx), in, opts...)
}

// ListFiles forwards through the gateway with this client's binding.
func (b *boundClient) ListFiles(ctx context.Context, in *nodev1.ListFilesRequest, opts ...grpc.CallOption) (*nodev1.ListFilesResponse, error) {
	return b.inner.ListFiles(b.bind(ctx), in, opts...)
}

// DeleteFile forwards through the gateway with this client's binding.
func (b *boundClient) DeleteFile(ctx context.Context, in *nodev1.DeleteFileRequest, opts ...grpc.CallOption) (*nodev1.DeleteFileResponse, error) {
	return b.inner.DeleteFile(b.bind(ctx), in, opts...)
}

// MakeDir forwards through the gateway with this client's binding.
func (b *boundClient) MakeDir(ctx context.Context, in *nodev1.MakeDirRequest, opts ...grpc.CallOption) (*nodev1.FileInfo, error) {
	return b.inner.MakeDir(b.bind(ctx), in, opts...)
}

// MoveFile forwards through the gateway with this client's binding.
func (b *boundClient) MoveFile(ctx context.Context, in *nodev1.MoveFileRequest, opts ...grpc.CallOption) (*nodev1.MoveFileResponse, error) {
	return b.inner.MoveFile(b.bind(ctx), in, opts...)
}

// ChmodFile forwards through the gateway with this client's binding.
func (b *boundClient) ChmodFile(ctx context.Context, in *nodev1.ChmodFileRequest, opts ...grpc.CallOption) (*nodev1.FileInfo, error) {
	return b.inner.ChmodFile(b.bind(ctx), in, opts...)
}

// AddSSHKey forwards through the gateway with this client's binding.
func (b *boundClient) AddSSHKey(ctx context.Context, in *nodev1.AddSSHKeyRequest, opts ...grpc.CallOption) (*nodev1.SSHAccess, error) {
	return b.inner.AddSSHKey(b.bind(ctx), in, opts...)
}

// AttachProcess forwards through the gateway with this client's binding.
func (b *boundClient) AttachProcess(ctx context.Context, in *nodev1.AttachProcessRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[nodev1.ProcessEvent], error) {
	return b.inner.AttachProcess(b.bind(ctx), in, opts...)
}

// ReadFile forwards through the gateway with this client's binding.
func (b *boundClient) ReadFile(ctx context.Context, in *nodev1.ReadFileRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[nodev1.ReadFileResponse], error) {
	return b.inner.ReadFile(b.bind(ctx), in, opts...)
}
