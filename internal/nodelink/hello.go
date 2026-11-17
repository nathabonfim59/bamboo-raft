// Package nodelink carries the server ⇄ node gRPC service over connections
// that the NODE opens (nodes dial the control plane, so
// they can sit behind NAT and the server never holds node credentials).
//
// A node dials the server's node port, sends a hello (node id + token), and
// once the server accepts it, the node serves nodev1.Node on that
// connection while the server uses it as a gRPC client. If the connection
// drops, the node dials again; the server forgets the old link.
//
// Wire format of the hello exchange, before HTTP/2 starts: each side sends one
// frame of a 4-byte big-endian length followed by that many bytes of JSON.
package nodelink

import (
	"encoding/binary"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

// Hello is the node's first frame.
type Hello struct {
	NodeID  string `json:"nodeId"`
	Token   string `json:"token"`
	Version string `json:"version,omitempty"`
}

// Welcome is the server's answer to a Hello.
type Welcome struct {
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

const (
	maxFrame         = 64 << 10
	handshakeTimeout = 10 * time.Second
)

// Keepalive settings shared by both ends, so neither side's enforcement
// policy rejects the other's pings.
var (
	clientKeepalive = keepalive.ClientParameters{
		Time:                20 * time.Second,
		Timeout:             10 * time.Second,
		PermitWithoutStream: true,
	}
	serverKeepalive = keepalive.ServerParameters{
		Time:    30 * time.Second,
		Timeout: 10 * time.Second,
	}
	serverEnforcement = keepalive.EnforcementPolicy{
		MinTime:             10 * time.Second,
		PermitWithoutStream: true,
	}
)

// ServerOptions returns the grpc.Server options a node must use for the
// server it passes to Serve; extra options are appended.
func ServerOptions(extra ...grpc.ServerOption) []grpc.ServerOption {
	return append([]grpc.ServerOption{
		grpc.KeepaliveParams(serverKeepalive),
		grpc.KeepaliveEnforcementPolicy(serverEnforcement),
	}, extra...)
}

func writeFrame(w io.Writer, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if len(b) > maxFrame {
		return fmt.Errorf("nodelink: frame of %d bytes exceeds %d", len(b), maxFrame)
	}
	buf := make([]byte, 4, 4+len(b))
	binary.BigEndian.PutUint32(buf, uint32(len(b)))
	_, err = w.Write(append(buf, b...))
	return err
}

func readFrame(r io.Reader, v any) error {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return err
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if n > maxFrame {
		return fmt.Errorf("nodelink: frame of %d bytes exceeds %d", n, maxFrame)
	}
	b := make([]byte, n)
	if _, err := io.ReadFull(r, b); err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}

// trackedConn closes Done the first time the connection fails or is closed.
// gRPC reads the connection continuously, so a dead peer shows up here.
type trackedConn struct {
	net.Conn
	done chan struct{}
	once sync.Once
}

func track(c net.Conn) *trackedConn {
	return &trackedConn{Conn: c, done: make(chan struct{})}
}

func (c *trackedConn) markDone() { c.once.Do(func() { close(c.done) }) }

func (c *trackedConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if err != nil && !isTimeout(err) {
		c.markDone()
	}
	return n, err
}

func (c *trackedConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	if err != nil && !isTimeout(err) {
		c.markDone()
	}
	return n, err
}

func (c *trackedConn) Close() error {
	c.markDone()
	return c.Conn.Close()
}

func isTimeout(err error) bool {
	ne, ok := errors.AsType[net.Error](err)
	return ok && ne.Timeout()
}
