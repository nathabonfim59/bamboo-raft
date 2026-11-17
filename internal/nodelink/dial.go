package nodelink

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
)

// DialConfig is the node side of a link.
type DialConfig struct {
	Addr    string // server's node port, host:port
	NodeID  string
	Token   string
	Version string
	TLS     *tls.Config // nil: plain TCP (loopback or a private network only)

	Log        *slog.Logger
	MinBackoff time.Duration // default 500ms
	MaxBackoff time.Duration // default 15s
}

// ErrRefused wraps the server's reason for refusing the node.
var ErrRefused = errors.New("nodelink: server refused the node")

// Serve keeps the node connected to the server and serves srv on each
// connection, dialing again with backoff whenever the link drops. It returns
// when ctx ends, after stopping srv. srv must be built with ServerOptions.
func Serve(ctx context.Context, cfg DialConfig, srv *grpc.Server) error {
	if cfg.MinBackoff <= 0 {
		cfg.MinBackoff = 500 * time.Millisecond
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = 15 * time.Second
	}
	ln := newChanListener()
	served := make(chan error, 1)
	go func() { served <- srv.Serve(ln) }()
	defer func() {
		srv.Stop()
		ln.Close()
		<-served
	}()

	backoff := cfg.MinBackoff
	for {
		tc, err := dial(ctx, cfg)
		if err == nil {
			cfg.Log.Info("connected to server", "addr", cfg.Addr)
			backoff = cfg.MinBackoff
			if !ln.push(ctx, tc) {
				tc.Close()
				return nil
			}
			select {
			case <-tc.done:
				cfg.Log.Warn("link to server lost; dialing again", "addr", cfg.Addr)
			case <-ctx.Done():
				return nil
			}
		} else {
			if ctx.Err() != nil {
				return nil
			}
			cfg.Log.Warn("cannot connect to server", "addr", cfg.Addr, "err", err, "retry_in", backoff)
		}
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return nil
		}
		backoff = min(backoff*2, cfg.MaxBackoff)
	}
}

func dial(ctx context.Context, cfg DialConfig) (*trackedConn, error) {
	ctx, cancel := context.WithTimeout(ctx, handshakeTimeout)
	defer cancel()
	var c net.Conn
	var err error
	if cfg.TLS != nil {
		d := &tls.Dialer{Config: cfg.TLS}
		c, err = d.DialContext(ctx, "tcp", cfg.Addr)
	} else {
		var d net.Dialer
		c, err = d.DialContext(ctx, "tcp", cfg.Addr)
	}
	if err != nil {
		return nil, err
	}
	deadline, _ := ctx.Deadline()
	c.SetDeadline(deadline)
	if err := writeFrame(c, Hello{NodeID: cfg.NodeID, Token: cfg.Token, Version: cfg.Version}); err != nil {
		c.Close()
		return nil, fmt.Errorf("send hello: %w", err)
	}
	var w Welcome
	if err := readFrame(c, &w); err != nil {
		c.Close()
		return nil, fmt.Errorf("read welcome: %w", err)
	}
	if !w.OK {
		c.Close()
		return nil, fmt.Errorf("%w: %s", ErrRefused, w.Error)
	}
	c.SetDeadline(time.Time{})
	return track(c), nil
}

// chanListener hands connections dialed by Serve to grpc.Server.Serve.
type chanListener struct {
	conns  chan net.Conn
	closed chan struct{}
	once   sync.Once
}

func newChanListener() *chanListener {
	return &chanListener{conns: make(chan net.Conn), closed: make(chan struct{})}
}

func (l *chanListener) push(ctx context.Context, c net.Conn) bool {
	select {
	case l.conns <- c:
		return true
	case <-l.closed:
		return false
	case <-ctx.Done():
		return false
	}
}

func (l *chanListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.conns:
		return c, nil
	case <-l.closed:
		return nil, net.ErrClosed
	}
}

func (l *chanListener) Close() error {
	l.once.Do(func() { close(l.closed) })
	return nil
}

func (l *chanListener) Addr() net.Addr { return linkAddr{} }

type linkAddr struct{}

func (linkAddr) Network() string { return "nodelink" }
func (linkAddr) String() string  { return "nodelink" }
