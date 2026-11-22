// Package nbd is a client for the Network Block Device protocol over a unix
// socket, written for QEMU's built-in NBD server and qemu-nbd.
//
// The node uses it to read a disk's dirty or allocated chunks for a snapshot
// (BlockStatus over qemu:dirty-bitmap:<name> or qemu:allocation-depth, then
// Read) and to write a restored disk (Write, WriteZeroes, Flush).
//
// Only the fixed newstyle handshake is spoken, structured replies are
// required, and extended (64-bit) headers are not used. A Client has one
// request in flight at a time; parallelism is several Clients. Every reply is
// checked against the request it answers: a server that sends data outside
// the requested range, overlapping chunks, unknown meta contexts or oversized
// payloads gets an error wrapping [ErrProtocol], never a panic or an
// allocation sized by the server beyond a fixed cap.
//
// An error the server reports for a command (an NBD errno) is an [*Error] and
// leaves the Client usable. Any other failure, including a cancelled context,
// leaves the byte stream out of sync: the Client is then unusable and every
// later call returns an error wrapping [ErrBroken].
package nbd

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"time"
	"unicode/utf8"
)

// ErrProtocol is wrapped by every error caused by a server that does not
// follow the protocol.
var ErrProtocol = errors.New("NBD protocol violation")

// ErrBroken is wrapped by errors from calls on a Client whose stream is out of
// sync after an earlier transport failure, protocol violation or cancelled
// call. Such a Client can only be closed.
var ErrBroken = errors.New("NBD connection unusable")

// ErrClosed is returned by calls on a closed Client.
var ErrClosed = errors.New("nbd: client closed")

// ErrUnsupported is wrapped when the export does not allow an operation
// (writing a read-only export, or a command the server did not advertise).
var ErrUnsupported = errors.New("nbd: operation not supported by export")

// Error is an error the server reported for a command. The connection stays
// usable after one.
type Error struct {
	// Errno is the NBD error value (EIO = 5, ...).
	Errno uint32
	// Message is the server's optional human-readable message.
	Message string
}

// Name returns the errno's symbolic name (for example "EIO").
func (e *Error) Name() string { return errnoName(e.Errno) }

func (e *Error) Error() string {
	if e.Message == "" {
		return "NBD server error " + e.Name()
	}
	return fmt.Sprintf("NBD server error %s: %q", e.Name(), e.Message)
}

// OptionError is a handshake option the server refused.
type OptionError struct {
	// Option is the NBD_OPT_* value sent.
	Option uint32
	// Reply is the NBD_REP_ERR_* value received.
	Reply uint32
	// Message is the server's optional message.
	Message string
}

func (e *OptionError) Error() string {
	s := fmt.Sprintf("NBD server refused %s with %s", optionName(e.Option), repName(e.Reply))
	if e.Message != "" {
		s += fmt.Sprintf(": %q", e.Message)
	}
	return s
}

// Options configures Dial.
type Options struct {
	// Export is the export name ("" is the default export).
	Export string
	// MetaContexts are requested with NBD_OPT_SET_META_CONTEXT (for example
	// "base:allocation", "qemu:allocation-depth",
	// "qemu:dirty-bitmap:bbraft-dirty"). Dial fails unless the server grants
	// every one. None skips the option.
	MetaContexts []string
	// CheckPeer, if set, runs on the connected socket before the handshake
	// (for example to compare SO_PEERCRED with QEMU's pid). An error aborts
	// Dial.
	CheckPeer func(*net.UnixConn) error
}

// Client is a connection to one NBD export. Its methods are safe for
// concurrent use but are serialized: one request is in flight at a time.
type Client struct {
	mu   sync.Mutex
	conn net.Conn
	r    *bufio.Reader
	w    *bufio.Writer

	size       uint64
	tflags     uint16
	minBlock   uint32
	maxPayload uint32
	contexts   map[string]uint32 // granted meta context name → id
	ctxNames   map[uint32]string // id → name

	cookie  uint64
	broken  error // non-nil once the stream is out of sync
	closed  bool
	scratch []byte
}

// Dial connects to the NBD server on socketPath and negotiates opts.Export.
// ctx bounds the connection and the handshake.
func Dial(ctx context.Context, socketPath string, opts Options) (*Client, error) {
	if err := validateOptions(opts); err != nil {
		return nil, err
	}
	var d net.Dialer
	nc, err := d.DialContext(ctx, "unix", socketPath)
	if err != nil {
		return nil, fmt.Errorf("nbd: dial %s: %w", socketPath, err)
	}
	uc, ok := nc.(*net.UnixConn)
	if !ok {
		nc.Close()
		return nil, fmt.Errorf("nbd: dial %s: not a unix socket connection", socketPath)
	}
	if opts.CheckPeer != nil {
		if err := opts.CheckPeer(uc); err != nil {
			uc.Close()
			return nil, fmt.Errorf("nbd: peer check on %s: %w", socketPath, err)
		}
	}
	c := newClient(uc)
	if err := c.withContext(ctx, func() error { return c.handshake(opts) }); err != nil {
		uc.Close()
		return nil, fmt.Errorf("nbd: handshake on %s: %w", socketPath, err)
	}
	return c, nil
}

func newClient(conn net.Conn) *Client {
	return &Client{
		conn: conn,
		r:    bufio.NewReaderSize(conn, 64<<10),
		w:    bufio.NewWriterSize(conn, 64<<10),
	}
}

func validateOptions(opts Options) error {
	if err := validateName("export name", opts.Export, true); err != nil {
		return err
	}
	if len(opts.MetaContexts) > maxMetaContexts {
		return fmt.Errorf("nbd: %d meta contexts requested, at most %d", len(opts.MetaContexts), maxMetaContexts)
	}
	seen := make(map[string]bool, len(opts.MetaContexts))
	for _, name := range opts.MetaContexts {
		if err := validateName("meta context", name, false); err != nil {
			return err
		}
		if seen[name] {
			return fmt.Errorf("nbd: meta context %q requested twice", name)
		}
		seen[name] = true
	}
	return nil
}

func validateName(what, s string, emptyOK bool) error {
	switch {
	case s == "" && !emptyOK:
		return fmt.Errorf("nbd: empty %s", what)
	case len(s) > maxStringLen:
		return fmt.Errorf("nbd: %s of %d bytes, at most %d", what, len(s), maxStringLen)
	case !utf8.ValidString(s):
		return fmt.Errorf("nbd: %s is not valid UTF-8", what)
	}
	return nil
}

// Size returns the export's size in bytes.
func (c *Client) Size() uint64 { return c.size }

// ReadOnly reports whether the export refuses writes.
func (c *Client) ReadOnly() bool { return c.tflags&tflagReadOnly != 0 }

// MinBlockSize returns the export's minimum block size: offsets and lengths
// of every request must be multiples of it.
func (c *Client) MinBlockSize() uint32 { return c.minBlock }

// MaxPayload returns the largest READ or WRITE payload sent in one request;
// longer Reads and Writes are split.
func (c *Client) MaxPayload() uint32 { return c.maxPayload }

// Close sends NBD_CMD_DISC, if the Client is idle and its stream in sync, and
// closes the connection. A call in flight is aborted (it fails and the
// Client is not reused), so Close never waits on the server for more than the
// short write of NBD_CMD_DISC. Closing twice returns nil.
func (c *Client) Close() error {
	if !c.mu.TryLock() {
		// A call holds the lock and may block on the server: closing the
		// connection makes its I/O fail, and it releases the lock.
		_ = c.conn.Close()
		c.mu.Lock()
	}
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	if c.broken == nil {
		// Best effort: the server needs no reply and closes its side.
		_ = c.conn.SetDeadline(time.Now().Add(2 * time.Second))
		c.cookie++
		if err := c.writeRequest(cmdDisc, 0, c.cookie, 0, 0, nil); err == nil {
			_ = c.w.Flush()
		}
	}
	if err := c.conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		return fmt.Errorf("nbd: close: %w", err)
	}
	return nil
}

// usable reports why no request can be sent, if so. Callers hold c.mu.
func (c *Client) usable() error {
	if c.closed {
		return ErrClosed
	}
	if c.broken != nil {
		return fmt.Errorf("%w: %w", ErrBroken, c.broken)
	}
	return nil
}

// pastDeadline interrupts blocked I/O on the connection.
var pastDeadline = time.Unix(1, 0)

// withContext runs fn, which does I/O on c.conn, under ctx: the context's
// deadline is set on the connection and its cancellation interrupts blocked
// I/O. Any error but an *Error from the server marks the Client broken. The
// returned error wraps ctx.Err() when the context ended the call.
func (c *Client) withContext(ctx context.Context, fn func() error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	dl, _ := ctx.Deadline() // zero: no deadline
	if err := c.conn.SetDeadline(dl); err != nil {
		c.broken = err
		return fmt.Errorf("set deadline: %w", err)
	}
	fired := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		_ = c.conn.SetDeadline(pastDeadline)
		close(fired)
	})
	err := fn()
	if !stop() {
		<-fired // the next call sets its own deadline after this one
	}
	if err == nil {
		return nil
	}
	if _, ok := errors.AsType[*Error](err); ok {
		return err
	}
	switch {
	case ctx.Err() != nil:
		err = fmt.Errorf("%w (%w)", context.Cause(ctx), err)
	case errors.Is(err, net.ErrClosed):
		err = fmt.Errorf("%w (%w)", ErrClosed, err)
	case errors.Is(err, os.ErrDeadlineExceeded):
		err = fmt.Errorf("%w (%w)", context.DeadlineExceeded, err)
	case errors.Is(err, io.ErrUnexpectedEOF), errors.Is(err, io.EOF):
		err = fmt.Errorf("%w: connection closed by server mid-reply: %w", ErrProtocol, err)
	}
	c.broken = err
	return fmt.Errorf("%w; %w", err, ErrBroken)
}

// handshake runs the fixed newstyle negotiation and NBD_OPT_GO.
func (c *Client) handshake(opts Options) error {
	var hdr [18]byte
	if _, err := io.ReadFull(c.r, hdr[:]); err != nil {
		return fmt.Errorf("read greeting: %w", err)
	}
	if m := binary.BigEndian.Uint64(hdr[0:]); m != magicNBD {
		return fmt.Errorf("%w: bad greeting magic %#x", ErrProtocol, m)
	}
	switch m := binary.BigEndian.Uint64(hdr[8:]); m {
	case magicIHaveOpt:
	case magicOldstyle:
		return fmt.Errorf("%w: oldstyle handshake not supported", ErrProtocol)
	default:
		return fmt.Errorf("%w: bad newstyle magic %#x", ErrProtocol, m)
	}
	hsFlags := binary.BigEndian.Uint16(hdr[16:])
	if hsFlags&flagFixedNewstyle == 0 {
		return fmt.Errorf("%w: server does not speak fixed newstyle", ErrProtocol)
	}
	cflags := flagCFixedNewstyle
	if hsFlags&flagNoZeroes != 0 {
		cflags |= flagCNoZeroes
	}
	// Buffered: sendOption flushes it with the first option.
	if _, err := c.w.Write(binary.BigEndian.AppendUint32(nil, cflags)); err != nil {
		return fmt.Errorf("send client flags: %w", err)
	}

	if err := c.negotiateStructured(); err != nil {
		return err
	}
	if len(opts.MetaContexts) > 0 {
		if err := c.negotiateMetaContexts(opts.Export, opts.MetaContexts); err != nil {
			return err
		}
	}
	return c.negotiateGo(opts.Export)
}

func (c *Client) sendOption(opt uint32, data []byte) error {
	b := make([]byte, 0, 16+len(data))
	b = binary.BigEndian.AppendUint64(b, magicIHaveOpt)
	b = binary.BigEndian.AppendUint32(b, opt)
	b = binary.BigEndian.AppendUint32(b, uint32(len(data)))
	b = append(b, data...)
	if _, err := c.w.Write(b); err != nil {
		return fmt.Errorf("send %s: %w", optionName(opt), err)
	}
	if err := c.w.Flush(); err != nil {
		return fmt.Errorf("send %s: %w", optionName(opt), err)
	}
	return nil
}

// readOptionReply reads one reply to opt. A server error reply is returned
// as an *OptionError (wrapped), with typ set.
func (c *Client) readOptionReply(opt uint32) (typ uint32, data []byte, err error) {
	var hdr [20]byte
	if _, err := io.ReadFull(c.r, hdr[:]); err != nil {
		return 0, nil, fmt.Errorf("read %s reply: %w", optionName(opt), err)
	}
	if m := binary.BigEndian.Uint64(hdr[0:]); m != magicOptReply {
		return 0, nil, fmt.Errorf("%w: bad option reply magic %#x", ErrProtocol, m)
	}
	if got := binary.BigEndian.Uint32(hdr[8:]); got != opt {
		return 0, nil, fmt.Errorf("%w: reply for %s while waiting for %s", ErrProtocol, optionName(got), optionName(opt))
	}
	typ = binary.BigEndian.Uint32(hdr[12:])
	n := binary.BigEndian.Uint32(hdr[16:])
	if n > maxOptionReply {
		return 0, nil, fmt.Errorf("%w: %s reply of %d bytes, at most %d", ErrProtocol, optionName(opt), n, maxOptionReply)
	}
	data = make([]byte, n)
	if _, err := io.ReadFull(c.r, data); err != nil {
		return 0, nil, fmt.Errorf("read %s reply: %w", optionName(opt), err)
	}
	if typ&repErrBit != 0 {
		return typ, nil, &OptionError{Option: opt, Reply: typ, Message: string(data)}
	}
	return typ, data, nil
}

func (c *Client) negotiateStructured() error {
	if err := c.sendOption(optStructuredReply, nil); err != nil {
		return err
	}
	typ, data, err := c.readOptionReply(optStructuredReply)
	if err != nil {
		return fmt.Errorf("structured replies are required: %w", err)
	}
	if typ != repAck || len(data) != 0 {
		return fmt.Errorf("%w: %s answered with %s of %d bytes", ErrProtocol, optionName(optStructuredReply), repName(typ), len(data))
	}
	return nil
}

func appendString32(b []byte, s string) []byte {
	b = binary.BigEndian.AppendUint32(b, uint32(len(s)))
	return append(b, s...)
}

func (c *Client) negotiateMetaContexts(export string, names []string) error {
	b := appendString32(nil, export)
	b = binary.BigEndian.AppendUint32(b, uint32(len(names)))
	wanted := make(map[string]bool, len(names))
	for _, n := range names {
		b = appendString32(b, n)
		wanted[n] = true
	}
	if err := c.sendOption(optSetMetaContext, b); err != nil {
		return err
	}
	c.contexts = make(map[string]uint32, len(names))
	c.ctxNames = make(map[uint32]string, len(names))
	for range maxOptionReplies {
		typ, data, err := c.readOptionReply(optSetMetaContext)
		if err != nil {
			return err
		}
		switch typ {
		case repAck:
			if len(data) != 0 {
				return fmt.Errorf("%w: %s ack with %d bytes", ErrProtocol, optionName(optSetMetaContext), len(data))
			}
			for _, n := range names {
				if _, ok := c.contexts[n]; !ok {
					return fmt.Errorf("meta context %q not granted by the server", n)
				}
			}
			return nil
		case repMetaContext:
			if len(data) < 5 || len(data) > 4+maxStringLen {
				return fmt.Errorf("%w: meta context reply of %d bytes", ErrProtocol, len(data))
			}
			id := binary.BigEndian.Uint32(data)
			name := string(data[4:])
			if !wanted[name] {
				return fmt.Errorf("%w: server granted meta context %q that was not requested", ErrProtocol, name)
			}
			if _, dup := c.contexts[name]; dup {
				return fmt.Errorf("%w: meta context %q granted twice", ErrProtocol, name)
			}
			if prev, dup := c.ctxNames[id]; dup {
				return fmt.Errorf("%w: meta context id %d given to both %q and %q", ErrProtocol, id, prev, name)
			}
			c.contexts[name] = id
			c.ctxNames[id] = name
		default:
			return fmt.Errorf("%w: unexpected %s to %s", ErrProtocol, repName(typ), optionName(optSetMetaContext))
		}
	}
	return fmt.Errorf("%w: more than %d replies to %s", ErrProtocol, maxOptionReplies, optionName(optSetMetaContext))
}

func (c *Client) negotiateGo(export string) error {
	b := appendString32(nil, export)
	b = binary.BigEndian.AppendUint16(b, 1)
	b = binary.BigEndian.AppendUint16(b, infoBlockSize)
	if err := c.sendOption(optGo, b); err != nil {
		return err
	}
	var (
		haveExport bool
		minB, maxB uint32 = 1, maxPayloadCap
	)
	for range maxOptionReplies {
		typ, data, err := c.readOptionReply(optGo)
		if err != nil {
			return err
		}
		switch typ {
		case repAck:
			if len(data) != 0 {
				return fmt.Errorf("%w: %s ack with %d bytes", ErrProtocol, optionName(optGo), len(data))
			}
			if !haveExport {
				return fmt.Errorf("%w: %s acked without NBD_INFO_EXPORT", ErrProtocol, optionName(optGo))
			}
			return c.setGeometry(minB, maxB)
		case repInfo:
			if len(data) < 2 {
				return fmt.Errorf("%w: info reply of %d bytes", ErrProtocol, len(data))
			}
			switch binary.BigEndian.Uint16(data) {
			case infoExport:
				if len(data) != 12 {
					return fmt.Errorf("%w: NBD_INFO_EXPORT of %d bytes", ErrProtocol, len(data))
				}
				c.size = binary.BigEndian.Uint64(data[2:])
				c.tflags = binary.BigEndian.Uint16(data[10:])
				if c.tflags&tflagHasFlags == 0 {
					c.tflags = 0
				}
				haveExport = true
			case infoBlockSize:
				if len(data) != 14 {
					return fmt.Errorf("%w: NBD_INFO_BLOCK_SIZE of %d bytes", ErrProtocol, len(data))
				}
				minB = binary.BigEndian.Uint32(data[2:])
				pref := binary.BigEndian.Uint32(data[6:])
				maxB = binary.BigEndian.Uint32(data[10:])
				if !isPow2(pref) || pref < minB {
					return fmt.Errorf("%w: preferred block size %d (minimum %d)", ErrProtocol, pref, minB)
				}
			default:
				// Other information (name, description) is not needed.
			}
		default:
			return fmt.Errorf("%w: unexpected %s to %s", ErrProtocol, repName(typ), optionName(optGo))
		}
	}
	return fmt.Errorf("%w: more than %d replies to %s", ErrProtocol, maxOptionReplies, optionName(optGo))
}

func isPow2(v uint32) bool { return v != 0 && v&(v-1) == 0 }

// setGeometry checks the export size and block sizes and derives the
// largest payload sent per request.
func (c *Client) setGeometry(minB, maxB uint32) error {
	if !isPow2(minB) || minB > maxMinBlock {
		return fmt.Errorf("%w: minimum block size %d", ErrProtocol, minB)
	}
	if maxB < minB || (maxB%minB != 0 && maxB != 0xffffffff) {
		return fmt.Errorf("%w: maximum block size %d (minimum %d)", ErrProtocol, maxB, minB)
	}
	if c.size == 0 {
		return fmt.Errorf("%w: export size 0", ErrProtocol)
	}
	if c.size%uint64(minB) != 0 {
		return fmt.Errorf("%w: export size %d not a multiple of the minimum block size %d", ErrProtocol, c.size, minB)
	}
	if c.size > 1<<63 {
		return fmt.Errorf("%w: export size %d too large", ErrProtocol, c.size)
	}
	c.minBlock = minB
	c.maxPayload = min(maxB, maxPayloadCap) &^ (minB - 1)
	return nil
}
