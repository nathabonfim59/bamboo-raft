package qmp

import (
	"bufio"
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

const (
	// MaxEventQueue is the number of undelivered asynchronous events a
	// Client buffers. Reaching it is a protocol/discipline failure: the
	// connection is closed instead of silently dropping an event.
	MaxEventQueue = 256

	// MaxInboundFrame bounds one inbound JSON message. QEMU replies to
	// query-block and similar commands can be large, but never unbounded.
	MaxInboundFrame = 8 << 20

	writeQueue = 64
)

var (
	// ErrClosed reports that the QMP connection is gone. Errors returned by
	// Execute after a failure wrap both ErrClosed and the underlying cause.
	ErrClosed = errors.New("qmp: connection closed")

	// ErrEventOverflow reports that the Events consumer fell MaxEventQueue
	// events behind. The connection is closed to avoid losing an event.
	ErrEventOverflow = errors.New("qmp: event queue overflow")

	// ErrFrameTooLarge reports an inbound message above MaxInboundFrame.
	ErrFrameTooLarge = errors.New("qmp: inbound frame too large")

	errClosedByCaller = errors.New("closed by caller")
)

// Version is a QEMU version triple.
type Version struct {
	Major   int    `json:"major"`
	Minor   int    `json:"minor"`
	Micro   int    `json:"micro"`
	Package string `json:"package,omitzero"`
}

// String formats the version as "major.minor.micro".
func (v Version) String() string {
	return fmt.Sprintf("%d.%d.%d", v.Major, v.Minor, v.Micro)
}

// Greeting is the first message QEMU sends on a QMP connection.
type Greeting struct {
	Version      Version
	Capabilities []string
}

// Error is a QMP error reply. Class is QEMU's error class, for example
// "GenericError" or "CommandNotFound".
type Error struct {
	Class       string `json:"class"`
	Description string `json:"desc"`
}

// Error implements error.
func (e *Error) Error() string {
	return fmt.Sprintf("qmp: %s: %s", e.Class, e.Description)
}

// Event is one unsolicited QMP event, for example STOP or RESET.
type Event struct {
	Name string
	Data jsontext.Value
}

// Decode unmarshals the event's data into v. Events without data are a no-op.
func (e Event) Decode(v any) error {
	if len(e.Data) == 0 {
		return nil
	}
	if err := json.Unmarshal(e.Data, v); err != nil {
		return fmt.Errorf("qmp: decode %s event: %w", e.Name, err)
	}
	return nil
}

type options struct {
	capabilities []string
	logger       *slog.Logger
}

// Option configures Dial.
type Option func(*options)

// WithCapabilities enables the named QMP capabilities during the handshake.
// Each must be advertised in QEMU's greeting, otherwise Dial fails.
func WithCapabilities(caps ...string) Option {
	return func(o *options) {
		o.capabilities = append(o.capabilities, caps...)
	}
}

// Client is one QMP connection. It is safe for concurrent use.
type Client struct {
	conn     net.Conn
	reader   *bufio.Reader
	socket   string
	greeting Greeting
	log      *slog.Logger

	writeCh chan writeRequest

	mu       sync.Mutex
	nextID   uint64
	pending  map[uint64]chan reply
	closed   bool
	closeErr error

	events  *eventQueue
	eventCh chan Event
	done    chan struct{}
}

type request struct {
	Execute   string         `json:"execute"`
	Arguments jsontext.Value `json:"arguments,omitzero"`
	ID        uint64         `json:"id"`
}

type reply struct {
	result jsontext.Value
	err    error
}

type writeRequest struct {
	data []byte
	ctx  context.Context
	// id and files are set only by ExecuteWithFiles: the writer attaches the
	// files' descriptors to the frame with SCM_RIGHTS, and reports a failure
	// that happened before any byte was written to the request id.
	id    uint64
	files []*os.File
}

type message struct {
	Event  string         `json:"event"`
	Data   jsontext.Value `json:"data"`
	ID     *uint64        `json:"id"`
	Return jsontext.Value `json:"return"`
	Error  *Error         `json:"error"`
}

type greetingMessage struct {
	QMP *struct {
		Version struct {
			QEMU    Version `json:"qemu"`
			Package string  `json:"package"`
		} `json:"version"`
		Capabilities []string `json:"capabilities"`
	} `json:"QMP"`
}

// Dial connects to the QMP unix socket at path, reads the greeting,
// negotiates capabilities and returns a ready client.
//
// The handshake, including the greeting and qmp_capabilities, is bounded by
// ctx. A successful call starts the connection's reader, writer and event
// pump; later operations use their own contexts.
func Dial(ctx context.Context, path string, log *slog.Logger, opts ...Option) (*Client, error) {
	o := options{logger: log}
	if o.logger == nil {
		o.logger = slog.New(slog.DiscardHandler)
	}
	for _, opt := range opts {
		opt(&o)
	}

	d := net.Dialer{}
	conn, err := d.DialContext(ctx, "unix", path)
	if err != nil {
		return nil, fmt.Errorf("qmp: dial %s: %w", path, err)
	}

	c := &Client{
		conn:    conn,
		reader:  bufio.NewReaderSize(conn, 64<<10),
		socket:  path,
		log:     o.logger,
		writeCh: make(chan writeRequest, writeQueue),
		pending: make(map[uint64]chan reply),
		done:    make(chan struct{}),
	}

	greeting, err := c.readGreeting(ctx)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("qmp: greet %s: %w", path, err)
	}
	if err := checkCapabilities(greeting, o.capabilities); err != nil {
		conn.Close()
		return nil, err
	}
	c.greeting = greeting

	c.events = newEventQueue(MaxEventQueue)
	c.eventCh = make(chan Event)
	go c.events.run(c.eventCh)
	go c.readLoop()
	go c.writeLoop()

	var args any
	if len(o.capabilities) > 0 {
		args = struct {
			Enable []string `json:"enable"`
		}{Enable: o.capabilities}
	}
	if err := c.Execute(ctx, "qmp_capabilities", args, nil); err != nil {
		c.fail(err)
		return nil, fmt.Errorf("qmp: negotiate capabilities on %s: %w", path, err)
	}
	c.log.Debug("qmp: connected", "socket", path, "qemu", greeting.Version.String(), "capabilities", len(o.capabilities))
	return c, nil
}

func checkCapabilities(g Greeting, want []string) error {
	for _, w := range want {
		found := false
		for _, have := range g.Capabilities {
			if have == w {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("qmp: capability %q not advertised in greeting %v", w, g.Capabilities)
		}
	}
	return nil
}

func (c *Client) readGreeting(ctx context.Context) (Greeting, error) {
	stop := context.AfterFunc(ctx, func() {
		c.conn.SetReadDeadline(time.Now())
	})
	frame, err := c.readFrame()
	stopped := stop()
	c.conn.SetReadDeadline(time.Time{})
	if !stopped {
		// The callback ran and set the deadline; the reset above is enough.
		return Greeting{}, ctx.Err()
	}
	if err != nil {
		return Greeting{}, err
	}

	var gm greetingMessage
	if err := json.Unmarshal(frame, &gm); err != nil {
		return Greeting{}, fmt.Errorf("qmp: decode greeting: %w", err)
	}
	if gm.QMP == nil {
		return Greeting{}, errors.New("qmp: first message is not a greeting")
	}
	return Greeting{
		Version: Version{
			Major:   gm.QMP.Version.QEMU.Major,
			Minor:   gm.QMP.Version.QEMU.Minor,
			Micro:   gm.QMP.Version.QEMU.Micro,
			Package: gm.QMP.Version.Package,
		},
		Capabilities: gm.QMP.Capabilities,
	}, nil
}

// Greeting returns the greeting QEMU sent at the start of the connection.
func (c *Client) Greeting() Greeting { return c.greeting }

// Execute runs one QMP command. args and result may be nil. QMP error replies
// are returned as *Error and leave the connection usable.
func (c *Client) Execute(ctx context.Context, command string, args, result any) error {
	return c.execute(ctx, command, args, result, nil)
}

// ExecuteWithFiles runs one QMP command and passes files to QEMU with
// SCM_RIGHTS on the same write as the command's first byte, which is how
// QEMU's add-fd and getfd receive descriptors. args and result may be nil;
// at least one file is required. Frames keep Execute's ordering: each
// command is written whole by the single writer, so the descriptors arrive
// with exactly this command.
//
// The caller owns files: the client never closes them, and the caller may
// close them once ExecuteWithFiles returned. Each descriptor is pinned only
// while the kernel copies it into the message, so a file closed concurrently
// is reported as an error and never replaced by a recycled descriptor
// number. A file that cannot be resolved before the write fails the call
// without writing anything and leaves the connection usable. The connection
// must be a unix socket.
func (c *Client) ExecuteWithFiles(ctx context.Context, command string, args, result any, files ...*os.File) error {
	if _, ok := c.conn.(*net.UnixConn); !ok {
		return fmt.Errorf("qmp: %s: passing files needs a unix socket connection", command)
	}
	if len(files) == 0 {
		return fmt.Errorf("qmp: %s: no files to pass", command)
	}
	if len(files) > maxPassedFiles {
		return fmt.Errorf("qmp: %s: %d files exceed the limit of %d per command", command, len(files), maxPassedFiles)
	}
	for i, f := range files {
		if f == nil {
			return fmt.Errorf("qmp: %s: file %d is nil", command, i)
		}
	}
	// Fail fast on a file that is already closed; the writer resolves the
	// descriptors again under the files' own references.
	if err := withRawFDs(files, nil, func([]int) error { return nil }); err != nil {
		return fmt.Errorf("qmp: %s: %w", command, err)
	}
	return c.execute(ctx, command, args, result, files)
}

// maxPassedFiles bounds the descriptors of one command. QEMU's socket
// chardev accepts at most 16 descriptors per message (TCP_MAX_FDS).
const maxPassedFiles = 16

func (c *Client) execute(ctx context.Context, command string, args, result any, files []*os.File) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	var raw jsontext.Value
	if args != nil {
		b, err := json.Marshal(args)
		if err != nil {
			return fmt.Errorf("qmp: marshal %s arguments: %w", command, err)
		}
		raw = jsontext.Value(b)
	}

	c.mu.Lock()
	if c.closed {
		err := c.closeErr
		c.mu.Unlock()
		return err
	}
	c.nextID++
	id := c.nextID
	ch := make(chan reply, 1)
	c.pending[id] = ch
	c.mu.Unlock()

	data, err := json.Marshal(request{Execute: command, Arguments: raw, ID: id})
	if err != nil {
		c.remove(id)
		return fmt.Errorf("qmp: marshal %s request: %w", command, err)
	}
	data = append(data, '\n')

	select {
	case c.writeCh <- writeRequest{data: data, ctx: ctx, id: id, files: files}:
	case <-ctx.Done():
		c.remove(id)
		return ctx.Err()
	case <-c.done:
		c.remove(id)
		return c.Err()
	}

	select {
	case r := <-ch:
		if r.err != nil {
			return r.err
		}
		if result != nil && len(r.result) > 0 {
			if err := json.Unmarshal(r.result, result); err != nil {
				return fmt.Errorf("qmp: %s: decode result: %w", command, err)
			}
		}
		return nil
	case <-ctx.Done():
		c.remove(id)
		return ctx.Err()
	}
}

// Events returns the channel of unsolicited events. It must have exactly one
// consumer. The channel is closed when the connection fails or is closed.
func (c *Client) Events() <-chan Event { return c.eventCh }

// Done is closed when the connection fails or is closed.
func (c *Client) Done() <-chan struct{} { return c.done }

// Err is nil while the connection is healthy, and the terminal error after
// Done is closed.
func (c *Client) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closeErr
}

// Close closes the connection and fails every pending Execute with ErrClosed.
func (c *Client) Close() error {
	c.fail(errClosedByCaller)
	return nil
}

// PeerPID returns the PID of the process at the other end of the unix socket
// (SO_PEERCRED). Callers use it to prove the QMP server is the process they
// started, not an unrelated QEMU that happens to own the path.
func (c *Client) PeerPID() (int, error) {
	uc, ok := c.conn.(*net.UnixConn)
	if !ok {
		return 0, errors.New("qmp: connection is not a unix socket")
	}
	raw, err := uc.SyscallConn()
	if err != nil {
		return 0, fmt.Errorf("qmp: peer credential: %w", err)
	}
	var cred *syscall.Ucred
	var credErr error
	if err := raw.Control(func(fd uintptr) {
		cred, credErr = syscall.GetsockoptUcred(int(fd), syscall.SOL_SOCKET, syscall.SO_PEERCRED)
	}); err != nil {
		return 0, fmt.Errorf("qmp: peer credential: %w", err)
	}
	if credErr != nil {
		return 0, fmt.Errorf("qmp: peer credential: %w", credErr)
	}
	return int(cred.Pid), nil
}

func (c *Client) remove(id uint64) {
	c.mu.Lock()
	delete(c.pending, id)
	c.mu.Unlock()
}

func (c *Client) readLoop() {
	for {
		frame, err := c.readFrame()
		if err != nil {
			c.fail(fmt.Errorf("qmp: read: %w", err))
			return
		}
		if err := c.dispatch(frame); err != nil {
			c.fail(err)
			return
		}
	}
}

func (c *Client) readFrame() (jsontext.Value, error) {
	var buf []byte
	for {
		frag, err := c.reader.ReadSlice('\n')
		if len(buf)+len(frag) > MaxInboundFrame {
			return nil, ErrFrameTooLarge
		}
		buf = append(buf, frag...)
		switch {
		case err == nil:
			return jsontext.Value(buf), nil
		case errors.Is(err, bufio.ErrBufferFull):
			continue
		default:
			return nil, err
		}
	}
}

func (c *Client) dispatch(frame jsontext.Value) error {
	var m message
	if err := json.Unmarshal(frame, &m); err != nil {
		return fmt.Errorf("qmp: decode message: %w", err)
	}
	switch {
	case m.Event != "":
		ev := Event{Name: m.Event, Data: m.Data}
		if err := c.events.push(ev); err != nil {
			return fmt.Errorf("%w: %s event", err, m.Event)
		}
		return nil
	case m.ID != nil:
		c.deliver(*m.ID, m)
		return nil
	default:
		return errors.New("qmp: message is neither a reply nor an event")
	}
}

func (c *Client) deliver(id uint64, m message) {
	c.mu.Lock()
	ch := c.pending[id]
	delete(c.pending, id)
	c.mu.Unlock()
	if ch == nil {
		c.log.Debug("qmp: reply for cancelled or unknown request", "socket", c.socket, "id", id)
		return
	}
	if m.Error != nil {
		ch <- reply{err: m.Error}
		return
	}
	ch <- reply{result: m.Return}
}

func (c *Client) writeLoop() {
	for {
		select {
		case <-c.done:
			return
		case wr := <-c.writeCh:
			if err := wr.ctx.Err(); err != nil {
				continue
			}
			if len(wr.files) > 0 {
				written, err := c.writeFilesWithContext(wr)
				if err != nil && !written {
					// Nothing reached the socket: only this request fails.
					c.deliverErr(wr.id, err)
					continue
				}
				if err != nil {
					c.fail(err)
					return
				}
				continue
			}
			if err := c.writeWithContext(wr); err != nil {
				c.fail(err)
				return
			}
		}
	}
}

// writeWithContext writes one frame and interrupts the write if ctx ends. A
// partial write cannot be resumed, so any write error poisons the connection.
func (c *Client) writeWithContext(wr writeRequest) error {
	interrupted := make(chan struct{})
	stop := context.AfterFunc(wr.ctx, func() {
		c.conn.SetWriteDeadline(time.Now())
		close(interrupted)
	})
	_, err := c.conn.Write(wr.data)
	if !stop() {
		<-interrupted
	}
	c.conn.SetWriteDeadline(time.Time{})
	if err != nil {
		return fmt.Errorf("qmp: write: %w", err)
	}
	return nil
}

// writeFilesWithContext writes one frame with wr.files attached as
// SCM_RIGHTS to its first byte, interruptible by wr.ctx like
// writeWithContext. written reports whether any byte may have reached the
// socket: an error with written false left the stream untouched.
func (c *Client) writeFilesWithContext(wr writeRequest) (written bool, err error) {
	uc, ok := c.conn.(*net.UnixConn)
	if !ok {
		return false, errors.New("qmp: passing files needs a unix socket connection")
	}
	interrupted := make(chan struct{})
	stop := context.AfterFunc(wr.ctx, func() {
		c.conn.SetWriteDeadline(time.Now())
		close(interrupted)
	})
	defer func() {
		if !stop() {
			<-interrupted
		}
		c.conn.SetWriteDeadline(time.Time{})
	}()
	var n int
	resolveErr := withRawFDs(wr.files, nil, func(fds []int) error {
		// From here on a failure may have left part of the frame in the
		// stream, so it poisons the connection like any write error.
		written = true
		var werr error
		n, _, werr = uc.WriteMsgUnix(wr.data, unix.UnixRights(fds...), nil)
		return werr
	})
	if resolveErr != nil && !written {
		return false, fmt.Errorf("qmp: pass files: %w", resolveErr)
	}
	if resolveErr != nil {
		return true, fmt.Errorf("qmp: write: %w", resolveErr)
	}
	// A stream socket may accept only part of the frame; the descriptors
	// travelled with the first part, the rest is plain data.
	if n < len(wr.data) {
		if _, err := c.conn.Write(wr.data[n:]); err != nil {
			return true, fmt.Errorf("qmp: write: %w", err)
		}
	}
	return true, nil
}

// withRawFDs calls fn with the raw descriptors of files while every file's
// descriptor is pinned by its own reference (os.File's RawConn.Control), so
// no descriptor number can be closed and recycled while fn runs.
func withRawFDs(files []*os.File, fds []int, fn func([]int) error) error {
	if len(files) == 0 {
		return fn(fds)
	}
	rc, err := files[0].SyscallConn()
	if err != nil {
		return fmt.Errorf("file %s: %w", files[0].Name(), err)
	}
	var inner error
	if err := rc.Control(func(fd uintptr) {
		inner = withRawFDs(files[1:], append(fds, int(fd)), fn)
	}); err != nil {
		// Control fails only when the file can no longer be referenced:
		// it was closed. Report it as os.ErrClosed, like os.File does.
		return fmt.Errorf("file %s: %w (%w)", files[0].Name(), os.ErrClosed, err)
	}
	return inner
}

// deliverErr fails one pending request without touching the connection.
func (c *Client) deliverErr(id uint64, err error) {
	c.mu.Lock()
	ch := c.pending[id]
	delete(c.pending, id)
	c.mu.Unlock()
	if ch != nil {
		ch <- reply{err: err}
	}
}

func (c *Client) fail(cause error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	if errors.Is(cause, errClosedByCaller) {
		c.closeErr = ErrClosed
	} else {
		c.closeErr = fmt.Errorf("%w: %w", ErrClosed, cause)
	}
	pending := c.pending
	c.pending = nil
	logErr := c.closeErr
	c.mu.Unlock()

	c.conn.Close()
	for _, ch := range pending {
		select {
		case ch <- reply{err: c.closeErr}:
		default:
		}
	}
	c.events.stopQueue()
	close(c.done)

	if !errors.Is(cause, errClosedByCaller) {
		c.log.Warn("qmp: connection lost", "socket", c.socket, "error", logErr)
	}
}

// eventQueue is a bounded FIFO with a single pump goroutine. It never blocks
// the reader: push fails on overflow, which the client treats as terminal.
type eventQueue struct {
	mu      sync.Mutex
	items   []Event
	limit   int
	stopped bool
	wake    chan struct{}
	stop    chan struct{}
	once    sync.Once
}

func newEventQueue(limit int) *eventQueue {
	return &eventQueue{
		limit: limit,
		wake:  make(chan struct{}, 1),
		stop:  make(chan struct{}),
	}
}

func (q *eventQueue) push(ev Event) error {
	q.mu.Lock()
	if q.stopped {
		q.mu.Unlock()
		return ErrClosed
	}
	if len(q.items) >= q.limit {
		q.mu.Unlock()
		return ErrEventOverflow
	}
	q.items = append(q.items, ev)
	q.mu.Unlock()
	select {
	case q.wake <- struct{}{}:
	default:
	}
	return nil
}

func (q *eventQueue) stopQueue() {
	q.once.Do(func() {
		q.mu.Lock()
		q.stopped = true
		q.items = nil
		q.mu.Unlock()
		close(q.stop)
	})
}

// run delivers events to out until the queue is stopped. It exits even when
// the consumer never reads out: the blocked send selects on stop.
func (q *eventQueue) run(out chan<- Event) {
	defer close(out)
	for {
		q.mu.Lock()
		if len(q.items) > 0 {
			ev := q.items[0]
			q.items[0] = Event{}
			q.items = q.items[1:]
			q.mu.Unlock()
			select {
			case out <- ev:
			case <-q.stop:
				return
			}
			continue
		}
		q.mu.Unlock()
		select {
		case <-q.wake:
		case <-q.stop:
			return
		}
	}
}
