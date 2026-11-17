package db

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

// errListenerClosed reports a listener whose lifetime ended (Close, or the
// context it was started with).
var errListenerClosed = errors.New("db: job listener closed")

// JobListener wakes workers after job activity. It holds one dedicated
// connection LISTENing on [JobsChannel] and wakes on every notification; a
// timed poll fallback guarantees progress even when a notification is lost,
// a connection drops, or a job was written without NOTIFY.
type JobListener struct {
	cfg       pgx.ConnConfig
	pollEvery time.Duration
	notify    chan struct{}
	ctx       context.Context
	cancel    context.CancelFunc
	done      chan struct{}
	closeOnce sync.Once
}

// listenerCloseTimeout bounds connection cleanup so Close and reconnects
// cannot block on a half-dead backend.
const listenerCloseTimeout = 2 * time.Second

// closeConn releases one listener connection under a bounded context; the
// listener lifetime can be cancelled already, so cleanup must not inherit it.
func closeConn(conn *pgx.Conn) {
	if conn == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), listenerCloseTimeout)
	defer cancel()
	_ = conn.Close(ctx) //nolint:errcheck // nothing to recover; the socket is dropped either way
}

// ListenJobs starts a listener bound to the Store's connection parameters
// and to ctx: cancelling ctx (or [JobListener.Close]) stops the listener and
// releases its connection. The listener owns the connection so no pool slot
// is blocked. pollEvery bounds how long a lost wake-up can delay a worker
// (it is not the claim interval: workers claim whenever they are free).
func (s *Store) ListenJobs(ctx context.Context, pollEvery time.Duration) (*JobListener, error) {
	if pollEvery <= 0 {
		pollEvery = 10 * time.Second
	}
	l := &JobListener{
		cfg:       *s.pool.Config().ConnConfig,
		pollEvery: pollEvery,
		notify:    make(chan struct{}, 1),
		done:      make(chan struct{}),
	}
	l.ctx, l.cancel = context.WithCancel(ctx)
	conn, err := pgx.ConnectConfig(l.ctx, &l.cfg)
	if err != nil {
		l.cancel()
		return nil, fmtError("listen jobs connect", err)
	}
	if _, err := conn.Exec(l.ctx, "LISTEN "+JobsChannel); err != nil {
		closeConn(conn)
		l.cancel()
		return nil, fmtError("listen jobs", err)
	}
	go l.receive(conn)
	return l, nil
}

// receive pumps notifications into the wake channel until the listener's
// lifetime ends. On connection loss it closes the dead connection and
// reconnects with a short backoff; while disconnected the poll fallback
// keeps workers going. The deferred closure closes whatever connection is
// current when the loop exits, so reconnects never leak; each failed
// connection is also closed explicitly before it is replaced.
func (l *JobListener) receive(conn *pgx.Conn) {
	defer close(l.done)
	defer func() { closeConn(conn) }()
	for {
		_, err := conn.WaitForNotification(l.ctx)
		if err == nil {
			l.wake()
			continue
		}
		if l.ctx.Err() != nil {
			return
		}
		// The connection is dead: release it before waiting.
		closeConn(conn)
		if !l.sleep(l.pollEvery) {
			return
		}
		for {
			c, cerr := pgx.ConnectConfig(l.ctx, &l.cfg)
			if cerr == nil {
				if _, eerr := c.Exec(l.ctx, "LISTEN "+JobsChannel); eerr == nil {
					conn = c
					break
				}
				closeConn(c)
			}
			if !l.sleep(3 * l.pollEvery) {
				return
			}
		}
	}
}

// wake delivers one pending signal, never blocking the receiver.
func (l *JobListener) wake() {
	select {
	case l.notify <- struct{}{}:
	default:
	}
}

// sleep waits d unless the listener's lifetime ends first; it reports
// whether to continue.
func (l *JobListener) sleep(d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-l.ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// Await blocks until a job may be runnable: a notification arrived, the poll
// interval elapsed, or ctx ended. Returning nil means "look now"; the claim
// is always the source of truth.
func (l *JobListener) Await(ctx context.Context) error {
	ticker := time.NewTicker(l.pollEvery)
	defer ticker.Stop()
	select {
	case <-l.notify:
		return nil
	case <-ticker.C:
		return nil
	case <-l.ctx.Done():
		return errListenerClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close stops the listener, releases its connection and joins the receiver
// goroutine, so nothing leaks. It is safe to call more than once.
func (l *JobListener) Close() {
	l.closeOnce.Do(l.cancel)
	<-l.done
}
