package guest

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/proto/guestv1"
)

// maxLeaseMs bounds a requested freeze lease. One hour is far beyond any
// sane snapshot copy; with requests capped here, the lease arithmetic
// cannot overflow time.Duration.
const maxLeaseMs = 3_600_000

// maxLease is maxLeaseMs as a duration.
const maxLease = maxLeaseMs * time.Millisecond

// Freezer freezes and thaws the guest's root filesystem. The real
// implementation drives the FIFREEZE/FITHAW ioctls; tests inject a fake so
// they never freeze the machine they run on.
type Freezer interface {
	Freeze() error
	Thaw() error
}

// FIFREEZE and FITHAW ioctl request numbers from linux/fs.h,
// _IOWR('X', 119, int) and _IOWR('X', 120, int), in the asm-generic
// encoding used by x86-64, arm64 and riscv64 — the guest architectures
// bamboo-raft boots. The kernel ignores the ioctl argument.
const (
	fifreezeReq = 0xc0045877
	fithawReq   = 0xc0045878
)

// ioctlFreezer freezes and thaws the filesystem holding dir. The mount
// point file is opened once and kept, so freezing never needs a filesystem
// lookup afterwards. Both ioctls require CAP_SYS_ADMIN: the real agent
// runs as root. Close is once-only and is only called after a successful
// thaw, when no further freeze can be accepted.
type ioctlFreezer struct {
	f     *os.File
	close func() error
}

// newIoctlFreezer opens the mount point at dir for later freeze ioctls.
func newIoctlFreezer(dir string) (*ioctlFreezer, error) {
	f, err := os.Open(dir)
	if err != nil {
		return nil, fmt.Errorf("open freeze target %s: %w", dir, err)
	}
	return &ioctlFreezer{f: f, close: sync.OnceValue(f.Close)}, nil
}

// Freeze issues FIFREEZE.
func (z *ioctlFreezer) Freeze() error { return z.ioctl(fifreezeReq) }

// Thaw issues FITHAW.
func (z *ioctlFreezer) Thaw() error { return z.ioctl(fithawReq) }

// Close releases the mount point file once.
func (z *ioctlFreezer) Close() error { return z.close() }

func (z *ioctlFreezer) ioctl(req uintptr) error {
	if _, _, errno := syscall.Syscall(syscall.SYS_IOCTL, z.f.Fd(), req, 0); errno != 0 {
		return errno
	}
	return nil
}

// RecoverFrozenRoot thaws a root filesystem left frozen by an earlier agent
// process. It must run before any logging or other filesystem write. EINVAL
// means the filesystem was not frozen and is therefore already recovered.
// Other failures are retried until ctx ends; production passes a background
// context so an agent restart cannot exit while the root remains frozen.
func RecoverFrozenRoot(ctx context.Context, retry time.Duration) error {
	return recoverFrozenRoot(ctx, retry, func() (rootRecoveryFreezer, error) {
		return newIoctlFreezer("/")
	})
}

type rootRecoveryFreezer interface {
	Freezer
	Close() error
}

// recoverFrozenRoot is the actual production open/thaw/close retry core. Tests
// inject only the opener, so host tests never issue FITHAW on their own root.
func recoverFrozenRoot(ctx context.Context, retry time.Duration, open func() (rootRecoveryFreezer, error)) error {
	if retry <= 0 {
		retry = time.Second
	}
	for {
		f, err := open()
		if err == nil {
			thawErr := f.Thaw()
			closeErr := f.Close()
			if thawErr == nil || errors.Is(thawErr, syscall.EINVAL) {
				return closeErr
			}
			err = thawErr
		}
		select {
		case <-ctx.Done():
			return errors.Join(context.Cause(ctx), err)
		case <-time.After(retry):
		}
	}
}

// newFreezerState builds the freeze bookkeeping with defaulted lease
// settings.
func newFreezerState(f Freezer, defaultLease, watchdogRetry time.Duration, log *slog.Logger) *freezerState {
	return &freezerState{
		log:           log,
		freezer:       f,
		defaultLease:  defaultLease,
		watchdogRetry: watchdogRetry,
	}
}

// Freeze freezes the guest root filesystem for a snapshot and starts the
// lease watchdog. A repeat Freeze returns the existing lease token and
// expiry unchanged, never extending it. Between the successful freezer
// call and the response this handler performs no filesystem writes and no
// logging: on a frozen filesystem either could block forever.
func (s *Service) Freeze(ctx context.Context, req *guestv1.FreezeRequest) (*guestv1.FreezeResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	st := s.freezer
	if ms := req.GetLeaseMs(); ms > maxLeaseMs {
		return nil, status.Errorf(codes.InvalidArgument, "lease_ms %d exceeds the maximum %d ms", ms, maxLeaseMs)
	}
	lease := time.Duration(req.GetLeaseMs()) * time.Millisecond
	if lease == 0 {
		lease = st.defaultLease
	}

	st.mu.Lock()
	defer st.mu.Unlock()
	// The request may have been canceled while it waited for the mutex: a
	// canceled Freeze must not freeze the guest late.
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if st.closing || st.closed {
		return nil, status.Error(codes.Unavailable, "guest service is shutting down")
	}
	if st.frozen {
		now := time.Now()
		if now.Before(st.leaseEnd) {
			return st.leaseResponse(now), nil
		}
		// The watchdog may be queued behind this mutex, but an expired token
		// is already invalid. Thaw it synchronously before issuing a
		// replacement; callers comparing tokens then invalidate the old save.
		if err := st.thawLocked(); err != nil {
			st.armWatchdogLocked(st.watchdogRetry)
			return nil, status.Errorf(codes.Internal, "thaw expired lease: %v", err)
		}
		// The synchronous thaw above can take a long time. A request canceled
		// while it ran must leave the guest thawed and must not create a new
		// lease the caller has already abandoned.
		if err := ctx.Err(); err != nil {
			return nil, status.FromContextError(err).Err()
		}
	}
	token, err := newLeaseToken()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "lease token: %v", err)
	}
	// Last log line before the filesystem freezes.
	st.log.Info("freezing guest filesystem", "lease", lease.String())
	if err := st.freezer.Freeze(); err != nil {
		return nil, status.Errorf(codes.Internal, "freeze: %v", err)
	}
	st.gen++
	st.frozen = true
	st.leaseToken = token
	st.leaseEnd = time.Now().Add(lease)
	st.thawFails = 0
	st.armWatchdogLocked(lease)
	return st.leaseResponse(time.Now()), nil
}

// Thaw unfreezes the guest. Thawing an unfrozen guest is OK whatever the
// token; while frozen, only the current lease token is accepted, so a
// stale cleanup from an earlier snapshot cannot thaw a later one. On a
// failed thaw the guest stays frozen and the watchdog retries.
func (s *Service) Thaw(ctx context.Context, req *guestv1.ThawRequest) (*guestv1.ThawResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	st := s.freezer
	st.mu.Lock()
	defer st.mu.Unlock()
	// Recheck after acquiring the mutex: a request canceled while it waited
	// must not thaw late under a token the caller already abandoned.
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if !st.frozen {
		return &guestv1.ThawResponse{}, nil
	}
	if req.GetLeaseToken() != st.leaseToken {
		return nil, status.Error(codes.FailedPrecondition, "stale freeze lease token")
	}
	if err := st.thawLocked(); err != nil {
		st.armWatchdogLocked(st.watchdogRetry)
		return nil, status.Errorf(codes.Internal, "thaw: %v", err)
	}
	// The filesystem is writable again, so logging cannot block on it.
	st.log.Info("guest filesystem thawed")
	return &guestv1.ThawResponse{}, nil
}

func (st *freezerState) leaseResponse(now time.Time) *guestv1.FreezeResponse {
	// Truncation is deliberate: the node must never round a remaining lease
	// up beyond the instant at which the guest watchdog may thaw it.
	remaining := max(time.Duration(0), st.leaseEnd.Sub(now))
	return &guestv1.FreezeResponse{
		LeaseToken:           st.leaseToken,
		LeaseExpiresAtUnixMs: st.leaseEnd.UnixMilli(),
		LeaseRemainingMs:     uint64(remaining / time.Millisecond),
	}
}

// thawLocked performs one thaw attempt and, on success, clears the lease.
// Callers hold mu.
func (st *freezerState) thawLocked() error {
	if err := st.freezer.Thaw(); err != nil {
		return err
	}
	st.gen++ // invalidate any watchdog callback still waiting on mu
	st.frozen = false
	st.leaseToken = ""
	st.thawFails = 0
	st.stopWatchdogLocked()
	return nil
}

// armWatchdogLocked (re)arms the one-shot thaw watchdog bound to the
// current lease generation, stopping any pending timer first so at most
// one is armed. Callers hold mu.
func (st *freezerState) armWatchdogLocked(d time.Duration) {
	st.stopWatchdogLocked()
	gen := st.gen
	st.watchdog = time.AfterFunc(d, func() { st.watchdogFire(gen) })
}

func (st *freezerState) stopWatchdogLocked() {
	if st.watchdog != nil {
		st.watchdog.Stop()
		st.watchdog = nil
	}
}

// watchdogFire runs when a lease expires or a thaw retry comes due: the
// node is gone or stuck, so the guest thaws itself. The callback is bound
// to the lease generation: a callback left waiting on mu while the lease
// changed underneath it must not thaw somebody else's snapshot, which
// Timer.Stop alone cannot guarantee. Nothing is logged until a thaw
// succeeds — a log write on a frozen filesystem could block forever. A
// failing thaw re-arms the watchdog and retries forever.
func (st *freezerState) watchdogFire(gen uint64) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if gen != st.gen || !st.frozen {
		return
	}
	if err := st.freezer.Thaw(); err != nil {
		st.thawFails++
		st.armWatchdogLocked(st.watchdogRetry)
		return
	}
	st.gen++
	st.frozen = false
	st.leaseToken = ""
	fails := st.thawFails
	st.thawFails = 0
	st.stopWatchdogLocked()
	// The filesystem is writable again, so logging cannot block on it.
	st.log.Warn("freeze lease expired; watchdog thawed the guest filesystem", "failed_attempts", fails)
}

// shutdown thaws the guest if frozen, retrying failed thaws on the
// watchdog cadence until ctx ends. If it gives up while still frozen the
// watchdog stays armed and the freezer stays open — the guest then thaws by
// itself — and it returns an error. On success it marks the service closed,
// so no Freeze can start a new lease without a watchdog, and closes the
// freezer's resources.
func (st *freezerState) shutdown(ctx context.Context) error {
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.closed {
		return st.closeErr
	}
	st.closing = true
	if !st.frozen {
		st.stopWatchdogLocked()
		return st.closeLocked()
	}
	for {
		if err := st.freezer.Thaw(); err == nil {
			st.gen++
			st.frozen = false
			st.leaseToken = ""
			st.thawFails = 0
			st.stopWatchdogLocked()
			return st.closeLocked()
		}
		if ctx.Err() != nil {
			// Keep retrying in the background after we give up.
			st.armWatchdogLocked(st.watchdogRetry)
			return fmt.Errorf("thaw at shutdown: %w", ctx.Err())
		}
		st.thawFails++
		wait := st.watchdogRetry
		st.mu.Unlock()
		select {
		case <-time.After(wait):
		case <-ctx.Done():
		}
		st.mu.Lock()
		if !st.frozen { // a concurrent Thaw or watchdog won the race
			return st.closeLocked()
		}
	}
}

// isClosed reports whether shutdown thawed the guest and released the
// freezer.
func (st *freezerState) isClosed() bool {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.closed
}

// closeLocked marks the service closed and releases the freezer once.
// Callers hold mu and must have thawed first: a failed thaw keeps the
// resources open for the watchdog's retries. After this point Freeze is
// rejected, so a closed freezer can never leave the guest frozen. The
// first close result is remembered: concurrent shutdown callers that meet
// after one of them unlocked during a retry all observe the same outcome
// instead of closing (or reporting) twice.
func (st *freezerState) closeLocked() error {
	if st.closed {
		return st.closeErr
	}
	st.closed = true
	if c, ok := st.freezer.(io.Closer); ok {
		if err := c.Close(); err != nil {
			st.closeErr = fmt.Errorf("close freezer: %w", err)
		}
	}
	return st.closeErr
}

// newLeaseToken mints an opaque single-use freeze lease token.
func newLeaseToken() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", b[:]), nil
}
