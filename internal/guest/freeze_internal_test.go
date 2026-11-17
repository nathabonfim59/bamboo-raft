package guest

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/proto/guestv1"
)

// generationFreezer counts freeze and thaw calls for the in-package
// watchdog tests; it never touches a real filesystem.
type generationFreezer struct {
	freezes int
	thaws   int
}

func (f *generationFreezer) Freeze() error {
	f.freezes++
	return nil
}

func (f *generationFreezer) Thaw() error {
	f.thaws++
	return nil
}

type startupFreezer struct {
	generationFreezer
	errs   []error
	closes int
}

func (f *startupFreezer) Close() error {
	f.closes++
	return nil
}

func (f *startupFreezer) Thaw() error {
	f.generationFreezer.Thaw()
	if len(f.errs) == 0 {
		return nil
	}
	err := f.errs[0]
	f.errs = f.errs[1:]
	return err
}

func TestRecoverFrozenRootRetriesAndAcceptsUnfrozen(t *testing.T) {
	for _, tc := range []struct {
		name string
		errs []error
		want int
	}{
		{"already thawed", []error{syscall.EINVAL}, 1},
		{"transient failures", []error{errors.New("one"), errors.New("two"), nil}, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &startupFreezer{errs: tc.errs}
			opens := 0
			open := func() (rootRecoveryFreezer, error) {
				opens++
				if opens == 1 && tc.name == "transient failures" {
					return nil, errors.New("open failed")
				}
				return f, nil
			}
			if err := recoverFrozenRoot(t.Context(), time.Millisecond, open); err != nil {
				t.Fatal(err)
			}
			if f.thaws != tc.want {
				t.Fatalf("thaw calls = %d, want %d", f.thaws, tc.want)
			}
			if f.closes != tc.want {
				t.Fatalf("close calls = %d, want one per successful open (%d)", f.closes, tc.want)
			}
		})
	}
}

// TestFreezeExpiredLeaseNotReturned proves an expired token cannot look valid
// merely because its watchdog is still waiting for the state mutex.
func TestFreezeExpiredLeaseNotReturned(t *testing.T) {
	fz := &generationFreezer{}
	st := newFreezerState(fz, time.Minute, time.Hour, slog.New(slog.DiscardHandler))
	svc := &Service{freezer: st}
	first, err := svc.Freeze(context.Background(), &guestv1.FreezeRequest{LeaseMs: 10_000})
	if err != nil {
		t.Fatal(err)
	}
	st.mu.Lock()
	st.leaseEnd = time.Now().Add(-time.Second)
	st.stopWatchdogLocked()
	st.mu.Unlock()

	second, err := svc.Freeze(context.Background(), &guestv1.FreezeRequest{LeaseMs: 10_000})
	if err != nil {
		t.Fatal(err)
	}
	if second.GetLeaseToken() == first.GetLeaseToken() {
		t.Fatal("Freeze returned the already-expired lease token")
	}
	if fz.thaws != 1 {
		t.Fatalf("expired lease thaw calls = %d, want 1 before a replacement freeze", fz.thaws)
	}
}

// gatedThawFreezer blocks one Thaw call until the test releases it, so a
// canceled Freeze can be staged while the expired-lease thaw is running.
type gatedThawFreezer struct {
	generationFreezer
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (f *gatedThawFreezer) Thaw() error {
	f.once.Do(func() { close(f.entered) })
	<-f.release
	return f.generationFreezer.Thaw()
}

// TestFreezeCancelDuringExpiredThawCreatesNoNewLease: canceling a Freeze while
// it synchronously thaws an expired lease must leave the guest unfrozen and
// must not create a replacement lease the caller already abandoned.
func TestFreezeCancelDuringExpiredThawCreatesNoNewLease(t *testing.T) {
	fz := &gatedThawFreezer{entered: make(chan struct{}), release: make(chan struct{})}
	st := newFreezerState(fz, time.Minute, time.Hour, slog.New(slog.DiscardHandler))
	svc := &Service{freezer: st}
	if _, err := svc.Freeze(context.Background(), &guestv1.FreezeRequest{LeaseMs: 10_000}); err != nil {
		t.Fatal(err)
	}
	st.mu.Lock()
	st.leaseEnd = time.Now().Add(-time.Second)
	st.stopWatchdogLocked()
	st.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := svc.Freeze(ctx, &guestv1.FreezeRequest{LeaseMs: 10_000})
		done <- err
	}()
	<-fz.entered
	cancel()
	close(fz.release)
	if err := <-done; status.Code(err) != codes.Canceled {
		t.Fatalf("canceled expired-lease Freeze = %v, want Canceled", err)
	}
	st.mu.Lock()
	frozen, token := st.frozen, st.leaseToken
	st.mu.Unlock()
	if frozen || token != "" {
		t.Fatalf("canceled Freeze left a lease: frozen=%t token=%q", frozen, token)
	}
	if fz.freezes != 1 {
		t.Fatalf("expired thaw path froze again: %d freeze calls", fz.freezes)
	}
	if fz.thaws != 1 {
		t.Fatalf("expired lease thaw calls = %d, want 1", fz.thaws)
	}
}

// TestWatchdogFireRejectsStaleGeneration executes the generation check
// directly: a callback bound to an older lease must neither thaw the guest
// nor clear its lease.
func TestWatchdogFireRejectsStaleGeneration(t *testing.T) {
	fz := &generationFreezer{}
	st := newFreezerState(fz, time.Minute, time.Millisecond, slog.New(slog.DiscardHandler))
	st.mu.Lock()
	st.frozen = true
	st.leaseToken = "lease-2"
	st.gen = 7
	st.mu.Unlock()

	st.watchdogFire(6) // callback left over from lease 1
	if fz.thaws != 0 {
		t.Fatalf("stale generation thawed the guest: %d thaw calls", fz.thaws)
	}
	st.mu.Lock()
	frozen, token := st.frozen, st.leaseToken
	st.mu.Unlock()
	if !frozen || token != "lease-2" {
		t.Fatalf("stale generation cleared the lease: frozen=%v token=%q", frozen, token)
	}

	st.watchdogFire(7) // the current generation still thaws
	if fz.thaws != 1 {
		t.Fatalf("current generation thaw calls = %d, want 1", fz.thaws)
	}
	st.mu.Lock()
	frozen = st.frozen
	st.mu.Unlock()
	if frozen {
		t.Fatal("current generation did not clear the freeze")
	}
}
