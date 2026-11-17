package guest_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/guest"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/guestv1"
)

// freeze starts a freeze with the given lease and returns the lease.
func (h *harness) freeze(t *testing.T, leaseMs uint32) *guestv1.FreezeResponse {
	t.Helper()
	resp, err := h.c.Freeze(t.Context(), &guestv1.FreezeRequest{LeaseMs: leaseMs})
	if err != nil {
		t.Fatalf("Freeze(%d ms): %v", leaseMs, err)
	}
	if resp.GetLeaseToken() == "" {
		t.Fatal("empty lease token")
	}
	return resp
}

func (h *harness) thaw(t *testing.T, token string) {
	t.Helper()
	if _, err := h.c.Thaw(t.Context(), &guestv1.ThawRequest{LeaseToken: token}); err != nil {
		t.Fatalf("Thaw(%q): %v", token, err)
	}
}

func TestFreezeThawLifecycle(t *testing.T) {
	h := newHarness(t, nil)
	before := time.Now().UnixMilli()
	lease := h.freeze(t, 10_000)
	if exp := lease.GetLeaseExpiresAtUnixMs(); exp < before+9_000 || exp > time.Now().UnixMilli()+11_000 {
		t.Fatalf("expiry %d not ~10s out", exp)
	}
	if lease.GetLeaseRemainingMs() == 0 || lease.GetLeaseRemainingMs() > 10_000 {
		t.Fatalf("remaining lease = %d ms, want (0,10000]", lease.GetLeaseRemainingMs())
	}
	if f, _, frozen := h.fz.counts(); frozen != 1 || f != 1 {
		t.Fatalf("after freeze: freeze calls=%d frozen=%d", f, frozen)
	}
	h.thaw(t, lease.GetLeaseToken())
	if _, thaw, frozen := h.fz.counts(); frozen != 0 || thaw != 1 {
		t.Fatalf("after thaw: thaw=%d frozen=%d", thaw, frozen)
	}
	// Thawing again is idempotent and does not touch the freezer.
	h.thaw(t, lease.GetLeaseToken())
	if _, thaw, _ := h.fz.counts(); thaw != 1 {
		t.Fatalf("repeat thaw reached the freezer: %d calls", thaw)
	}
	// A new freeze gets a fresh token.
	next := h.freeze(t, 10_000)
	if next.GetLeaseToken() == lease.GetLeaseToken() {
		t.Fatal("token reused across freezes")
	}
	h.thaw(t, next.GetLeaseToken())
}

// TestFreezeRepeatNeverExtends is the near-expiry regression: a repeat
// Freeze must return the same token and the same deadline, never extra
// time.
func TestFreezeRepeatNeverExtends(t *testing.T) {
	h := newHarness(t, nil)
	first := h.freeze(t, 300)
	time.Sleep(150 * time.Millisecond) // halfway into the lease
	second, err := h.c.Freeze(t.Context(), &guestv1.FreezeRequest{LeaseMs: 60_000})
	if err != nil {
		t.Fatal(err)
	}
	if second.GetLeaseToken() != first.GetLeaseToken() {
		t.Fatalf("repeat Freeze returned a new token %q", second.GetLeaseToken())
	}
	if second.GetLeaseRemainingMs() >= first.GetLeaseRemainingMs() {
		t.Fatalf("repeat remaining lease did not decrease: first=%d second=%d", first.GetLeaseRemainingMs(), second.GetLeaseRemainingMs())
	}
	if second.GetLeaseExpiresAtUnixMs() != first.GetLeaseExpiresAtUnixMs() {
		t.Fatalf("repeat Freeze moved the deadline: %d vs %d",
			second.GetLeaseExpiresAtUnixMs(), first.GetLeaseExpiresAtUnixMs())
	}
	if f, _, _ := h.fz.counts(); f != 1 {
		t.Fatalf("repeat Freeze froze again: %d calls", f)
	}
	h.thaw(t, first.GetLeaseToken())
}

func TestFreezeStaleTokenRejected(t *testing.T) {
	h := newHarness(t, nil)
	lease := h.freeze(t, 10_000)
	_, err := h.c.Thaw(t.Context(), &guestv1.ThawRequest{LeaseToken: "not-the-token"})
	wantCode(t, err, codes.FailedPrecondition)
	if _, _, frozen := h.fz.counts(); frozen != 1 {
		t.Fatal("stale token thawed the guest")
	}
	h.thaw(t, lease.GetLeaseToken())
}

// TestFreezeWatchdogExpiry: the guest thaws by itself when the lease
// runs out, and everything stays idempotent afterwards.
func TestFreezeWatchdogExpiry(t *testing.T) {
	h := newHarness(t, nil)
	lease := h.freeze(t, 100)
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, _, frozen := h.fz.counts(); frozen == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("watchdog did not thaw after lease expiry")
		}
		time.Sleep(20 * time.Millisecond)
	}
	// The old token thaws nothing but is not an error.
	h.thaw(t, lease.GetLeaseToken())
	// And the guest can freeze again.
	next := h.freeze(t, 10_000)
	if next.GetLeaseToken() == lease.GetLeaseToken() {
		t.Fatal("token reused after watchdog thaw")
	}
	h.thaw(t, next.GetLeaseToken())
}

// TestWatchdogRetriesFailedThaws: failing thaws are retried until one
// succeeds.
func TestWatchdogRetriesFailedThaws(t *testing.T) {
	h := newHarness(t, func(cfg *guest.Config) { cfg.WatchdogRetry = 40 * time.Millisecond })
	h.freeze(t, 60)
	h.fz.queueThawErrs(errors.New("thaw failed once"), errors.New("thaw failed twice"))
	deadline := time.Now().Add(3 * time.Second)
	for {
		if _, thaw, frozen := h.fz.counts(); frozen == 0 {
			if thaw < 3 {
				t.Fatalf("thawed after only %d attempts", thaw)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("watchdog never managed to thaw")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestFreezeDefaultLease(t *testing.T) {
	h := newHarness(t, func(cfg *guest.Config) { cfg.DefaultLease = 5 * time.Second })
	before := time.Now().UnixMilli()
	lease := h.freeze(t, 0)
	if exp := lease.GetLeaseExpiresAtUnixMs(); exp < before+4_000 || exp > time.Now().UnixMilli()+6_000 {
		t.Fatalf("expiry %d not ~5s out", exp)
	}
	h.thaw(t, lease.GetLeaseToken())
}

// TestFreezeLeaseBound rejects leases over the maximum (the value that
// would overflow-safe guard everything else) and accepts the maximum.
func TestFreezeLeaseBound(t *testing.T) {
	h := newHarness(t, nil)
	_, err := h.c.Freeze(t.Context(), &guestv1.FreezeRequest{LeaseMs: 3_600_001})
	wantCode(t, err, codes.InvalidArgument)
	// The maximum itself is fine and thaws cleanly.
	lease := h.freeze(t, 3_600_000)
	h.thaw(t, lease.GetLeaseToken())
}

// TestNewRejectsBadConfig validates the lease and uid/gid bounds at
// construction.
func TestNewRejectsBadConfig(t *testing.T) {
	base := func() guest.Config {
		return guest.Config{
			HomeRoot: t.TempDir(),
			TmpRoot:  t.TempDir(),
			Freezer:  &fakeFreezer{},
		}
	}
	for _, tt := range []struct {
		name string
		mod  func(*guest.Config)
	}{
		{"negative uid", func(c *guest.Config) { c.UID = -1 }},
		{"uid is the no-change sentinel", func(c *guest.Config) { c.UID = 4294967295 }},
		{"gid out of range", func(c *guest.Config) { c.GID = 1 << 40 }},
		{"default lease over max", func(c *guest.Config) { c.DefaultLease = 2 * time.Hour }},
	} {
		cfg := base()
		tt.mod(&cfg)
		if _, err := guest.New(cfg); err == nil {
			t.Errorf("%s: New accepted the config", tt.name)
		}
	}
	// UID 0 means "use the default", which is valid.
	cfg := base()
	if _, err := guest.New(cfg); err != nil {
		t.Fatalf("defaults rejected: %v", err)
	}
}

func TestFreezeFreezeFailure(t *testing.T) {
	h := newHarness(t, nil)
	h.fz.setFreezeErr(errors.New("cannot freeze"))
	_, err := h.c.Freeze(t.Context(), &guestv1.FreezeRequest{LeaseMs: 10_000})
	wantCode(t, err, codes.Internal)
	if _, _, frozen := h.fz.counts(); frozen != 0 {
		t.Fatal("failed freeze left the guest frozen")
	}
	// The state recovers: the next freeze works.
	h.fz.setFreezeErr(nil)
	lease := h.freeze(t, 10_000)
	h.thaw(t, lease.GetLeaseToken())
}

// TestThawFailureStaysFrozenAndRetries: a failed Thaw keeps the lease; the
// same token can thaw again.
func TestThawFailureStaysFrozenAndRetries(t *testing.T) {
	h := newHarness(t, nil)
	lease := h.freeze(t, 10_000)
	h.fz.queueThawErrs(errors.New("thaw hiccup"))
	_, err := h.c.Thaw(t.Context(), &guestv1.ThawRequest{LeaseToken: lease.GetLeaseToken()})
	wantCode(t, err, codes.Internal)
	if _, _, frozen := h.fz.counts(); frozen != 1 {
		t.Fatal("failed thaw unfroze the guest")
	}
	h.thaw(t, lease.GetLeaseToken())
	if _, _, frozen := h.fz.counts(); frozen != 0 {
		t.Fatal("second thaw with same token failed")
	}
}

func TestThawUnfrozenAnyTokenOK(t *testing.T) {
	h := newHarness(t, nil)
	h.thaw(t, "garbage")
}

// TestPingAndThawWhileFrozen: the service keeps serving with a frozen
// filesystem (with the fake freezer standing in for the real ioctl).
func TestPingAndThawWhileFrozen(t *testing.T) {
	h := newHarness(t, nil)
	lease := h.freeze(t, 10_000)
	resp, err := h.c.Ping(t.Context(), &guestv1.PingRequest{})
	if err != nil || resp.GetVersion() != "test" {
		t.Fatalf("Ping while frozen: %v %v", resp, err)
	}
	h.thaw(t, lease.GetLeaseToken())
}

// TestWatchdogDoesNotThawLaterLease is the stale-callback race: a watchdog
// callback that already fired and is waiting on the state mutex while the
// lease changes underneath it must not thaw the new lease. The race is
// staged by gating a Thaw RPC mid-call — it holds the mutex while the
// lease-1 callback queues behind it — then releasing and immediately
// freezing a long second lease. Waits use precise per-call counters, and
// the generation-rejection branch itself is proven separately in
// freeze_internal_test.go.
func TestWatchdogDoesNotThawLaterLease(t *testing.T) {
	h := newHarness(t, func(cfg *guest.Config) { cfg.WatchdogRetry = 30 * time.Millisecond })

	for range 10 {
		first := h.freeze(t, 40)

		// A Thaw RPC acquires the mutex and parks inside the fake
		// freezer on the gate.
		gate := make(chan struct{})
		var releaseOnce sync.Once
		release := func() { releaseOnce.Do(func() { close(gate) }) }
		t.Cleanup(release) // every failure path releases the gate
		h.fz.gateThaw(gate)
		before := h.fz.thawCallsNow()
		thawDone := make(chan error, 1)
		go func() {
			_, err := h.c.Thaw(context.Background(), &guestv1.ThawRequest{LeaseToken: first.GetLeaseToken()})
			thawDone <- err
		}()
		waitFor(t, time.Second, func() bool { return h.fz.thawCallsNow() == before+1 })

		// Lease 1 expires; its watchdog callback fires and queues on
		// the mutex held by the parked Thaw.
		time.Sleep(80 * time.Millisecond)

		// Release the gate: the Thaw completes (thawing lease 1 and
		// bumping the generation) and the queued callback races the
		// second freeze for the mutex.
		release()
		if err := <-thawDone; err != nil {
			t.Fatalf("gated Thaw: %v", err)
		}
		second := h.freeze(t, 10_000)

		// Lease 2 has 10 s left; nothing legitimate can thaw it in
		// this window — only a stale lease-1 callback could.
		time.Sleep(300 * time.Millisecond)
		if _, _, frozen := h.fz.counts(); frozen != 1 {
			t.Fatal("a stale watchdog callback thawed the later lease")
		}
		h.thaw(t, second.GetLeaseToken())
	}
}

// TestFreezeCtxRecheckedAfterLock: a Freeze queued behind a blocked
// operation, canceled while it waits for the mutex, must not freeze the
// guest after it finally acquires the lock.
func TestFreezeCtxRecheckedAfterLock(t *testing.T) {
	h := newHarness(t, nil)
	first := h.freeze(t, 10_000)

	gate := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(gate) }) }
	t.Cleanup(release)
	h.fz.gateThaw(gate)

	thawDone := make(chan error, 1)
	thawBefore := h.fz.thawCallsNow()
	go func() {
		_, err := h.svc.Thaw(context.Background(), &guestv1.ThawRequest{LeaseToken: first.GetLeaseToken()})
		thawDone <- err
	}()
	waitFor(t, time.Second, func() bool { return h.fz.thawCallsNow() == thawBefore+1 })

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	freezeDone := make(chan error, 1)
	go func() {
		_, err := h.svc.Freeze(ctx, &guestv1.FreezeRequest{LeaseMs: 10_000})
		freezeDone <- err
	}()
	time.Sleep(100 * time.Millisecond) // let Freeze queue on the mutex
	cancel()
	release()
	if err := <-thawDone; err != nil {
		t.Fatalf("gated thaw: %v", err)
	}
	if err := <-freezeDone; status.Code(err) != codes.Canceled {
		t.Fatalf("queued canceled Freeze returned %v, want Canceled", err)
	}
	if f, _, frozen := h.fz.counts(); f != 1 || frozen != 0 {
		t.Fatalf("canceled Freeze had a side effect: %d freeze calls, frozen=%d", f, frozen)
	}
}

// TestThawCtxRecheckedAfterLock: a Thaw queued behind a blocked Freeze,
// canceled while it waits, must not touch the freezer once the lock is
// released under the new lease.
func TestThawCtxRecheckedAfterLock(t *testing.T) {
	h := newHarness(t, nil)

	gate := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(gate) }) }
	t.Cleanup(release)
	h.fz.gateFreeze(gate)

	type freezeResult struct {
		resp *guestv1.FreezeResponse
		err  error
	}
	freezeDone := make(chan freezeResult, 1)
	before, _, _ := h.fz.counts()
	go func() {
		resp, err := h.svc.Freeze(context.Background(), &guestv1.FreezeRequest{LeaseMs: 10_000})
		freezeDone <- freezeResult{resp: resp, err: err}
	}()
	waitFor(t, time.Second, func() bool { f, _, _ := h.fz.counts(); return f == before+1 })

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	thawDone := make(chan error, 1)
	go func() {
		_, err := h.svc.Thaw(ctx, &guestv1.ThawRequest{LeaseToken: "whatever"})
		thawDone <- err
	}()
	time.Sleep(100 * time.Millisecond) // let Thaw queue on the mutex
	cancel()
	release()
	fr := <-freezeDone
	if fr.err != nil {
		t.Fatalf("gated freeze: %v", fr.err)
	}
	if err := <-thawDone; status.Code(err) != codes.Canceled {
		t.Fatalf("queued canceled Thaw returned %v, want Canceled", err)
	}
	if _, thaw, _ := h.fz.counts(); thaw != 0 {
		t.Fatalf("canceled Thaw reached the freezer: %d calls", thaw)
	}
	h.thaw(t, fr.resp.GetLeaseToken())
}

// TestConcurrentShutdownClosesFreezerOnce: two shutdown callers can both
// be inside the retry loop when one of them unlocks between attempts. The
// close is guarded, so exactly one of them releases the freezer, both see
// the same outcome, and no freeze can start afterwards.
func TestConcurrentShutdownClosesFreezerOnce(t *testing.T) {
	h := newHarness(t, func(cfg *guest.Config) { cfg.WatchdogRetry = 20 * time.Millisecond })
	h.freeze(t, 10_000)
	h.fz.setThawErr(errors.New("still frozen"))

	var wg sync.WaitGroup
	results := make(chan error, 2)
	for range 2 {
		wg.Go(func() { results <- h.svc.Shutdown(context.Background()) })
	}
	waitFor(t, 5*time.Second, func() bool {
		_, thaw, _ := h.fz.counts()
		return thaw >= 2 // both callers reached the retrying loop
	})
	h.fz.setThawErr(nil)
	wg.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatalf("concurrent shutdown: %v", err)
		}
	}
	if got := h.fz.closeCount(); got != 1 {
		t.Fatalf("freezer closes = %d, want exactly 1", got)
	}
	// No refreeze after the concurrent shutdowns.
	_, err := h.c.Freeze(t.Context(), &guestv1.FreezeRequest{LeaseMs: 10_000})
	wantCode(t, err, codes.Unavailable)
	if f, _, _ := h.fz.counts(); f != 1 {
		t.Fatalf("closed service froze again: %d freeze calls", f)
	}
}

// TestShutdownThawsFrozenGuestAndClosesOnce: a successful shutdown thaws,
// closes the freezer exactly once, is retry-safe, and rejects new freezes.
func TestShutdownThawsFrozenGuestAndClosesOnce(t *testing.T) {
	h := newHarness(t, nil)
	lease := h.freeze(t, 10_000)
	if err := h.svc.Shutdown(t.Context()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if _, _, frozen := h.fz.counts(); frozen != 0 {
		t.Fatal("shutdown left the guest frozen")
	}
	if got := h.fz.closeCount(); got != 1 {
		t.Fatalf("freezer closes = %d, want 1", got)
	}
	// A second shutdown is retry-safe.
	if err := h.svc.Shutdown(t.Context()); err != nil {
		t.Fatalf("second shutdown: %v", err)
	}
	if got := h.fz.closeCount(); got != 1 {
		t.Fatalf("second shutdown closed the freezer again: %d", got)
	}
	// The closed service must not start a new lease.
	_, err := h.c.Freeze(t.Context(), &guestv1.FreezeRequest{LeaseMs: 10_000})
	wantCode(t, err, codes.Unavailable)
	if f, _, _ := h.fz.counts(); f != 1 {
		t.Fatalf("closed service froze again: %d freeze calls", f)
	}
	_ = lease
}

// TestShutdownRetriesFailedThaws: a transient thaw failure is retried on
// the watchdog cadence and then closes cleanly.
func TestShutdownRetriesFailedThaws(t *testing.T) {
	h := newHarness(t, func(cfg *guest.Config) { cfg.WatchdogRetry = 30 * time.Millisecond })
	h.freeze(t, 10_000)
	h.fz.queueThawErrs(errors.New("nope"), errors.New("nope"))
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	if err := h.svc.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown with retries: %v", err)
	}
	if _, thaw, frozen := h.fz.counts(); frozen != 0 || thaw < 3 {
		t.Fatalf("frozen=%d thaw calls=%d", frozen, thaw)
	}
	if got := h.fz.closeCount(); got != 1 {
		t.Fatalf("freezer closes = %d, want 1", got)
	}
}

// TestShutdownGiveUpRetainsResources: when the deadline expires while the
// guest is still frozen, shutdown returns an error but keeps the freezer
// and roots usable for the watchdog and for an explicit later thaw; a
// second shutdown then closes exactly once.
func TestShutdownGiveUpRetainsResources(t *testing.T) {
	h := newHarness(t, func(cfg *guest.Config) { cfg.WatchdogRetry = 30 * time.Millisecond })
	lease := h.freeze(t, 10_000)
	h.fz.setThawErr(errors.New("frozen solid"))
	giveUpCtx, giveUpCancel := context.WithTimeout(t.Context(), 150*time.Millisecond)
	defer giveUpCancel()
	if err := h.svc.Shutdown(giveUpCtx); err == nil {
		t.Fatal("shutdown with a dead freezer succeeded")
	}
	if _, _, frozen := h.fz.counts(); frozen != 1 {
		t.Fatal("shutdown state corrupted while still frozen")
	}
	if got := h.fz.closeCount(); got != 0 {
		t.Fatalf("failed thaw closed the freezer: %d", got)
	}
	// The confinement roots stayed open: the service can still run
	// commands while the watchdog retries the thaw.
	res := h.exec(t, "echo still-alive")
	if res.Stdout != "still-alive\n" {
		t.Fatalf("service unusable after failed shutdown: stdout %q", res.Stdout)
	}
	// New freezes are refused while the service is shutting down.
	_, err := h.c.Freeze(t.Context(), &guestv1.FreezeRequest{LeaseMs: 10_000})
	wantCode(t, err, codes.Unavailable)
	// The resources stayed valid: the same lease token can still thaw.
	h.fz.setThawErr(nil)
	h.thaw(t, lease.GetLeaseToken())
	if _, _, frozen := h.fz.counts(); frozen != 0 {
		t.Fatal("explicit thaw after a failed shutdown did not thaw")
	}
	if err := h.svc.Shutdown(t.Context()); err != nil {
		t.Fatalf("second shutdown: %v", err)
	}
	if got := h.fz.closeCount(); got != 1 {
		t.Fatalf("freezer closes = %d, want 1", got)
	}
}

// TestConcurrentThawAndShutdown: handlers and shutdown racing on a frozen
// guest converge to thawed without double-thawing.
func TestConcurrentThawAndShutdown(t *testing.T) {
	for range 10 {
		h := newHarness(t, nil)
		lease := h.freeze(t, 10_000)
		done := make(chan error, 1)
		go func() {
			_, err := h.c.Thaw(context.Background(), &guestv1.ThawRequest{LeaseToken: lease.GetLeaseToken()})
			done <- err
		}()
		if err := h.svc.Shutdown(t.Context()); err != nil {
			t.Fatalf("shutdown racing a Thaw: %v", err)
		}
		if err := <-done; err != nil {
			t.Fatalf("Thaw racing shutdown: %v", err)
		}
		if _, thaw, frozen := h.fz.counts(); frozen != 0 || thaw > 2 {
			t.Fatalf("frozen=%d thaw calls=%d", frozen, thaw)
		}
	}
}

// waitFor polls cond until it holds or the deadline passes.
func waitFor(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition never became true")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
