// Package guest_test exercises the guest agent service over real gRPC
// (bufconn) with temp confinement roots and an injected fake freezer, so
// nothing here freezes or writes outside the test's directories. It runs
// unprivileged: the exec credential is the test process itself.
package guest_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/nathabonfim59/bamboo-raft/internal/guest"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/guestv1"
)

// fakeFreezer records freeze/thaw calls, fails on cue and can gate a
// freeze or thaw so a test can hold the service mutex mid-operation while
// another request queues behind it.
type fakeFreezer struct {
	mu          sync.Mutex
	frozen      int
	freezeErr   error
	thawErr     error   // persistent failure for every Thaw
	thawErrs    []error // consumed left to right, one per Thaw call
	freezeCalls int
	thawCalls   int
	gate        chan struct{}
	freezeGate  chan struct{}
	closeCalls  int
	closeErr    error
}

func (f *fakeFreezer) Freeze() error {
	f.mu.Lock()
	f.freezeCalls++
	gate := f.freezeGate
	f.freezeGate = nil
	err := f.freezeErr
	f.mu.Unlock()
	if gate != nil {
		<-gate // hold the caller (and the service mutex) mid-freeze
	}
	if err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.frozen++
	return nil
}

func (f *fakeFreezer) Thaw() error {
	f.mu.Lock()
	f.thawCalls++
	gate := f.gate
	f.gate = nil
	err := f.thawErr
	if len(f.thawErrs) > 0 {
		e := f.thawErrs[0]
		f.thawErrs = f.thawErrs[1:]
		if e != nil {
			err = e
		}
	}
	f.mu.Unlock()
	if gate != nil {
		<-gate // hold the caller (and the service mutex) mid-thaw
	}
	if err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.frozen == 0 {
		return errors.New("thaw of an unfrozen filesystem")
	}
	f.frozen--
	return nil
}

// Close lets tests observe the service's once-only freezer release.
func (f *fakeFreezer) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closeCalls++
	return f.closeErr
}

func (f *fakeFreezer) setFreezeErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.freezeErr = err
}

func (f *fakeFreezer) setThawErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.thawErr = err
}

// gateThaw makes the next Thaw block on ch until it is closed.
func (f *fakeFreezer) gateThaw(ch chan struct{}) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gate = ch
}

// gateFreeze makes the next Freeze block on ch until it is closed.
func (f *fakeFreezer) gateFreeze(ch chan struct{}) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.freezeGate = ch
}

// thawCallsNow is a precise per-call counter for synchronization.
func (f *fakeFreezer) thawCallsNow() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.thawCalls
}

// closeCount reports how many times the service released the freezer.
func (f *fakeFreezer) closeCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closeCalls
}

func (f *fakeFreezer) queueThawErrs(errs ...error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.thawErrs = append(f.thawErrs, errs...)
}

func (f *fakeFreezer) counts() (freeze, thaw, frozen int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.freezeCalls, f.thawCalls, f.frozen
}

// harness is one service on bufconn with temp roots.
type harness struct {
	t    *testing.T
	c    guestv1.GuestClient
	fz   *fakeFreezer
	svc  *guest.Service
	home string // the configured home root (HOME and cwd base)
	tmp  string // the configured tmp root
	uid  int
	gid  int
}

// newHarness starts a service; mod can adjust the config before New.
func newHarness(t *testing.T, mod func(*guest.Config)) *harness {
	return newHarnessMode(t, mod, false)
}

// newHelperHarness forces the credential-dropped file helper, so the whole
// file RPC path speaks the helper protocol even though the test process is
// unprivileged. The test binary re-executes itself via TestMain and, because
// NoSetGroups is set, keeps its own credentials.
func newHelperHarness(t *testing.T) *harness {
	return newHarnessMode(t, nil, true)
}

// newHarnessMode starts a service; force selects the file helper.
func newHarnessMode(t *testing.T, mod func(*guest.Config), forceHelper bool) *harness {
	t.Helper()
	base := t.TempDir()
	home := filepath.Join(base, "home/user")
	tmp := filepath.Join(base, "tmp")
	for _, d := range []string{home, tmp} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	fz := &fakeFreezer{}
	cfg := guest.Config{
		Log:             slog.New(slog.DiscardHandler),
		Version:         "test",
		UID:             os.Getuid(),
		GID:             os.Getgid(),
		NoSetGroups:     true,
		ForceFileHelper: forceHelper,
		HomeRoot:        home,
		TmpRoot:         tmp,
		DefaultLease:    2 * time.Minute,
		WatchdogRetry:   5 * time.Second,
		Freezer:         fz,
	}
	if mod != nil {
		mod(&cfg)
	}
	return startHarness(t, cfg, home, tmp, fz, os.Getuid(), os.Getgid())
}

// startHarness builds the service and its gRPC client over bufconn for an
// already-prepared pair of roots.
func startHarness(t *testing.T, cfg guest.Config, home, tmp string, fz *fakeFreezer, uid, gid int) *harness {
	t.Helper()
	svc, err := guest.New(cfg)
	if err != nil {
		t.Fatalf("guest.New: %v", err)
	}
	lis := bufconn.Listen(1 << 20)
	g := grpc.NewServer()
	svc.Register(g)
	go g.Serve(lis)
	t.Cleanup(g.Stop)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := svc.Shutdown(ctx); err != nil {
			t.Errorf("shutdown: %v", err)
		}
	})
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return &harness{
		t:    t,
		c:    guestv1.NewGuestClient(conn),
		fz:   fz,
		svc:  svc,
		home: home,
		tmp:  tmp,
		uid:  uid,
		gid:  gid,
	}
}

func wantCode(t *testing.T, err error, code codes.Code) {
	t.Helper()
	if got := status.Code(err); got != code {
		t.Fatalf("got %v (%v), want code %v", got, err, code)
	}
}

func TestPingReturnsVersion(t *testing.T) {
	h := newHarness(t, nil)
	resp, err := h.c.Ping(t.Context(), &guestv1.PingRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetVersion() != "test" {
		t.Fatalf("version %q, want %q", resp.GetVersion(), "test")
	}
}

// TestExecRunsAsConfiguredUser proves the credential is applied: the
// command runs as the uid/gid the service was configured with (the test
// process here, uid 1000 in a real guest) with HOME at the home root.
func TestExecRunsAsConfiguredUser(t *testing.T) {
	h := newHarness(t, nil)
	res := h.exec(t, `id -u; id -g; printf '%s' "$HOME"; test -w "$HOME" && printf '\nwritable'`)
	want := fmt.Sprintf("%d\n%d\n%s\nwritable", h.uid, h.gid, h.home)
	if res.Stdout != want {
		t.Fatalf("stdout %q, want %q", res.Stdout, want)
	}
	if res.Exit.ExitCode == nil || *res.Exit.ExitCode != 0 {
		t.Fatalf("exit %v", res.Exit)
	}
}
