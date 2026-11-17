// Internal regressions for the detached-process review findings: a failed
// log creation must not leak untracked state, a retained rollback must keep
// its pinned metadata-directory handle usable until the durable removal, and
// a real ESRCH from kill(2) must never be reported as a signal sent. The
// tests construct the service directly (no gRPC) so they can set the
// service-scoped procFS seams.
package guest

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/proto/guestv1"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// procRegressHarness is a service rooted under its own temp directory, with
// the unprivileged test user and a no-op freezer.
type procRegressHarness struct {
	t     *testing.T
	svc   *Service
	home  string
	procs string
}

// newProcRegressHarness starts a service and registers a shutdown cleanup.
func newProcRegressHarness(t *testing.T, mod func(*Config)) *procRegressHarness {
	t.Helper()
	base := t.TempDir()
	home := filepath.Join(base, "home/user")
	tmp := filepath.Join(base, "tmp")
	for _, d := range []string{home, tmp} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	cfg := Config{
		Log:         slog.New(slog.DiscardHandler),
		UID:         os.Getuid(),
		GID:         os.Getgid(),
		NoSetGroups: true,
		HomeRoot:    home,
		TmpRoot:     tmp,
		Freezer:     noopFreezer{},
	}
	if mod != nil {
		mod(&cfg)
	}
	svc, err := New(cfg)
	if err != nil {
		t.Fatalf("guest.New: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := svc.Shutdown(ctx); err != nil {
			t.Logf("shutdown: %v", err)
		}
	})
	return &procRegressHarness{
		t:     t,
		svc:   svc,
		home:  home,
		procs: filepath.Join(home, ".bbraft", "processes"),
	}
}

// start calls StartProcess directly and returns the error under test.
func (h *procRegressHarness) start(id uint32, command string) error {
	h.t.Helper()
	_, err := h.svc.StartProcess(h.t.Context(), &guestv1.StartProcessRequest{ProcessId: id, Command: command})
	return err
}

// waitDone blocks until the process record finalizes.
func (h *procRegressHarness) waitDone(id uint32) *nodev1.Process {
	h.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		p := h.svc.lookupProc(id)
		if p == nil {
			h.t.Fatalf("process %d vanished before finalizing", id)
		}
		select {
		case <-p.done:
			return h.svc.processSnapshot(p)
		default:
		}
		if time.Now().After(deadline) {
			h.t.Fatalf("process %d never finalized", id)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// wantInternal fails unless err is an INTERNAL status, matching the guest
// contract for a failed start.
func wantInternal(t *testing.T, err error, what string) {
	t.Helper()
	if status.Code(err) != codes.Internal {
		t.Fatalf("%s = %v, want INTERNAL", what, err)
	}
}

// procRegressFDs counts this process's open descriptors.
func procRegressFDs(t *testing.T) int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	return len(entries)
}

// procRegressInventory asserts the process state directory holds exactly the
// successful process's names and the planted foreign entry, with no
// temporary or orphaned files. It returns nothing; failures are fatal.
func procRegressInventory(t *testing.T, procs, foreign string, id uint32) {
	t.Helper()
	entries, err := os.ReadDir(procs)
	if err != nil {
		t.Fatalf("read process state: %v", err)
	}
	var names []string
	for _, e := range entries {
		switch e.Name() {
		case foreign:
			continue
		case ".bbraft":
			t.Fatalf("unexpected .bbraft entry inside the processes directory")
		default:
			names = append(names, e.Name())
		}
	}
	idName := strconv.FormatUint(uint64(id), 10)
	want := []string{idName, idName + ".err.log", idName + ".log"}
	slices.Sort(names)
	slices.Sort(want)
	if !slices.Equal(names, want) {
		t.Fatalf("process state inventory = %v, want %v (foreign %q must survive)", names, want, foreign)
	}
	sub, err := os.ReadDir(filepath.Join(procs, idName))
	if err != nil {
		t.Fatal(err)
	}
	var inner []string
	for _, e := range sub {
		inner = append(inner, e.Name())
	}
	slices.Sort(inner)
	if wantInner := []string{"exit.json", "meta.json"}; !slices.Equal(inner, wantInner) {
		t.Fatalf("metadata inventory = %v, want %v", inner, wantInner)
	}
	if got, err := os.ReadFile(filepath.Join(procs, foreign)); err != nil || string(got) != "foreign" {
		t.Fatalf("foreign entry = %q, %v; want preserved content", got, err)
	}
}

// plantForeign creates the foreign entry the inventory assertion preserves.
func (h *procRegressHarness) plantForeign() string {
	h.t.Helper()
	if err := os.MkdirAll(h.procs, 0o755); err != nil {
		h.t.Fatal(err)
	}
	const name = "foreign-entry"
	if err := os.WriteFile(filepath.Join(h.procs, name), []byte("foreign"), 0o644); err != nil {
		h.t.Fatal(err)
	}
	return name
}

// TestProcessFrozenLogParentSyncLeak: the stdout log exists before its
// parent sync. A failure there must leave nothing behind and must not
// consume the id — the log is owned from its O_EXCL create on.
func TestProcessFrozenLogParentSyncLeak(t *testing.T) {
	const id = 601
	var armed atomic.Bool
	h := newProcRegressHarness(t, func(c *Config) {
		procs := filepath.Join(c.HomeRoot, ".bbraft", "processes")
		c.SyncFile = func(f *os.File) error {
			if !armed.Load() || !strings.HasSuffix(f.Name(), "/processes") {
				return nil
			}
			if _, err := os.Stat(filepath.Join(procs, strconv.FormatUint(id, 10)+".log")); err == nil {
				return errors.New("injected log parent sync failure")
			}
			return nil
		}
	})
	foreign := h.plantForeign()
	armed.Store(true)
	err := h.start(id, "echo x")
	wantInternal(t, err, "start with a failing log parent sync")
	armed.Store(false)

	// Fixed: the attempt removed every entry it created. Before the fix the
	// untracked stdout log survived and the id stayed wedged.
	if _, statErr := os.Stat(filepath.Join(h.procs, strconv.FormatUint(id, 10)+".log")); !os.IsNotExist(statErr) {
		t.Fatalf("failed start left the untracked stdout log behind: %v", statErr)
	}
	if err := h.start(id, "echo ok"); err != nil {
		t.Fatalf("retry after a failed log creation: %v", err)
	}
	h.waitDone(id)
	procRegressInventory(t, h.procs, foreign, id)
}

// TestProcessFrozenRetainedRollbackHandle: when the rollback cannot prove
// its removal durable, the reservation and the metadata-directory handle are
// kept; the retry must be able to stat through that same handle. Before the
// fix the handle was closed and every retry failed with EBADF.
func TestProcessFrozenRetainedRollbackHandle(t *testing.T) {
	const id = 602
	var failMeta, rollbackBroken atomic.Bool
	var metaSyncs atomic.Int32
	h := newProcRegressHarness(t, func(c *Config) {
		c.SyncFile = func(f *os.File) error {
			switch {
			// Fail the second metadata write (the post-fork replacement):
			// the first write already succeeded, so its final record is in
			// the attempt's inventory with the metadata-directory handle as
			// its parent.
			case failMeta.Load() && strings.Contains(f.Name(), ".tmp.meta.json") && metaSyncs.Add(1) == 2:
				rollbackBroken.Store(true)
				return errors.New("injected metadata sync failure")
			case rollbackBroken.Load() && strings.HasSuffix(f.Name(), "/processes"):
				return errors.New("injected rollback sync failure")
			}
			return nil
		}
	})
	foreign := h.plantForeign()
	failMeta.Store(true)
	err := h.start(id, "echo x")
	wantInternal(t, err, "start with a broken rollback")
	if !strings.Contains(err.Error(), "cleanup also failed") {
		t.Fatalf("first failure = %v, want the failed cleanup named", err)
	}
	p := h.svc.lookupProc(id)
	if p == nil {
		t.Fatal("retained reservation vanished")
	}
	p.mu.Lock()
	dir, rollbackErr := p.dir, p.rollbackErr
	p.mu.Unlock()
	if rollbackErr == nil || dir == nil {
		t.Fatalf("retained rollback handles = dir %v, err %v; want both pinned", dir, rollbackErr)
	}

	failMeta.Store(false)
	rollbackBroken.Store(false)
	if err := h.start(id, "echo after-retention"); err != nil {
		t.Fatalf("retry after the retained rollback: %v", err)
	}
	h.waitDone(id)
	procRegressInventory(t, h.procs, foreign, id)
	p.mu.Lock()
	dir, outLog := p.dir, p.wOut
	p.mu.Unlock()
	if dir != nil || outLog != nil {
		t.Fatal("retained attempt handles were not closed exactly once after the durable removal")
	}
}

// TestProcessFrozenSharedStateDirParentSync: a shared state-directory
// parent-sync failure during New's first-use walk leaves the directory
// created but not durable. The retry's mkdir reports EEXIST, and it must
// still re-sync that parent entry before any process may start. Before the
// fix an existing component skipped the barrier entirely and a process
// reported durable state in a directory whose entry was never synced.
func TestProcessFrozenSharedStateDirParentSync(t *testing.T) {
	cases := []struct {
		name   string
		id     uint32
		target func(home string) string
	}{
		{"home-to-bbraft", 711, func(home string) string { return home }},
		{"bbraft-to-processes", 712, func(home string) string { return filepath.Join(home, ".bbraft") }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var armed atomic.Bool
			armed.Store(true)
			var fired atomic.Bool
			var mu sync.Mutex
			var synced []string
			var home string
			h := newProcRegressHarness(t, func(c *Config) {
				home = c.HomeRoot
				c.SyncFile = func(f *os.File) error {
					mu.Lock()
					synced = append(synced, filepath.Clean(f.Name()))
					mu.Unlock()
					if armed.Load() && filepath.Clean(f.Name()) == filepath.Clean(tc.target(c.HomeRoot)) {
						if fired.CompareAndSwap(false, true) {
							return errors.New("injected shared-state parent sync failure")
						}
					}
					return f.Sync()
				}
			})
			if !fired.Load() {
				t.Fatalf("New never reached the %s parent sync", tc.name)
			}
			armed.Store(false)
			mu.Lock()
			synced = nil
			mu.Unlock()

			if err := h.start(tc.id, "echo ok"); err != nil {
				t.Fatalf("first use after the %s failure: %v", tc.name, err)
			}
			h.waitDone(tc.id)
			mu.Lock()
			got := slices.Clone(synced)
			mu.Unlock()
			want := filepath.Clean(tc.target(home))
			if !slices.Contains(got, want) {
				t.Fatalf("retry did not re-sync %s after EEXIST: synced %q", want, got)
			}
			foreign := h.plantForeign()
			procRegressInventory(t, h.procs, foreign, tc.id)
		})
	}
}

// TestProcessFrozenKillGroupRealESRCH: kill(2) answering ESRCH for a process
// group with no members is not a signal sent. Before the fix the error was
// dropped and Signaled was set.
func TestProcessFrozenKillGroupRealESRCH(t *testing.T) {
	s := &Service{log: slog.New(slog.DiscardHandler), syncFile: func(*os.File) error { return nil }}
	// A pgid above every Linux pid_max cannot have members.
	p := &detachedProc{
		rec:  &nodev1.Process{State: nodev1.ProcessState_PROCESS_STATE_RUNNING},
		pgid: 1 << 30,
	}
	sent, err := s.killGroup(t.Context(), p, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("killGroup on an absent group = %v, want nil once the scan agrees", err)
	}
	if sent {
		t.Fatal("killGroup reported a signal for a group with no members")
	}
}
