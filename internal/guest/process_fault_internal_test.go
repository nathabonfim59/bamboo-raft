// Exhaustive fault-injection table for detached-process creation and
// rollback. Every post-create boundary is failed exactly once through
// service-scoped seams installed before the service does any work; each
// hook consults atomic fault state, so restoring an injection never swaps a
// function table underneath a live supervisor. Each row asserts the exact
// stage was reached, restores the fault, retries the same id successfully,
// then checks the final inventory is exact (no temporary or orphaned
// entries) and a planted foreign entry survived. Retained-rollback rows also
// prove the pinned inventory handles stay usable until the durable removal.
package guest

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/nathabonfim59/bamboo-raft/internal/proto/guestv1"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// procFaultID is the id every table row starts; each row gets a fresh
// service and directory, so the id can be reused across rows.
const procFaultID = 701

var errProcFault = errors.New("injected fault")

// procFaultRow is one failure stage. install wires the fault before the
// start attempt and returns the restore function; retained marks a row whose
// rollback must keep its pinned handles for the retry.
type procFaultRow struct {
	name     string
	retained bool
	install  func(h *procRegressHarness, fired *atomic.Bool) (restore func())
}

// procFaultOnce fails exactly the first call that reaches it.
func procFaultOnce(fired *atomic.Bool, err error) error {
	if fired.CompareAndSwap(false, true) {
		return err
	}
	return nil
}

// procLogNamesFor is the table's log-name helper.
func procLogNamesFor(id uint32) (out, errLog string) {
	s := strconv.FormatUint(uint64(id), 10)
	return s + ".log", s + ".err.log"
}

// TestProcessFaultInjectionEveryPostCreateStep drives the table.
func TestProcessFaultInjectionEveryPostCreateStep(t *testing.T) {
	idName := strconv.FormatUint(procFaultID, 10)
	outName, errName := procLogNamesFor(procFaultID)

	rows := []procFaultRow{
		{
			name: "mkdirat",
			install: func(h *procRegressHarness, fired *atomic.Bool) func() {
				var off atomic.Bool
				h.svc.procFS.mkdirat = func(dir *os.File, name string, mode uint32) error {
					if !off.Load() && name == idName {
						if err := procFaultOnce(fired, errProcFault); err != nil {
							return err
						}
					}
					return unix.Mkdirat(int(dir.Fd()), name, mode)
				}
				return func() { off.Store(true) }
			},
		},
		{
			name: "metadata-dir-open",
			install: func(h *procRegressHarness, fired *atomic.Bool) func() {
				var off atomic.Bool
				h.svc.procFS.openat = func(dir *os.File, name string, flags int, mode uint32) (*os.File, error) {
					if !off.Load() && name == idName && flags&unix.O_DIRECTORY != 0 {
						if err := procFaultOnce(fired, errProcFault); err != nil {
							return nil, err
						}
					}
					return openAt(dir, name, flags, mode)
				}
				return func() { off.Store(true) }
			},
		},
		{
			name: "metadata-dir-chown",
			install: func(h *procRegressHarness, fired *atomic.Bool) func() {
				var off atomic.Bool
				h.svc.procFS.chown = func(f *os.File, uid, gid int) error {
					if !off.Load() && strings.HasSuffix(f.Name(), "/"+idName) {
						if err := procFaultOnce(fired, errProcFault); err != nil {
							return err
						}
					}
					return f.Chown(uid, gid)
				}
				return func() { off.Store(true) }
			},
		},
		{
			name: "metadata-dir-sync",
			install: func(h *procRegressHarness, fired *atomic.Bool) func() {
				var off atomic.Bool
				h.svc.syncFile = func(f *os.File) error {
					if !off.Load() && strings.HasSuffix(f.Name(), "/processes") {
						_, outErr := os.Stat(filepath.Join(h.procs, outName))
						_, errErr := os.Stat(filepath.Join(h.procs, errName))
						if os.IsNotExist(outErr) && os.IsNotExist(errErr) {
							if err := procFaultOnce(fired, errProcFault); err != nil {
								return err
							}
						}
					}
					return f.Sync()
				}
				return func() { off.Store(true) }
			},
		},
		{
			name: "stdout-log-open",
			install: func(h *procRegressHarness, fired *atomic.Bool) func() {
				var off atomic.Bool
				h.svc.procFS.openat = func(dir *os.File, name string, flags int, mode uint32) (*os.File, error) {
					if !off.Load() && name == outName && flags&unix.O_EXCL != 0 {
						if err := procFaultOnce(fired, errProcFault); err != nil {
							return nil, err
						}
					}
					return openAt(dir, name, flags, mode)
				}
				return func() { off.Store(true) }
			},
		},
		{
			name: "stdout-log-chown",
			install: func(h *procRegressHarness, fired *atomic.Bool) func() {
				var off atomic.Bool
				h.svc.procFS.chown = func(f *os.File, uid, gid int) error {
					if !off.Load() && strings.HasSuffix(f.Name(), "/"+outName) {
						if err := procFaultOnce(fired, errProcFault); err != nil {
							return err
						}
					}
					return f.Chown(uid, gid)
				}
				return func() { off.Store(true) }
			},
		},
		{
			name: "stdout-log-chmod",
			install: func(h *procRegressHarness, fired *atomic.Bool) func() {
				var off atomic.Bool
				h.svc.procFS.chmod = func(f *os.File, mode os.FileMode) error {
					if !off.Load() && strings.HasSuffix(f.Name(), "/"+outName) {
						if err := procFaultOnce(fired, errProcFault); err != nil {
							return err
						}
					}
					return f.Chmod(mode)
				}
				return func() { off.Store(true) }
			},
		},
		{
			name: "stdout-log-parent-sync",
			install: func(h *procRegressHarness, fired *atomic.Bool) func() {
				var off atomic.Bool
				h.svc.syncFile = func(f *os.File) error {
					if !off.Load() && strings.HasSuffix(f.Name(), "/processes") {
						_, outErr := os.Stat(filepath.Join(h.procs, outName))
						_, errErr := os.Stat(filepath.Join(h.procs, errName))
						if outErr == nil && os.IsNotExist(errErr) {
							if err := procFaultOnce(fired, errProcFault); err != nil {
								return err
							}
						}
					}
					return f.Sync()
				}
				return func() { off.Store(true) }
			},
		},
		{
			name: "stderr-log-open",
			install: func(h *procRegressHarness, fired *atomic.Bool) func() {
				var off atomic.Bool
				h.svc.procFS.openat = func(dir *os.File, name string, flags int, mode uint32) (*os.File, error) {
					if !off.Load() && name == errName && flags&unix.O_EXCL != 0 {
						if err := procFaultOnce(fired, errProcFault); err != nil {
							return nil, err
						}
					}
					return openAt(dir, name, flags, mode)
				}
				return func() { off.Store(true) }
			},
		},
		{
			name: "stderr-log-chown",
			install: func(h *procRegressHarness, fired *atomic.Bool) func() {
				var off atomic.Bool
				h.svc.procFS.chown = func(f *os.File, uid, gid int) error {
					if !off.Load() && strings.HasSuffix(f.Name(), "/"+errName) {
						if err := procFaultOnce(fired, errProcFault); err != nil {
							return err
						}
					}
					return f.Chown(uid, gid)
				}
				return func() { off.Store(true) }
			},
		},
		{
			name: "stderr-log-chmod",
			install: func(h *procRegressHarness, fired *atomic.Bool) func() {
				var off atomic.Bool
				h.svc.procFS.chmod = func(f *os.File, mode os.FileMode) error {
					if !off.Load() && strings.HasSuffix(f.Name(), "/"+errName) {
						if err := procFaultOnce(fired, errProcFault); err != nil {
							return err
						}
					}
					return f.Chmod(mode)
				}
				return func() { off.Store(true) }
			},
		},
		{
			name: "stderr-log-parent-sync",
			install: func(h *procRegressHarness, fired *atomic.Bool) func() {
				var off atomic.Bool
				h.svc.syncFile = func(f *os.File) error {
					if !off.Load() && strings.HasSuffix(f.Name(), "/processes") {
						_, outErr := os.Stat(filepath.Join(h.procs, outName))
						_, errErr := os.Stat(filepath.Join(h.procs, errName))
						if outErr == nil && errErr == nil {
							if err := procFaultOnce(fired, errProcFault); err != nil {
								return err
							}
						}
					}
					return f.Sync()
				}
				return func() { off.Store(true) }
			},
		},
		{
			name: "meta-temp-open",
			install: func(h *procRegressHarness, fired *atomic.Bool) func() {
				var off atomic.Bool
				h.svc.procFS.openat = func(dir *os.File, name string, flags int, mode uint32) (*os.File, error) {
					if !off.Load() && strings.HasPrefix(name, ".tmp.meta.json") {
						if err := procFaultOnce(fired, errProcFault); err != nil {
							return nil, err
						}
					}
					return openAt(dir, name, flags, mode)
				}
				return func() { off.Store(true) }
			},
		},
		{
			name: "meta-temp-write",
			install: func(h *procRegressHarness, fired *atomic.Bool) func() {
				var off atomic.Bool
				h.svc.procFS.write = func(f *os.File, b []byte) (int, error) {
					if !off.Load() && strings.Contains(f.Name(), ".tmp.meta.json") {
						if err := procFaultOnce(fired, errProcFault); err != nil {
							return 0, err
						}
					}
					return f.Write(b)
				}
				return func() { off.Store(true) }
			},
		},
		{
			name: "meta-temp-sync",
			install: func(h *procRegressHarness, fired *atomic.Bool) func() {
				var off atomic.Bool
				h.svc.syncFile = func(f *os.File) error {
					if !off.Load() && strings.Contains(f.Name(), ".tmp.meta.json") {
						if err := procFaultOnce(fired, errProcFault); err != nil {
							return err
						}
					}
					return f.Sync()
				}
				return func() { off.Store(true) }
			},
		},
		{
			name: "meta-temp-close",
			install: func(h *procRegressHarness, fired *atomic.Bool) func() {
				var off atomic.Bool
				h.svc.procFS.close = func(f *os.File) error {
					if !off.Load() && strings.Contains(f.Name(), ".tmp.meta.json") {
						_ = f.Close() // closed either way; the reported failure is injected
						if err := procFaultOnce(fired, errProcFault); err != nil {
							return err
						}
						return nil
					}
					return f.Close()
				}
				return func() { off.Store(true) }
			},
		},
		{
			name: "meta-rename",
			install: func(h *procRegressHarness, fired *atomic.Bool) func() {
				var off atomic.Bool
				h.svc.procFS.renameat = func(dir *os.File, name, newName string) error {
					if !off.Load() && strings.HasPrefix(name, ".tmp.meta.json") {
						if err := procFaultOnce(fired, errProcFault); err != nil {
							return err
						}
					}
					return renameAt(dir, name, newName)
				}
				return func() { off.Store(true) }
			},
		},
		{
			name: "meta-dir-sync-after-rename",
			install: func(h *procRegressHarness, fired *atomic.Bool) func() {
				var off atomic.Bool
				h.svc.syncFile = func(f *os.File) error {
					if !off.Load() && strings.HasSuffix(f.Name(), "/"+idName) {
						if err := procFaultOnce(fired, errProcFault); err != nil {
							return err
						}
					}
					return f.Sync()
				}
				return func() { off.Store(true) }
			},
		},
		{
			name: "post-fork-meta-replacement",
			install: func(h *procRegressHarness, fired *atomic.Bool) func() {
				var off atomic.Bool
				var opens atomic.Int32
				h.svc.procFS.openat = func(dir *os.File, name string, flags int, mode uint32) (*os.File, error) {
					if !off.Load() && strings.HasPrefix(name, ".tmp.meta.json") && opens.Add(1) == 2 {
						if err := procFaultOnce(fired, errProcFault); err != nil {
							return nil, err
						}
					}
					return openAt(dir, name, flags, mode)
				}
				return func() { off.Store(true) }
			},
		},
		{
			name: "post-fork-meta-write",
			install: func(h *procRegressHarness, fired *atomic.Bool) func() {
				var off atomic.Bool
				var writes atomic.Int32
				h.svc.procFS.write = func(f *os.File, b []byte) (int, error) {
					if !off.Load() && strings.Contains(f.Name(), ".tmp.meta.json") && writes.Add(1) == 2 {
						if err := procFaultOnce(fired, errProcFault); err != nil {
							return 0, err
						}
					}
					return f.Write(b)
				}
				return func() { off.Store(true) }
			},
		},
		{
			name: "post-fork-meta-sync-before-rename",
			install: func(h *procRegressHarness, fired *atomic.Bool) func() {
				var off atomic.Bool
				var syncs atomic.Int32
				h.svc.syncFile = func(f *os.File) error {
					if !off.Load() && strings.Contains(f.Name(), ".tmp.meta.json") && syncs.Add(1) == 2 {
						if err := procFaultOnce(fired, errProcFault); err != nil {
							return err
						}
					}
					return f.Sync()
				}
				return func() { off.Store(true) }
			},
		},
		{
			name: "post-fork-meta-close-before-rename",
			install: func(h *procRegressHarness, fired *atomic.Bool) func() {
				var off atomic.Bool
				var closes atomic.Int32
				h.svc.procFS.close = func(f *os.File) error {
					if !off.Load() && strings.Contains(f.Name(), ".tmp.meta.json") && closes.Add(1) == 2 {
						_ = f.Close()
						if err := procFaultOnce(fired, errProcFault); err != nil {
							return err
						}
						return nil
					}
					return f.Close()
				}
				return func() { off.Store(true) }
			},
		},
		{
			name: "post-fork-meta-rename",
			install: func(h *procRegressHarness, fired *atomic.Bool) func() {
				var off atomic.Bool
				var renames atomic.Int32
				h.svc.procFS.renameat = func(dir *os.File, name, newName string) error {
					if !off.Load() && strings.HasPrefix(name, ".tmp.meta.json") && renames.Add(1) == 2 {
						if err := procFaultOnce(fired, errProcFault); err != nil {
							return err
						}
					}
					return renameAt(dir, name, newName)
				}
				return func() { off.Store(true) }
			},
		},
		{
			name: "post-fork-meta-dir-sync-after-rename",
			install: func(h *procRegressHarness, fired *atomic.Bool) func() {
				var off atomic.Bool
				var syncs atomic.Int32
				h.svc.syncFile = func(f *os.File) error {
					if !off.Load() && strings.HasSuffix(f.Name(), "/"+idName) && syncs.Add(1) == 2 {
						if err := procFaultOnce(fired, errProcFault); err != nil {
							return err
						}
					}
					return f.Sync()
				}
				return func() { off.Store(true) }
			},
		},
		{
			name:     "rollback-unlink",
			retained: true,
			install: func(h *procRegressHarness, fired *atomic.Bool) func() {
				var off, primary atomic.Bool
				h.svc.syncFile = func(f *os.File) error {
					if !off.Load() && strings.Contains(f.Name(), ".tmp.meta.json") && primary.CompareAndSwap(false, true) {
						return errProcFault
					}
					return f.Sync()
				}
				h.svc.procFS.unlinkat = func(dir *os.File, name string, flags int) error {
					if !off.Load() && primary.Load() && name == outName {
						if err := procFaultOnce(fired, errProcFault); err != nil {
							return err
						}
					}
					return unix.Unlinkat(int(dir.Fd()), name, flags)
				}
				return func() { off.Store(true) }
			},
		},
		{
			name:     "rollback-directory-sync",
			retained: true,
			install: func(h *procRegressHarness, fired *atomic.Bool) func() {
				var off, primary atomic.Bool
				h.svc.syncFile = func(f *os.File) error {
					if !off.Load() && strings.Contains(f.Name(), ".tmp.meta.json") && primary.CompareAndSwap(false, true) {
						return errProcFault
					}
					if !off.Load() && primary.Load() && strings.HasSuffix(f.Name(), "/processes") {
						if err := procFaultOnce(fired, errProcFault); err != nil {
							return err
						}
					}
					return f.Sync()
				}
				return func() { off.Store(true) }
			},
		},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			h := newProcRegressHarness(t, nil)
			foreign := h.plantForeign()
			before := procRegressFDs(t)
			var fired atomic.Bool
			restore := row.install(h, &fired)

			err := h.start(procFaultID, "echo ok")
			wantInternal(t, err, "start with the "+row.name+" fault")
			if !fired.Load() {
				t.Fatalf("the %s fault never reached its exact stage: %v", row.name, err)
			}

			var retained *detachedProc
			if row.retained {
				retained = h.svc.lookupProc(procFaultID)
				if retained == nil {
					t.Fatal("retained reservation vanished")
				}
				retained.mu.Lock()
				dir, rbErr := retained.dir, retained.rollbackErr
				retained.mu.Unlock()
				if rbErr == nil || dir == nil {
					t.Fatalf("retained rollback handles = dir %v, err %v; want both pinned", dir, rbErr)
				}
			}

			// Fault removed by atomic state: the same id starts fresh.
			restore()
			if err := h.start(procFaultID, "echo ok"); err != nil {
				t.Fatalf("retry after the %s fault: %v", row.name, err)
			}
			h.waitDone(procFaultID)
			procRegressInventory(t, h.procs, foreign, procFaultID)

			if retained != nil {
				retained.mu.Lock()
				dir, outLog := retained.dir, retained.wOut
				retained.mu.Unlock()
				if dir != nil || outLog != nil {
					t.Fatalf("retained handles after the durable retry = dir %v, out %v; want closed once", dir, outLog)
				}
			}
			time.Sleep(50 * time.Millisecond)
			if delta := procRegressFDs(t) - before; delta > 2 {
				t.Fatalf("%s leaked %d descriptors", row.name, delta)
			}
		})
	}
}

// TestProcessKillGroupInjectedErrorPropagates: a real signaling error other
// than ESRCH must come back as an error, never as Signaled=true.
func TestProcessKillGroupInjectedErrorPropagates(t *testing.T) {
	pgid := procSpawnGroup(t)
	s := procKillTestService()
	s.procFS.kill = func(pgid int, sig syscall.Signal) error {
		return fmt.Errorf("kill(%d, %s): %w", pgid, sig, syscall.EPERM)
	}
	p := &detachedProc{rec: &nodev1.Process{State: nodev1.ProcessState_PROCESS_STATE_RUNNING}, pgid: pgid}
	sent, err := s.killGroup(t.Context(), p, 50*time.Millisecond)
	if !errors.Is(err, syscall.EPERM) {
		t.Fatalf("killGroup with an EPERM signal = %v, want EPERM", err)
	}
	if sent {
		t.Fatal("killGroup reported a signal sent although the kernel refused it")
	}
}

// TestProcessKillGroupESRCHScanDisagrees: ESRCH is only trusted as "ended"
// when the group scan agrees. A scan that still sees members makes the kill
// an error, never a silent success.
func TestProcessKillGroupESRCHScanDisagrees(t *testing.T) {
	pgid := procSpawnGroup(t)
	s := procKillTestService()
	s.groupScan = func(int, bool) bool { return true }
	s.procFS.kill = func(int, syscall.Signal) error { return syscall.ESRCH }
	p := &detachedProc{rec: &nodev1.Process{State: nodev1.ProcessState_PROCESS_STATE_RUNNING}, pgid: pgid}
	sent, err := s.killGroup(t.Context(), p, 50*time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "ESRCH") {
		t.Fatalf("killGroup ESRCH with a live scan = %v, want an ESRCH disagreement error", err)
	}
	if sent {
		t.Fatal("killGroup reported a signal sent for an ESRCH")
	}
}

// newProcSharedDirService builds a minimal Service over an existing home
// tree, so the shared state-directory walk can be faulted directly.
func newProcSharedDirService(t *testing.T, home string, pfs procFS) *Service {
	t.Helper()
	homeRoot, err := os.OpenRoot(home)
	if err != nil {
		t.Fatal(err)
	}
	tmpRoot, err := os.OpenRoot(t.TempDir())
	if err != nil {
		homeRoot.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		homeRoot.Close()
		tmpRoot.Close()
	})
	return &Service{
		log:      slog.New(slog.DiscardHandler),
		uid:      os.Getuid(),
		gid:      os.Getgid(),
		homeDir:  home,
		homeRoot: homeRoot,
		tmpRoot:  tmpRoot,
		syncFile: func(f *os.File) error { return f.Sync() },
		procFS:   pfs,
		procs:    map[uint32]*detachedProc{},
	}
}

// TestProcessSharedStateDirOwnershipRepairedOnEEXIST: a chown failure during
// the shared state-directory walk leaves the component created under the
// wrong owner. The retry's mkdir reports EEXIST, but ownership must be
// re-applied before the walk succeeds — skipping it would leave state the
// sandbox user cannot read or write.
func TestProcessSharedStateDirOwnershipRepairedOnEEXIST(t *testing.T) {
	for _, target := range []string{".bbraft", "processes"} {
		t.Run(target, func(t *testing.T) {
			home := t.TempDir()
			if err := os.MkdirAll(filepath.Join(home, ".bbraft", "processes"), 0o755); err != nil {
				t.Fatal(err)
			}
			var fail atomic.Bool
			fail.Store(true)
			var mu sync.Mutex
			var chowned []string
			pfs := procFS{chown: func(f *os.File, uid, gid int) error {
				name := filepath.Base(f.Name())
				mu.Lock()
				chowned = append(chowned, name)
				mu.Unlock()
				if fail.Load() && name == target {
					fail.Store(false)
					return errProcFault
				}
				return f.Chown(uid, gid)
			}}
			svc := newProcSharedDirService(t, home, pfs)
			if _, err := svc.procStateDir(); err == nil {
				t.Fatalf("the %s chown fault was never reached", target)
			}
			if _, err := svc.procStateDir(); err != nil {
				t.Fatalf("retry after the %s chown failure: %v", target, err)
			}
			mu.Lock()
			got := slices.Clone(chowned)
			mu.Unlock()
			first := slices.Index(got, target)
			if first < 0 || slices.Index(got[first+1:], target) < 0 {
				t.Fatalf("%s chown was not re-applied on EEXIST: calls %q", target, got)
			}
		})
	}
}

// TestProcessFailedStartCleanupBounded: a child whose group cannot be proven
// dead must not block the start RPC behind an unbounded reap. The lying scan
// forces killGroup to its context bound, the attempt is retained, and once
// the truth is restored the same id retries cleanly.
func TestProcessFailedStartCleanupBounded(t *testing.T) {
	const id = 702
	h := newProcRegressHarness(t, nil)
	foreign := h.plantForeign()
	var armed, lie atomic.Bool
	armed.Store(true)
	lie.Store(true)
	var metaOpens atomic.Int32
	h.svc.procFS.openat = func(dir *os.File, name string, flags int, mode uint32) (*os.File, error) {
		if armed.Load() && strings.HasPrefix(name, ".tmp.meta.json") && metaOpens.Add(1) == 2 {
			return nil, errProcFault
		}
		return openAt(dir, name, flags, mode)
	}
	h.svc.groupScan = func(int, bool) bool { return lie.Load() }

	start := time.Now()
	err := h.start(id, "sleep 300")
	elapsed := time.Since(start)
	wantInternal(t, err, "start whose failed child cannot be proven dead")
	if elapsed < 4*time.Second {
		t.Fatalf("cleanup gave up after %v, before exercising the bounded kill", elapsed)
	}
	if elapsed > procAbortBound+2*time.Second {
		t.Fatalf("cleanup took %v, above the %v bound", elapsed, procAbortBound)
	}
	retained := h.svc.lookupProc(id)
	if retained == nil {
		t.Fatal("bounded cleanup did not retain the reservation")
	}
	retained.mu.Lock()
	rbErr, dir := retained.rollbackErr, retained.dir
	retained.mu.Unlock()
	if rbErr == nil || dir == nil {
		t.Fatalf("bounded cleanup retained err %v, dir %v; want both", rbErr, dir)
	}

	// Truth restored: the group was killed by the bounded attempt and the
	// owner reaps it, so the retry can prove death and start fresh.
	lie.Store(false)
	armed.Store(false)
	if err := h.start(id, "echo ok"); err != nil {
		t.Fatalf("retry after bounded cleanup: %v", err)
	}
	h.waitDone(id)
	procRegressInventory(t, h.procs, foreign, id)
}

// TestProcessFailedStartKillErrorRetains: when the failed start's kill is
// refused, the child is still alive. The attempt must retain its state and
// reservation, return promptly, and retry only after the test proves death.
func TestProcessFailedStartKillErrorRetains(t *testing.T) {
	const id = 703
	h := newProcRegressHarness(t, nil)
	foreign := h.plantForeign()
	var armed atomic.Bool
	armed.Store(true)
	var metaOpens atomic.Int32
	h.svc.procFS.openat = func(dir *os.File, name string, flags int, mode uint32) (*os.File, error) {
		if armed.Load() && strings.HasPrefix(name, ".tmp.meta.json") && metaOpens.Add(1) == 2 {
			return nil, errProcFault
		}
		return openAt(dir, name, flags, mode)
	}
	var refused atomic.Bool
	h.svc.procFS.kill = func(pgid int, sig syscall.Signal) error {
		if refused.CompareAndSwap(false, true) {
			return syscall.EPERM
		}
		return syscall.Kill(pgid, sig)
	}

	start := time.Now()
	err := h.start(id, "sleep 300")
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("failed-start cleanup with a refused kill took %v; want bounded", elapsed)
	}
	wantInternal(t, err, "start whose failed child could not be signalled")
	retained := h.svc.lookupProc(id)
	if retained == nil {
		t.Fatal("refused-kill cleanup did not retain the reservation")
	}
	retained.mu.Lock()
	rbErr, pgid := retained.rollbackErr, retained.pgid
	retained.mu.Unlock()
	if rbErr == nil || pgid <= 0 {
		t.Fatalf("retained refused-kill attempt = err %v, pgid %d; want both", rbErr, pgid)
	}
	if err := syscall.Kill(pgid, 0); err != nil {
		t.Fatalf("child of the refused kill is not alive: %v", err)
	}
	t.Cleanup(func() { _ = syscall.Kill(-pgid, syscall.SIGKILL) })

	// The child dies for real; the retry then proves death and starts fresh.
	if err := syscall.Kill(-pgid, syscall.SIGKILL); err != nil {
		t.Fatalf("killing the retained child: %v", err)
	}
	armed.Store(false)
	if err := h.start(id, "echo ok"); err != nil {
		t.Fatalf("retry after the refused-kill retention: %v", err)
	}
	h.waitDone(id)
	procRegressInventory(t, h.procs, foreign, id)
}

// TestProcessFailedStartPausedSignalAfterFinalReap is the deterministic
// ownership race: the failed start's kill is parked between its running check
// and its signal while the leader has exited and its last descendant is
// ending. The owner proves the group empty and reaps under the signal lock;
// the resumed kill must find ownership ended and send nothing, never a
// signal to a possibly reused pgid.
func TestProcessFailedStartPausedSignalAfterFinalReap(t *testing.T) {
	const id = 704
	h := newProcRegressHarness(t, nil)
	foreign := h.plantForeign()
	var armed atomic.Bool
	armed.Store(true)
	var metaOpens atomic.Int32
	h.svc.procFS.openat = func(dir *os.File, name string, flags int, mode uint32) (*os.File, error) {
		if armed.Load() && strings.HasPrefix(name, ".tmp.meta.json") && metaOpens.Add(1) == 2 {
			return nil, errProcFault
		}
		return openAt(dir, name, flags, mode)
	}
	var kills atomic.Int32
	h.svc.procFS.kill = func(pgid int, sig syscall.Signal) error {
		kills.Add(1)
		return syscall.Kill(pgid, sig)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	h.svc.preSignal = func() {
		once.Do(func() { close(entered) })
		<-release
	}

	errCh := make(chan error, 1)
	go func() { errCh <- h.start(id, "( sleep 0.4 ) & exit 0") }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the failed start's kill never reached its paused signal")
	}
	p := h.svc.lookupProc(id)
	if p == nil {
		t.Fatal("process vanished while its kill was parked")
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		p.mu.Lock()
		reaped := p.reaped
		p.mu.Unlock()
		if reaped {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the failed-start owner never reaped after the group ended")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if n := kills.Load(); n != 0 {
		t.Fatalf("the parked kill sent %d signal(s) after finalization; want none", n)
	}
	close(release)
	select {
	case err := <-errCh:
		wantInternal(t, err, "paused-signal failed start")
	case <-time.After(5 * time.Second):
		t.Fatal("the failed start never returned after the parked kill resumed")
	}
	armed.Store(false)
	h.svc.preSignal = nil
	if err := h.start(id, "echo ok"); err != nil {
		t.Fatalf("retry after the paused-signal race: %v", err)
	}
	h.waitDone(id)
	procRegressInventory(t, h.procs, foreign, id)
}

// TestProcessRetainedRollbackConcurrentRetries: concurrent same-id retries
// after the fault is removed serialize on the rollback lock, so one cannot
// close the inventory handles underneath another; all callers converge on
// the one fresh process.
func TestProcessRetainedRollbackConcurrentRetries(t *testing.T) {
	const id = 705
	var failMeta, rollbackBroken atomic.Bool
	var metaSyncs atomic.Int32
	h := newProcRegressHarness(t, nil)
	foreign := h.plantForeign()
	h.svc.syncFile = func(f *os.File) error {
		switch {
		// Fail the post-fork replacement, after the first record is in the
		// inventory: the retained cleanup needs the metadata-directory
		// handle, which concurrent retries must not close underneath each
		// other.
		case failMeta.Load() && strings.Contains(f.Name(), ".tmp.meta.json") && metaSyncs.Add(1) == 2:
			rollbackBroken.Store(true)
			return errProcFault
		case rollbackBroken.Load() && strings.HasSuffix(f.Name(), "/processes"):
			return errProcFault
		}
		return f.Sync()
	}
	failMeta.Store(true)
	err := h.start(id, "echo x")
	wantInternal(t, err, "start with a broken rollback")
	if !strings.Contains(err.Error(), "cleanup also failed") {
		t.Fatalf("first failure = %v, want the failed cleanup named", err)
	}
	failMeta.Store(false)
	rollbackBroken.Store(false)

	const callers = 4
	var wg sync.WaitGroup
	errs := make([]error, callers)
	pids := make([]int32, callers)
	for i := range callers {
		wg.Go(func() {
			p, err := h.svc.StartProcess(t.Context(), &guestv1.StartProcessRequest{
				ProcessId: id, Command: "echo converged",
			})
			errs[i] = err
			if err == nil {
				pids[i] = p.GetPid()
			}
		})
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("concurrent retry %d: %v", i, err)
		}
	}
	slices.Sort(pids)
	if pids[0] <= 0 || pids[0] != pids[callers-1] {
		t.Fatalf("concurrent retries produced pids %v; want one process", pids)
	}
	h.waitDone(id)
	procRegressInventory(t, h.procs, foreign, id)
}
