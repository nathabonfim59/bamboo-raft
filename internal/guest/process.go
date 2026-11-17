// Detached guest processes (Phase 1): a process runs `bash -lc` like Exec
// but in its own session and process group, independent of any RPC, with
// stdout and stderr appended to durable logs under the home root:
//
//	<home>/.bbraft/processes/<id>.log      stdout log, append-only
//	<home>/.bbraft/processes/<id>.err.log  stderr log, append-only
//	<home>/.bbraft/processes/<id>/         private metadata directory
//	  meta.json   identity, durable before bash starts
//	  exit.json   final status, durable only after both logs are synced
//
// Every state path is reached through a directory handle opened without
// following symlinks: the sandbox user owns the home and could plant
// symlinks at any of these names. A start attempt owns exactly the state
// it created — tracked by inode — and never deletes anything else; a
// failed attempt rolls back durably or keeps its id reservation until the
// cleanup is proven. At startup the metadata directories are scanned: valid
// metadata plus a valid exit record recovers as EXITED, valid metadata
// without one as LOST (it was running when this service instance, or the
// VM, stopped), and anything that does not validate is invisible. PIDs
// recorded on disk are never signaled: after a restart the live handle is
// gone, and a reused PID must not receive a stray signal.
package guest

import (
	"cmp"
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/nathabonfim59/bamboo-raft/internal/proto/guestv1"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// Process state names inside the processes directory. The two log names
// are the guest paths v1 API consumers see in Process.log_path and
// err_log_path; the per-id metadata directory is private service state.
const (
	procDirRel   = ".bbraft/processes"
	procMetaName = "meta.json"
	procExitName = "exit.json"

	// procJSONBound is the one size bound for a metadata record, enforced
	// identically at write and read: the write refuses anything larger, so
	// every record this service accepts always reads back after a restart.
	// One MiB is above Linux's per-argument limit (128 KiB), so any command
	// the kernel accepts fits with its JSON overhead.
	procJSONBound = 1 << 20

	// procStateVersion is the on-disk record version this build writes and
	// the only one it accepts.
	procStateVersion = 1

	// defaultKillGraceSec is KillProcess's grace period when the request
	// omits grace_seconds.
	defaultKillGraceSec = 5

	// defaultKillProve bounds how long killGroup waits to prove a SIGKILLed
	// group is really gone before giving up without claiming success.
	defaultKillProve = 10 * time.Second

	// procGroupPoll is how often the supervisor re-checks a group whose
	// leader already exited.
	procGroupPoll = 25 * time.Millisecond

	// procEmptyConfirmations is how many consecutive empty scans the
	// supervisor requires before declaring a group ended: a single scan
	// that misses a member (a /proc read racing an exec) must not end
	// ownership early.
	procEmptyConfirmations = 3

	// procAbortKillGrace is the TERM grace a failed start gives its child
	// before SIGKILL. procAbortBound bounds the whole kill, so a child that
	// cannot be proven dead never blocks the RPC. procReapBound bounds the
	// leader reap once the group is proven empty.
	procAbortKillGrace = 2 * time.Second
	procAbortBound     = 5 * time.Second
	procReapBound      = 2 * time.Second
)

// errGroupUnproven reports a kill that sent its signals but cannot prove
// the group ended; callers must not report success for it.
var errGroupUnproven = errors.New("still has members after SIGKILL")

// errOwnershipEnded reports that the process finalized between a kill's
// running check and its signal: the group is no longer ours to signal.
var errOwnershipEnded = errors.New("process finalized before the signal")

// errGroupGone reports that the kernel found no member of the process group
// to signal (ESRCH). Callers confirm it with a scan before treating the
// group as ended, so a group that still has visible members is an error,
// never a silent success.
var errGroupGone = errors.New("process group has no members")

// procFS is the service's narrow seam table for the filesystem and signal
// calls process-state creation, rollback and group kill depend on. Every nil
// field is the real syscall, so the zero value keeps production behaviour;
// tests set individual fields on one Service (never package state) to fail
// exactly one boundary and restore it.
type procFS struct {
	openat   func(dir *os.File, name string, flags int, mode uint32) (*os.File, error)
	mkdirat  func(dir *os.File, name string, mode uint32) error
	chown    func(f *os.File, uid, gid int) error
	chmod    func(f *os.File, mode os.FileMode) error
	write    func(f *os.File, p []byte) (int, error)
	renameat func(dir *os.File, name, newName string) error
	unlinkat func(dir *os.File, name string, flags int) error
	close    func(f *os.File) error
	kill     func(pgid int, sig syscall.Signal) error
}

func (s *Service) procOpenat(dir *os.File, name string, flags int, mode uint32) (*os.File, error) {
	if s.procFS.openat != nil {
		return s.procFS.openat(dir, name, flags, mode)
	}
	return openAt(dir, name, flags, mode)
}

func (s *Service) procMkdirat(dir *os.File, name string, mode uint32) error {
	if s.procFS.mkdirat != nil {
		return s.procFS.mkdirat(dir, name, mode)
	}
	return unix.Mkdirat(int(dir.Fd()), name, mode)
}

func (s *Service) procChown(f *os.File, uid, gid int) error {
	if s.procFS.chown != nil {
		return s.procFS.chown(f, uid, gid)
	}
	return f.Chown(uid, gid)
}

func (s *Service) procChmod(f *os.File, mode os.FileMode) error {
	if s.procFS.chmod != nil {
		return s.procFS.chmod(f, mode)
	}
	return f.Chmod(mode)
}

func (s *Service) procWrite(f *os.File, p []byte) (int, error) {
	if s.procFS.write != nil {
		return s.procFS.write(f, p)
	}
	return f.Write(p)
}

func (s *Service) procRenameat(dir *os.File, name, newName string) error {
	if s.procFS.renameat != nil {
		return s.procFS.renameat(dir, name, newName)
	}
	return renameAt(dir, name, newName)
}

func (s *Service) procUnlinkat(dir *os.File, name string, flags int) error {
	if s.procFS.unlinkat != nil {
		return s.procFS.unlinkat(dir, name, flags)
	}
	return unix.Unlinkat(int(dir.Fd()), name, flags)
}

func (s *Service) procClose(f *os.File) error {
	if s.procFS.close != nil {
		return s.procFS.close(f)
	}
	return f.Close()
}

func (s *Service) procKill(pgid int, sig syscall.Signal) error {
	if s.procFS.kill != nil {
		return s.procFS.kill(pgid, sig)
	}
	return syscall.Kill(pgid, sig)
}

// procLogNames returns the file names of a process's two logs inside the
// processes directory.
func procLogNames(id uint32) (out, errLog string) {
	s := strconv.FormatUint(uint64(id), 10)
	return s + ".log", s + ".err.log"
}

// procSpecLogPath is the v1 API path of one log: the logs live under the
// sandbox user's home, in the spec path space resolve() maps to.
func procSpecLogPath(id uint32, errLog bool) string {
	outName, errName := procLogNames(id)
	name := outName
	if errLog {
		name = errName
	}
	return path.Join(homeSpec, procDirRel, name)
}

// procMeta is the durable identity of a detached process. The first write
// lands before bash starts (with Pid 0); a second one adds the leader pid
// once it exists. Recovery trusts a process only when its metadata
// validates: matching ids, a plausible start time.
type procMeta struct {
	Version         int    `json:"version"`
	ProcessID       uint32 `json:"process_id"`
	Command         string `json:"command"`
	Cwd             string `json:"cwd"`
	Pid             int    `json:"pid"`
	StartedAtUnixMs int64  `json:"started_at_unix_ms"`
}

// procExit is the durable final status. It is written only after both logs
// are synced, so its presence after a restart means the EXITED record and
// its logs are complete; its absence means LOST.
type procExit struct {
	Version          int    `json:"version"`
	ExitCode         *int32 `json:"exit_code,omitzero"`
	Signal           string `json:"signal,omitzero"`
	DurationMs       int64  `json:"duration_ms"`
	FinishedAtUnixMs int64  `json:"finished_at_unix_ms"`
}

// ownedEntry names one filesystem object a start attempt created, by
// inode: the rollback removes an entry only when it is still the object
// this attempt made, so foreign files, symlinks and whole-directory
// replacements planted at the same names survive untouched. proven is false
// when the identity of a created name could not be read from its descriptor;
// such an entry is never removed — the ambiguity is retained instead of
// deleting a possible foreign replacement.
type ownedEntry struct {
	parent *os.File // directory handle the name lives under
	name   string
	isDir  bool
	dev    uint64
	ino    uint64
	proven bool
}

// detachedProc is one registered detached process: the live handle while
// it runs under this service instance, or its recovered record afterwards.
// A recovered entry has cmd nil, pgid 0, dir nil and both channels
// pre-closed; it is never signaled. dir is closed and nil once the final
// record is durable (or right after recovery): no per-process private
// handle outlives its purpose.
type detachedProc struct {
	mu       sync.Mutex
	rec      *nodev1.Process // current record; mutated under mu
	cmd      *exec.Cmd       // the supervised leader; nil once recovered
	pgid     int             // group to signal; never taken from disk state
	dir      *os.File        // private metadata directory, no-follow pinned
	wOut     *os.File        // log append handles, kept for the exit sync
	wErr     *os.File
	ready    chan struct{} // closed when the start attempt resolved
	done     chan struct{} // closed when the final status is recorded
	startErr error         // why the start attempt failed; read after ready
	// rollbackErr is set when a failed attempt could not remove its state
	// durably: the reservation is then retained (the id stays taken) until
	// a later StartProcess proves the cleanup, retrying it. ownedState is
	// that attempt's inode-tracked inventory. Both read after ready,
	// guarded by mu once live.
	rollbackErr error
	ownedState  []ownedEntry
	// childStarted records that the failed attempt did fork. reaped is set
	// by the sole cmd.Wait owner (superviseProc or ownFailedStart) before
	// it releases the signal lock. reapedCh closes when a failed start's
	// reap returns. retained rollback removes state only after the group
	// is killed and the leader reaped.
	childStarted bool
	reaped       bool
	reapedCh     chan struct{}

	// sigMu serializes group ownership decisions: every signal the kill
	// path sends, and the final reap-plus-finalize (supervisor or failed-start
	// owner), run under it, so no signal can target a group the process no
	// longer owns (a re-used pgid after finalization).
	sigMu sync.Mutex
	// rollbackMu serializes retained-rollback retries: concurrent callers
	// for the same id must not remove state or close the pinned inventory
	// handles underneath one another.
	rollbackMu sync.Mutex
}

// StartProcess starts a detached process and returns once it runs (state
// RUNNING, pid set) or already ended. Idempotent on process_id: an
// existing id returns that process whatever the other fields say. The id
// is reserved for the duration of one start attempt and released when the
// attempt fails and its state is durably removed; an attempt whose
// rollback did not prove durable keeps the reservation, and the next
// StartProcess for that id retries the cleanup before starting fresh.
func (s *Service) StartProcess(ctx context.Context, req *guestv1.StartProcessRequest) (*nodev1.Process, error) {
	id := req.GetProcessId()
	if id == 0 {
		return nil, status.Error(codes.InvalidArgument, "process_id must be at least 1")
	}
	for {
		if p := s.lookupProc(id); p != nil {
			got, err := waitProcReady(ctx, p)
			if err != nil {
				return nil, err
			}
			if got {
				return s.processSnapshot(p), nil
			}
			// The attempt failed. A durably-cleaned one released the id
			// already (or releases it now); one with a failed rollback
			// keeps it until the cleanup is proven.
			p.mu.Lock()
			rollbackErr, startErr := p.rollbackErr, p.startErr
			p.mu.Unlock()
			if rollbackErr != nil {
				if err := s.finishRollback(id, p); err != nil {
					return nil, status.Errorf(codes.Internal,
						"process %d: start failed (%v) and its state is not durably removed: %v",
						id, startErr, err)
				}
			}
			s.releaseReservation(id, p)
			continue // the id is free again: this caller starts fresh
		}
		p := &detachedProc{ready: make(chan struct{}), done: make(chan struct{})}
		s.procMu.Lock()
		if _, ok := s.procs[id]; ok { // lost the reservation race
			s.procMu.Unlock()
			continue
		}
		s.procs[id] = p
		s.procMu.Unlock()

		err := s.registerProcess(id, req, p)
		p.startErr = err
		close(p.ready)
		p.mu.Lock()
		rollbackErr := p.rollbackErr
		p.mu.Unlock()
		if err == nil {
			return s.processSnapshot(p), nil
		}
		if rollbackErr != nil {
			// Keep the reservation: the state on disk is still this
			// attempt's, and deleting someone else's would be worse.
			return nil, status.Errorf(codes.Internal,
				"process %d: %v; state cleanup also failed: %v", id, err, rollbackErr)
		}
		s.releaseReservation(id, p)
		return nil, err
	}
}

// releaseReservation drops a failed attempt's id reservation.
func (s *Service) releaseReservation(id uint32, p *detachedProc) {
	s.procMu.Lock()
	defer s.procMu.Unlock()
	if s.procs[id] == p {
		delete(s.procs, id)
	}
}

// waitProcReady waits for a start attempt to resolve and reports whether
// it produced a process (true) or failed (false).
func waitProcReady(ctx context.Context, p *detachedProc) (bool, error) {
	select {
	case <-p.ready:
		return p.startErr == nil, nil
	case <-ctx.Done():
		return false, status.FromContextError(ctx.Err()).Err()
	}
}

// lookupProc returns the registered process for id, or nil.
func (s *Service) lookupProc(id uint32) *detachedProc {
	s.procMu.Lock()
	defer s.procMu.Unlock()
	return s.procs[id]
}

// resolveProc finds the process for id, waiting for a still-running start
// attempt to resolve, with the contract's NOT_FOUND for unknown ids.
func (s *Service) resolveProc(ctx context.Context, id uint32) (*detachedProc, error) {
	p := s.lookupProc(id)
	if p == nil {
		return nil, status.Errorf(codes.NotFound, "process %d not found", id)
	}
	if got, err := waitProcReady(ctx, p); err != nil || !got {
		if err != nil {
			return nil, err
		}
		return nil, status.Errorf(codes.NotFound, "process %d not found", id)
	}
	return p, nil
}

// registerProcess performs one start attempt for a reserved id. On success
// it fills p's live fields (rec, cmd, pgid, dir, log handles) and starts
// the supervisor. On failure nothing of the attempt survives when the
// rollback is provable: exactly the entries this attempt created — by
// inode — are removed and the processes directory synced; a process that
// did start is killed and reaped first. When the rollback fails, p keeps
// rollbackErr and ownedState so the reservation can retry it.
func (s *Service) registerProcess(id uint32, req *guestv1.StartProcessRequest, p *detachedProc) error {
	if err := validateEnv(req.GetEnv()); err != nil {
		return err
	}
	// The service's own state comes first: a closed service reports
	// INTERNAL here rather than misreading its dead roots as a bad cwd.
	base, err := s.procStateDir()
	if err != nil {
		return status.Errorf(codes.Internal, "process state: %v", err)
	}
	wd, canonical, err := s.openWorkdir(req.GetCwd())
	if err != nil {
		return err // no state exists yet: nothing to roll back
	}
	defer wd.Close()

	committed := false
	defer func() {
		if committed {
			return
		}
		if p.cmd != nil {
			// A failed start's child is stopped here, bounded: when its
			// death cannot be proven the reservation, its state and every
			// pinned cleanup handle are retained for the retry.
			s.abortFailedStart(id, p)
		}
		p.mu.Lock()
		rollbackErr := p.rollbackErr
		owned := slices.Clone(p.ownedState)
		p.mu.Unlock()
		if rollbackErr != nil {
			s.log.Warn("failed start retained for retry", "process_id", id, "err", rollbackErr)
			return
		}
		if len(owned) == 0 {
			s.closeProcAttemptHandles(p)
			return
		}
		if rollbackErr = s.removeOwnedProcState(base, owned); rollbackErr != nil {
			p.mu.Lock()
			p.rollbackErr = rollbackErr
			p.mu.Unlock()
			return // handles stay pinned for the retry
		}
		p.mu.Lock()
		p.ownedState = nil
		p.rollbackErr = nil
		p.mu.Unlock()
		s.closeProcAttemptHandles(p)
	}()

	cred, err := s.credential()
	if err != nil {
		return status.Errorf(codes.Internal, "credential: %v", err)
	}
	dir, outLog, errLog, err := s.createProcState(id, base, p)
	// The handles created so far stay pinned until the cleanup is durable;
	// the rollback owns exactly the entries tracked by createProcState.
	p.mu.Lock()
	p.dir, p.wOut, p.wErr = dir, outLog, errLog
	p.mu.Unlock()
	if err != nil {
		return status.Errorf(codes.Internal, "process state: %v", err)
	}

	start := time.Now()
	meta := procMeta{
		Version:         procStateVersion,
		ProcessID:       id,
		Command:         req.GetCommand(),
		Cwd:             canonical,
		StartedAtUnixMs: start.UnixMilli(),
	}
	if err := s.writeOwnedState(dir, procMetaName, &meta, p); err != nil {
		return status.Errorf(codes.Internal, "persist process metadata: %v", err)
	}

	cmd := exec.Command("bash", "-lc", req.GetCommand())
	// The child chdirs through the pinned working-directory descriptor
	// before execve (see Exec for why the original fd, never a dup).
	cmd.Dir = "/proc/self/fd/" + strconv.Itoa(int(wd.Fd()))
	cmd.Env = s.commandEnvList(req.GetEnv())
	cmd.Stdout, cmd.Stderr = outLog, errLog
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true, Credential: cred}
	if err := cmd.Start(); err != nil {
		return status.Errorf(codes.Internal, "start: %v", err)
	}
	p.mu.Lock()
	p.cmd = cmd
	p.pgid = cmd.Process.Pid
	p.childStarted = true
	p.reapedCh = make(chan struct{})
	p.mu.Unlock()

	// The record exists from the first moment a process does, so the kill
	// path's ownership checks always have state to read — including on the
	// failure paths below.
	rec := &nodev1.Process{
		ProcessId:       id,
		Pid:             int32(cmd.Process.Pid),
		State:           nodev1.ProcessState_PROCESS_STATE_RUNNING,
		Command:         req.GetCommand(),
		Cwd:             canonical,
		StartedAtUnixMs: start.UnixMilli(),
		LogPath:         procSpecLogPath(id, false),
		ErrLogPath:      procSpecLogPath(id, true),
	}
	p.mu.Lock()
	p.rec = rec
	p.mu.Unlock()

	// Record the leader pid durably before announcing the process: a crash
	// between fork and this write recovers as LOST with an unknown pid
	// rather than losing the process entirely.
	meta.Pid = cmd.Process.Pid
	if err := s.writeOwnedState(dir, procMetaName, &meta, p); err != nil {
		return status.Errorf(codes.Internal, "persist process metadata: %v", err)
	}

	committed = true
	s.log.Info("process started", "process_id", id, "pid", cmd.Process.Pid, "cwd", canonical)
	go s.superviseProc(p, start)
	return nil
}

// writeOwnedState writes one durable metadata record for a starting attempt.
// The temporary name is tracked from the moment its descriptor exists and
// the final name is tracked the moment the rename lands — before the
// directory sync — so every failure leaves the object it created in the
// attempt's inventory. A rename that fails leaves the temporary entry; a
// rename that succeeded but whose directory sync failed leaves the final
// entry. The two are distinct and rollback removes exactly what exists.
func (s *Service) writeOwnedState(dir *os.File, name string, record any, p *detachedProc) error {
	return s.writeProcStateRecord(dir, name, record, p)
}

// abortFailedStart stops the child of a failed start attempt without
// blocking the RPC: one goroutine owns the leader reap, the kill is bounded,
// and the caller waits a bounded time for the reap. When death and reap
// cannot be proven, rollbackErr records that and the attempt keeps its
// reservation, state and pinned handles; a later start retries the cleanup.
func (s *Service) abortFailedStart(id uint32, p *detachedProc) {
	p.mu.Lock()
	started := p.childStarted && p.cmd != nil && !p.reaped
	reapedCh := p.reapedCh
	p.mu.Unlock()
	if !started {
		return
	}
	go s.ownFailedStart(p)
	ctx, cancel := context.WithTimeout(context.Background(), procAbortBound)
	defer cancel()
	if _, err := s.killGroup(ctx, p, procAbortKillGrace); err != nil {
		s.setRollbackErr(p, fmt.Errorf("process %d not proven dead: %w", id, err))
		return
	}
	select {
	case <-reapedCh:
	case <-ctx.Done():
		s.setRollbackErr(p, fmt.Errorf("process %d leader reap not proven: %w", id, ctx.Err()))
	}
}

// ownFailedStart is the single owner of a failed start's leader reap. It
// preserves the leader zombie — and with it the process group id — until the
// whole group is observably gone, then reaps under the signal lock, exactly
// as the normal supervisor does. A kill racing here either signals while the
// group is still owned or is refused by the reaped ownership check; it can
// never signal a reused pgid after the reap. The owner deliberately outlives
// the RPC: a child that dies later is still reaped once.
func (s *Service) ownFailedStart(p *detachedProc) {
	p.mu.Lock()
	cmd, pgid, ch := p.cmd, p.pgid, p.reapedCh
	p.mu.Unlock()
	for empty := 0; empty < procEmptyConfirmations; {
		if s.scanGroup(pgid, false) {
			empty = 0
		} else {
			empty++
		}
		time.Sleep(procGroupPoll)
	}
	p.sigMu.Lock()
	_ = cmd.Wait()
	p.mu.Lock()
	p.cmd = nil
	p.reaped = true
	p.mu.Unlock()
	p.sigMu.Unlock()
	close(ch)
}

// setRollbackErr records why a failed attempt's cleanup must be retained.
func (s *Service) setRollbackErr(p *detachedProc, err error) {
	p.mu.Lock()
	p.rollbackErr = err
	p.mu.Unlock()
}

// trackOwned records an entry the current start attempt created. A later
// entry for the same parent and name supersedes its predecessor.
func (p *detachedProc) trackOwned(e ownedEntry) {
	p.mu.Lock()
	defer p.mu.Unlock()
	kept := p.ownedState[:0]
	for _, old := range p.ownedState {
		if old.parent == e.parent && old.name == e.name {
			continue
		}
		kept = append(kept, old)
	}
	p.ownedState = append(kept, e)
}

// renameOwned moves a tracked entry to its name after a successful rename:
// the inode is unchanged, so its identity stays exact. An entry the rename
// replaced at the new name belongs to the same attempt and is dropped.
func (p *detachedProc) renameOwned(parent *os.File, oldName, newName string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	kept := p.ownedState[:0]
	for _, e := range p.ownedState {
		if e.parent == parent && e.name == newName {
			continue
		}
		if e.parent == parent && e.name == oldName {
			e.name = newName
		}
		kept = append(kept, e)
	}
	p.ownedState = kept
}

// superviseProc waits for the process to end — the whole group, not just
// the bash leader: descendants outlive their leader, still hold the log
// fds open and must keep the process alive for kill and attach — then syncs
// the logs and attempts to persist the final status before publishing EXITED
// in memory. The leader stays un-reaped until the group's last live member is
// gone: its zombie keeps the pid — and with it the process group id — owned,
// so a concurrent kill can never signal a group the process no longer owns.
// Reaping and the reaped mark run under the process's signal lock. The lock is
// released before durable I/O, and the mark rejects every later signal even
// while persistence is still pending. The supervisor runs independently of
// any RPC: a detached process outlives its StartProcess call, and its exit
// record must land even if no RPC ever touches it again. A log-sync failure
// leaves no exit record; a later write or directory-sync failure can leave a
// complete record behind. The in-memory record still turns EXITED after the
// durability attempt because the process did end. After a restart, a valid
// surviving exit record recovers as EXITED; without one, the process is LOST.
func (s *Service) superviseProc(p *detachedProc, start time.Time) {
	id := p.rec.ProcessId // immutable for the lifetime of the record
	pgid := p.pgid
	// The group's last live member ends the process. Emptiness must be
	// observed repeatedly before it counts: one scan that misses a member
	// (a /proc read racing an exec, say) must never end ownership early.
	for empty := 0; empty < procEmptyConfirmations; {
		if s.scanGroup(pgid, false) {
			empty = 0
		} else {
			empty++
		}
		time.Sleep(procGroupPoll)
	}
	p.sigMu.Lock()
	p.cmd.Wait() // reap the leader zombie: ownership ends here
	exit := procExitRecord(p.cmd, start)
	finished := time.Now().UnixMilli()
	p.mu.Lock()
	p.reaped = true
	dir, wOut, wErr := p.dir, p.wOut, p.wErr
	p.mu.Unlock()
	p.sigMu.Unlock()

	// Logs first, then the final status: an exit record without durable
	// logs would promise output a crash can lose.
	durErr := errors.Join(s.syncFile(wOut), s.syncFile(wErr))
	if durErr == nil {
		fin := procExit{
			Version:          procStateVersion,
			ExitCode:         exit.ExitCode,
			Signal:           exit.Signal,
			DurationMs:       exit.DurationMs,
			FinishedAtUnixMs: finished,
		}
		durErr = s.writeProcState(dir, procExitName, fin)
	}
	if durErr != nil {
		s.log.Error("process exit not durable; a restart may report it LOST",
			"process_id", id, "err", durErr)
	}
	dir.Close()
	p.mu.Lock()
	p.rec.State = nodev1.ProcessState_PROCESS_STATE_EXITED
	p.rec.Exit = exit
	p.rec.FinishedAtUnixMs = finished
	p.dir = nil // finalized: no private handle is held past the record
	p.mu.Unlock()
	wOut.Close()
	wErr.Close()
	close(p.done)
}

// procExitRecord converts the reaped leader's wait status into the record's
// exit, with ExecEvent.Exit's meaning. timed_out stays false: detached
// processes have no timeout.
func procExitRecord(cmd *exec.Cmd, start time.Time) *nodev1.ExecEvent_Exit {
	exit := &nodev1.ExecEvent_Exit{DurationMs: time.Since(start).Milliseconds()}
	if ps := cmd.ProcessState; ps != nil {
		if ws, ok := ps.Sys().(syscall.WaitStatus); ok {
			switch {
			case ws.Exited():
				exit.ExitCode = new(int32(ws.ExitStatus()))
			case ws.Signaled():
				exit.Signal = signalName(ws.Signal())
			}
		}
	}
	return exit
}

// KillProcess sends SIGTERM to the running process's whole group, then
// SIGKILL after the grace period to whatever survives — a process group
// outlives its leader, so descendants bash leaves behind are reached too,
// even once the leader is gone — and returns once the process ended. The
// process stays RUNNING until its whole group is gone, so a kill always
// finds a live group to signal, and every signal is sent while the
// supervisor provably still owns the group: between the running check and
// the signal the process may finalize, and the signal is then skipped
// (signaled reflects that it was running when the RPC arrived). An
// already-ended process (EXITED, or recovered LOST) returns with
// signaled=false and is never signaled: recovered entries carry no live
// handle, and the service never signals a PID taken from disk state. A
// canceled call stops waiting promptly; the signals already sent are not
// taken back, but no unproven success is reported.
func (s *Service) KillProcess(ctx context.Context, req *guestv1.KillProcessRequest) (*nodev1.KillProcessResponse, error) {
	p, err := s.resolveProc(ctx, req.GetProcessId())
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	running := p.rec.State == nodev1.ProcessState_PROCESS_STATE_RUNNING && p.pgid > 0
	p.mu.Unlock()
	sent := false
	if running {
		grace := time.Duration(cmp.Or(req.GetGraceSeconds(), defaultKillGraceSec)) * time.Second
		sent, err = s.killGroup(ctx, p, grace)
		if err != nil {
			if ctx.Err() != nil {
				return nil, status.FromContextError(ctx.Err()).Err()
			}
			return nil, status.Errorf(codes.Internal, "kill process %d: %v", req.GetProcessId(), err)
		}
		select {
		case <-p.done:
		case <-ctx.Done():
			return nil, status.FromContextError(ctx.Err()).Err()
		}
	}
	return &nodev1.KillProcessResponse{Process: s.processSnapshot(p), Signaled: sent}, nil
}

// killGroup ends every live member of the process group: SIGTERM first,
// then SIGKILL once the grace period passes, and reports whether it sent
// any signal at all. Every signal is sent under the process's signal lock
// with a fresh running check, so a group the process finalizes out from
// under the kill is never signaled (its pgid could already be reused) —
// the kill then reports sent=false alongside the process's own final
// state. Membership is read from /proc rather than kill(-pgid) alone, so
// members that already died (zombies waiting to be reaped) do not hold the
// loop open and the scan keeps working after the group leader — the
// un-reaped bash — is gone. Every wait is bounded and context-aware: when
// ctx ends the loop returns without claiming anything, and a group that
// cannot be proven empty after SIGKILL is reported as an error instead of
// a success.
func (s *Service) killGroup(ctx context.Context, p *detachedProc, grace time.Duration) (sent bool, err error) {
	p.mu.Lock()
	pgid := p.pgid
	p.mu.Unlock()
	if pause := s.preSignalHook(); pause != nil {
		pause() // test staging only: between the kill's checks and its signal
	}
	switch err := s.signalGroup(p, syscall.SIGTERM); {
	case err == nil:
		sent = true
	case errors.Is(err, errOwnershipEnded):
		return false, nil
	case errors.Is(err, errGroupGone):
		// ESRCH is only accepted as "ended" when the scan agrees; a group
		// that still has visible members is an error, never a silent success.
		if s.scanGroup(pgid, false) {
			return false, fmt.Errorf("group %d: kernel reports no members (ESRCH) but the scan sees live ones", pgid)
		}
		return false, nil
	default:
		return false, err
	}
	deadline := time.Now().Add(grace)
	for s.scanGroup(pgid, false) && time.Now().Before(deadline) {
		if ctx.Err() != nil {
			return sent, ctx.Err()
		}
		time.Sleep(15 * time.Millisecond)
	}
	if s.scanGroup(pgid, false) {
		switch err := s.signalGroup(p, syscall.SIGKILL); {
		case err == nil:
		case errors.Is(err, errOwnershipEnded):
			return sent, nil // TERM went out; the group ended on its own
		case errors.Is(err, errGroupGone):
			if s.scanGroup(pgid, false) {
				return sent, fmt.Errorf("group %d: kernel reports no members (ESRCH) but the scan sees live ones", pgid)
			}
			return sent, nil
		default:
			return sent, err
		}
		prove := time.Now().Add(s.killProveBound())
		for s.scanGroup(pgid, false) {
			if ctx.Err() != nil {
				return sent, ctx.Err()
			}
			if time.Now().After(prove) {
				return sent, fmt.Errorf("group %d: %w", pgid, errGroupUnproven)
			}
			time.Sleep(15 * time.Millisecond)
		}
	}
	// Let the reaper collect the dead so the group is observably empty;
	// bounded, and never a precondition for claiming the kill worked.
	sweep := time.Now().Add(2 * time.Second)
	for time.Now().Before(sweep) && s.scanGroup(pgid, true) {
		if ctx.Err() != nil {
			return sent, ctx.Err()
		}
		time.Sleep(10 * time.Millisecond)
	}
	return sent, nil
}

// signalGroup sends sig to the process group under the signal lock with a
// fresh running check. A kernel ESRCH (no member to signal) is reported as
// errGroupGone; every other syscall error is returned, never swallowed.
// Callers confirm errGroupGone with a scan before treating the group as
// ended. The lock is held only across the check and the syscall: nothing
// that cleanup needs is blocked behind it.
func (s *Service) signalGroup(p *detachedProc, sig syscall.Signal) error {
	p.sigMu.Lock()
	defer p.sigMu.Unlock()
	p.mu.Lock()
	// A reaped leader ends ownership even though no final record has been
	// written yet (a failed start): its pgid may already be reused.
	running := p.rec != nil && p.rec.State == nodev1.ProcessState_PROCESS_STATE_RUNNING && !p.reaped
	pgid := p.pgid
	p.mu.Unlock()
	if !running || pgid <= 0 {
		return errOwnershipEnded
	}
	if err := s.procKill(-pgid, sig); err != nil {
		if errors.Is(err, syscall.ESRCH) {
			return errGroupGone
		}
		return fmt.Errorf("signal %s to group %d: %w", sig, pgid, err)
	}
	return nil
}

// scanGroup reports whether the process group pgid still has members,
// counting zombies only when includeZombies, through this service's
// swappable scan (tests inject a lying one). It reads /proc directly:
// after the leader dies the group's members can belong to any reaper.
func (s *Service) scanGroup(pgid int, includeZombies bool) bool {
	s.procMu.Lock()
	scan := s.groupScan
	s.procMu.Unlock()
	if scan == nil {
		scan = groupAlive
	}
	return scan(pgid, includeZombies)
}

// killProveBound is this service's bound for proving a SIGKILLed group
// ended (tests shorten it).
func (s *Service) killProveBound() time.Duration {
	s.procMu.Lock()
	d := s.killProve
	s.procMu.Unlock()
	if d <= 0 {
		return defaultKillProve
	}
	return d
}

// preSignalHook is the service's test staging point between a kill's
// running check and its first signal (nil in production).
func (s *Service) preSignalHook() func() {
	s.procMu.Lock()
	defer s.procMu.Unlock()
	return s.preSignal
}

// groupAlive reports whether the process group pgid still has members,
// counting zombies only when includeZombies.
func groupAlive(pgid int, includeZombies bool) bool {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return true // cannot prove emptiness: assume alive
	}
	for _, e := range entries {
		data, err := os.ReadFile("/proc/" + e.Name() + "/stat")
		if err != nil {
			continue // raced with exit: not evidence of life
		}
		// Fields after the comm field in parentheses: state, ppid, pgrp.
		_, rest, ok := strings.CutLast(string(data), ")")
		if !ok {
			continue
		}
		fields := strings.Fields(rest)
		if len(fields) < 3 {
			continue
		}
		if !includeZombies && fields[0] == "Z" {
			continue
		}
		if pgrp, err := strconv.Atoi(fields[2]); err == nil && pgrp == pgid {
			return true
		}
	}
	return false
}

// processSnapshot clones the current record with fresh log sizes read
// through the processes directory.
func (s *Service) processSnapshot(p *detachedProc) *nodev1.Process {
	p.mu.Lock()
	rec := proto.Clone(p.rec).(*nodev1.Process)
	p.mu.Unlock()
	outName, errName := procLogNames(rec.ProcessId)
	rec.StdoutSize = s.logSize(outName)
	rec.StderrSize = s.logSize(errName)
	return rec
}

// logSize returns the current size of one log in the processes directory,
// 0 when it is missing or not a regular file (a planted symlink, say).
func (s *Service) logSize(name string) int64 {
	base, err := s.procStateDir()
	if err != nil {
		return 0
	}
	var st unix.Stat_t
	if err := unix.Fstatat(int(base.Fd()), name, &st, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return 0
	}
	if st.Mode&unix.S_IFMT != unix.S_IFREG {
		return 0
	}
	return st.Size
}

// --- state directory plumbing ------------------------------------------------

// procStateDir returns the shared processes directory under the home root,
// opening (and creating, owned by the sandbox user) it on first use. The
// walk is anchored at the pinned confinement root — never re-resolved
// from the configured path, which a rename could have detached — and every
// owned component below it is opened without following symlinks, so a
// hostile home cannot redirect the service's state writes.
func (s *Service) procStateDir() (*os.File, error) {
	s.procMu.Lock()
	defer s.procMu.Unlock()
	if s.procDir != nil {
		return s.procDir, nil
	}
	if s.procDirClosed {
		return nil, errors.New("process state directory is closed")
	}
	anchor, err := s.homeRoot.Open(".")
	if err != nil {
		return nil, err
	}
	defer anchor.Close()
	dir, err := s.openStateDirFrom(anchor, procDirRel, true)
	if err != nil {
		return nil, err
	}
	s.procDir = dir
	return dir, nil
}

// openStateDirFrom walks rel below anchor one component at a time,
// creating missing components as directories owned by the sandbox user
// when create. Every component is chowned and its entry in the parent is
// synced even when mkdir reported EEXIST: a previous walk may have created
// the entry and failed before its ownership or parent sync completed, and a
// retry must re-establish the step instead of skipping it. A crash
// therefore cannot leave a state directory the service relies on without
// both its owner and its durable parent entry.
func (s *Service) openStateDirFrom(anchor *os.File, rel string, create bool) (*os.File, error) {
	f := anchor
	mine := false // whether f is a handle this walk must close
	for part := range strings.SplitSeq(rel, "/") {
		if create {
			if _, err := mkdirAt(f, part, s.uid, s.gid); err != nil {
				if mine {
					f.Close()
				}
				return nil, fmt.Errorf("mkdir %s: %w", part, err)
			}
		}
		next, err := s.procOpenat(f, part, unix.O_RDONLY|unix.O_DIRECTORY, 0)
		if err != nil {
			if mine {
				f.Close()
			}
			return nil, fmt.Errorf("open %s: %w", part, err)
		}
		if create {
			if err := s.procChown(next, s.uid, s.gid); err != nil {
				next.Close()
				if mine {
					f.Close()
				}
				return nil, fmt.Errorf("chown %s: %w", part, err)
			}
			if err := s.syncFile(f); err != nil {
				next.Close()
				if mine {
					f.Close()
				}
				return nil, fmt.Errorf("sync %s entry: %w", part, err)
			}
		}
		if mine {
			f.Close()
		}
		f, mine = next, true
	}
	if !mine {
		return nil, errors.New("empty process state path")
	}
	return f, nil
}

// createProcState claims the private metadata directory and both logs of
// one process id, all owned by the sandbox user. Any pre-existing entry at
// those names is refused — a fresh reservation only ever creates fresh
// state, and what is already there is not this attempt's to delete — and
// every object it creates is tracked by inode from the descriptor that
// created it, before any chown, chmod or sync can fail. A created name whose
// descriptor cannot be read is tracked as unproven instead of being deleted
// as a possible foreign replacement.
func (s *Service) createProcState(id uint32, base *os.File, p *detachedProc) (dir, outLog, errLog *os.File, err error) {
	dirName := strconv.FormatUint(uint64(id), 10)
	outName, errName := procLogNames(id)
	for _, name := range []string{dirName, outName, errName} {
		exists, ferr := nameExists(base, name)
		if ferr != nil {
			return nil, nil, nil, ferr
		}
		if exists {
			return nil, nil, nil, fmt.Errorf("state for process %d already exists (%s): refusing to touch it", id, name)
		}
	}
	if err = s.procMkdirat(base, dirName, 0o755); err != nil {
		return nil, nil, nil, fmt.Errorf("create metadata directory: %w", err)
	}
	// The name is this attempt's from the successful mkdir on. mkdir hands
	// back no descriptor, so its identity is read by name right away; the
	// opened descriptor is then checked against it. A failed chown, or even
	// a failed open, can never leave the directory untracked.
	entry, statErr := statOwnedDir(base, dirName)
	p.trackOwned(entry)
	if statErr != nil {
		return nil, nil, nil, fmt.Errorf("stat metadata directory: %w", statErr)
	}
	dir, err = s.procOpenat(base, dirName, unix.O_RDONLY|unix.O_DIRECTORY, 0)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("open metadata directory: %w", err)
	}
	opened, entryErr := entryOf(dir, base, dirName, true)
	if entryErr != nil {
		s.procClose(dir)
		return nil, nil, nil, fmt.Errorf("stat metadata directory: %w", entryErr)
	}
	if opened.dev != entry.dev || opened.ino != entry.ino {
		s.procClose(dir)
		return nil, nil, nil, errors.New("metadata directory was replaced during creation")
	}
	if err = s.procChown(dir, s.uid, s.gid); err != nil {
		s.procClose(dir)
		return nil, nil, nil, fmt.Errorf("chown metadata directory: %w", err)
	}
	if err = s.syncFile(base); err != nil {
		s.procClose(dir)
		return nil, nil, nil, fmt.Errorf("sync processes directory: %w", err)
	}
	outLog, err = s.openProcLog(base, outName, p)
	if err != nil {
		return dir, nil, nil, fmt.Errorf("open stdout log: %w", err)
	}
	errLog, err = s.openProcLog(base, errName, p)
	if err != nil {
		return dir, outLog, nil, fmt.Errorf("open stderr log: %w", err)
	}
	return dir, outLog, errLog, nil
}

// removeOwnedProcState durably removes a failed attempt's state: each
// inventory entry is removed only if it is still the object this attempt
// created (same inode); anything planted or replaced at those names is
// preserved and reported. Removals finish with a sync of the processes
// directory, so a crash cannot resurrect the id from a delete the disk
// never saw.
func (s *Service) removeOwnedProcState(base *os.File, owned []ownedEntry) error {
	var errs []error
	// Newest first, so the metadata files go before the directory itself.
	for i := len(owned) - 1; i >= 0; i-- {
		e := owned[i]
		if !e.proven {
			// The created name's identity never became readable. Deleting it
			// could destroy a foreign replacement, so keep the ambiguity and
			// let the reservation stay retained.
			errs = append(errs, fmt.Errorf("%s: identity unproven; preserved", e.name))
			continue
		}
		var st unix.Stat_t
		err := unix.Fstatat(int(e.parent.Fd()), e.name, &st, unix.AT_SYMLINK_NOFOLLOW)
		if errors.Is(err, fs.ErrNotExist) {
			continue // already gone: idempotent retries
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("stat %s: %w", e.name, err))
			continue
		}
		if st.Dev != e.dev || st.Ino != e.ino {
			// Not ours anymore: preserve whatever is there and say so.
			errs = append(errs, fmt.Errorf("%s was replaced after the attempt; preserved", e.name))
			continue
		}
		flags := 0
		if e.isDir {
			flags = unix.AT_REMOVEDIR
		}
		if err := s.procUnlinkat(e.parent, e.name, flags); err != nil && !errors.Is(err, fs.ErrNotExist) {
			errs = append(errs, fmt.Errorf("remove %s: %w", e.name, err))
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return s.syncFile(base)
}

// finishRollback retries the durable cleanup of a retained reservation,
// clearing the failure when it finally proves out. The attempt's pinned
// handles stay open until the removal is durable, because ownedEntry.parent
// is the metadata directory handle the retry stats through; they are closed
// exactly once afterwards.
func (s *Service) finishRollback(id uint32, p *detachedProc) error {
	p.rollbackMu.Lock()
	defer p.rollbackMu.Unlock()
	base, err := s.procStateDir()
	if err != nil {
		return err
	}
	if err := s.settleFailedChild(id, p); err != nil {
		s.setRollbackErr(p, err)
		return err
	}
	p.mu.Lock()
	owned := slices.Clone(p.ownedState)
	p.mu.Unlock()
	if len(owned) == 0 {
		p.mu.Lock()
		p.rollbackErr = nil
		p.mu.Unlock()
		s.closeProcAttemptHandles(p)
		return nil
	}
	if err := s.removeOwnedProcState(base, owned); err != nil {
		s.setRollbackErr(p, err)
		return err
	}
	p.mu.Lock()
	p.rollbackErr = nil
	p.ownedState = nil
	p.mu.Unlock()
	s.closeProcAttemptHandles(p)
	return nil
}

// settleFailedChild proves a retained failed start's child is dead and
// reaped before its state may be removed: a bounded kill retry, then a
// bounded wait on the single reaper that owns cmd.Wait.
func (s *Service) settleFailedChild(id uint32, p *detachedProc) error {
	p.mu.Lock()
	started := p.childStarted
	reapedCh := p.reapedCh
	p.mu.Unlock()
	if !started {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), procAbortBound)
	defer cancel()
	if _, err := s.killGroup(ctx, p, procAbortKillGrace); err != nil {
		return fmt.Errorf("process %d not proven dead: %w", id, err)
	}
	select {
	case <-reapedCh:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("process %d leader reap not proven: %w", id, ctx.Err())
	}
}

// closeProcAttemptHandles releases a failed attempt's pinned handles exactly
// once, after its rollback is durable. It is never called while a rollback
// is pending: ownedEntry.parent needs the directory handle to retry.
func (s *Service) closeProcAttemptHandles(p *detachedProc) {
	p.mu.Lock()
	handles := []*os.File{p.wOut, p.wErr, p.dir}
	p.wOut, p.wErr, p.dir = nil, nil, nil
	p.mu.Unlock()
	for _, f := range handles {
		if f == nil {
			continue
		}
		if err := s.procClose(f); err != nil {
			s.log.Warn("closing a failed start's handle", "err", err)
		}
	}
}

// nameExists reports whether name exists under dir, without following a
// symlink at the final component.
func nameExists(dir *os.File, name string) (bool, error) {
	var st unix.Stat_t
	err := unix.Fstatat(int(dir.Fd()), name, &st, unix.AT_SYMLINK_NOFOLLOW)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	return false, err
}

// statOwnedDir captures the inode identity of a directory just created
// under dir. mkdir hands back no descriptor, so this single read is by name;
// the caller then checks the opened descriptor against it. The entry is
// returned even on error with proven=false, so the caller can retain the
// ambiguity instead of guessing.
func statOwnedDir(dir *os.File, name string) (ownedEntry, error) {
	e := ownedEntry{parent: dir, name: name, isDir: true}
	var st unix.Stat_t
	if err := unix.Fstatat(int(dir.Fd()), name, &st, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return e, err
	}
	e.dev, e.ino, e.proven = uint64(st.Dev), uint64(st.Ino), true
	return e, nil
}

// entryOf captures an already-open descriptor's inode identity under name.
// The returned entry is returned even on error with proven=false, so the
// caller can retain the ambiguity instead of guessing.
func entryOf(f *os.File, parent *os.File, name string, isDir bool) (ownedEntry, error) {
	e := ownedEntry{parent: parent, name: name, isDir: isDir}
	fi, err := f.Stat()
	if err != nil {
		return e, err
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return e, fmt.Errorf("stat %s: no unix file info", name)
	}
	e.dev, e.ino, e.proven = uint64(st.Dev), uint64(st.Ino), true
	return e, nil
}

// openProcLog creates one process log for appending, owned by the sandbox
// user, and rejects anything already at the name or anything that is not a
// regular file: the id is reserved, so a fresh process starts with fresh
// logs — never bytes a plant left behind — and a planted FIFO can neither
// hang the open nor inherit a service fd. O_APPEND keeps interleaved
// writes from the process and its descendants ordered no matter who seeks
// the shared description; the nonblocking flag is inert on regular files.
func (s *Service) openProcLog(base *os.File, name string, p *detachedProc) (*os.File, error) {
	f, err := s.procOpenat(base, name, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_APPEND|unix.O_NONBLOCK, 0o644)
	if err != nil {
		return nil, err
	}
	// Track from the O_EXCL descriptor before chown, chmod or the parent
	// sync: every later failure leaves the log in the attempt's inventory.
	entry, entryErr := entryOf(f, base, name, false)
	p.trackOwned(entry)
	if entryErr != nil {
		s.procClose(f)
		return nil, fmt.Errorf("stat %s: %w", name, entryErr)
	}
	if fi, serr := f.Stat(); serr != nil || !fi.Mode().IsRegular() {
		s.procClose(f)
		if serr != nil {
			return nil, fmt.Errorf("stat %s: %w", name, serr)
		}
		return nil, fmt.Errorf("%s is not a regular file", name)
	}
	if err := s.procChown(f, s.uid, s.gid); err != nil {
		s.procClose(f)
		return nil, err
	}
	if err := s.procChmod(f, 0o644); err != nil { // openat's mode is umask-filtered
		s.procClose(f)
		return nil, err
	}
	// The log's directory entry must be durable before the process can be
	// trusted to write to it.
	if err := s.syncFile(base); err != nil {
		s.procClose(f)
		return nil, err
	}
	return f, nil
}

// writeProcState atomically replaces name under dir with the JSON record:
// a hidden temporary sibling is written, synced, renamed over the name and
// the directory synced, so a crash never leaves a torn record behind a
// new one or a missing one behind a rename.
func (s *Service) writeProcState(dir *os.File, name string, record any) error {
	return s.writeProcStateRecord(dir, name, record, nil)
}

// writeProcStateRecord is writeProcState with optional attempt tracking:
// when p is non-nil, the temporary entry is tracked from its descriptor and
// handed to the final name immediately after the rename succeeds. A failed
// rename therefore leaves the temporary entry, while a failed post-rename
// sync leaves the final entry — rollback removes exactly what exists.
func (s *Service) writeProcStateRecord(dir *os.File, name string, record any, p *detachedProc) error {
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	if len(data) > procJSONBound {
		// The same bound the reader enforces: a record we would refuse to
		// read must never be written and then vanish on restart.
		return fmt.Errorf("metadata record %s is %d bytes, above the %d-byte bound", name, len(data), procJSONBound)
	}
	tmp := ".tmp." + name + "." + randSuffix()
	f, err := s.procOpenat(dir, tmp, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	// Track from the descriptor, before any fallible durability step: the
	// name is provably this attempt's from the O_EXCL create on.
	entry, entryErr := entryOf(f, dir, tmp, false)
	if p != nil {
		p.trackOwned(entry)
	}
	if entryErr != nil {
		s.procClose(f)
		return entryErr
	}
	keep := false
	defer func() {
		if !keep {
			_ = s.procUnlinkat(dir, tmp, 0)
		}
	}()
	if _, err := s.procWrite(f, data); err != nil {
		s.procClose(f)
		return err
	}
	if err := s.syncFile(f); err != nil {
		s.procClose(f)
		return err
	}
	if err := s.procClose(f); err != nil {
		return err
	}
	if err := s.procRenameat(dir, tmp, name); err != nil {
		return err
	}
	// The rename landed: from here the object lives under the final name,
	// even if the directory sync below fails.
	if p != nil {
		p.renameOwned(dir, tmp, name)
	}
	keep = true
	return s.syncFile(dir)
}

// readProcState reads and decodes one bounded JSON record under dir. The
// open is nonblocking with a regular-file requirement — a FIFO planted at
// a metadata name must be corruption to skip, not a hang — and a file
// larger than the bound is corruption, not state this service wrote.
func readProcState(dir *os.File, name string, record any) error {
	f, err := openAt(dir, name, unix.O_RDONLY|unix.O_NONBLOCK, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	if fi, err := f.Stat(); err != nil {
		return err
	} else if !fi.Mode().IsRegular() {
		return fmt.Errorf("%s is not a regular file", name)
	}
	data, err := io.ReadAll(io.LimitReader(f, procJSONBound+1))
	if err != nil {
		return err
	}
	if len(data) > procJSONBound {
		return fmt.Errorf("%s: larger than %d bytes", name, procJSONBound)
	}
	return json.Unmarshal(data, record)
}

// --- recovery ---------------------------------------------------------------

// initProcesses scans the processes directory and rebuilds the registry
// from the durable records: EXITED for a validated identity plus a
// validated exit, LOST for a validated identity without one, invisible for
// anything that does not validate. PIDs from disk are never signalable.
func (s *Service) initProcesses() {
	base, err := s.procStateDir()
	if err != nil {
		s.log.Error("guest: process state directory unavailable; StartProcess cannot serve",
			"err", err)
		return
	}
	entries, err := base.Readdirnames(-1)
	if err != nil {
		s.log.Error("guest: cannot scan process state", "err", err)
		return
	}
	for _, name := range entries {
		id, err := strconv.ParseUint(name, 10, 32)
		if err != nil || id == 0 {
			continue // a log file, or not ours
		}
		s.recoverProc(base, uint32(id))
	}
}

// recoverProc rebuilds one registry entry from disk. The metadata must
// validate; an exit record that does not validate leaves the process LOST
// (ended without a recorded exit) rather than guessing a status.
func (s *Service) recoverProc(base *os.File, id uint32) {
	name := strconv.FormatUint(uint64(id), 10)
	dir, err := openAt(base, name, unix.O_RDONLY|unix.O_DIRECTORY, 0)
	if err != nil {
		s.log.Warn("guest: ignoring unreadable process directory", "process_id", id, "err", err)
		return
	}
	var meta procMeta
	if err := readProcState(dir, procMetaName, &meta); err != nil || !validProcMeta(meta, id) {
		dir.Close()
		s.log.Warn("guest: ignoring invalid process metadata", "process_id", id, "err", err)
		return
	}
	rec := &nodev1.Process{
		ProcessId:       id,
		Pid:             int32(meta.Pid),
		State:           nodev1.ProcessState_PROCESS_STATE_LOST,
		Command:         meta.Command,
		Cwd:             meta.Cwd,
		StartedAtUnixMs: meta.StartedAtUnixMs,
		LogPath:         procSpecLogPath(id, false),
		ErrLogPath:      procSpecLogPath(id, true),
	}
	var fin procExit
	switch err := readProcState(dir, procExitName, &fin); {
	case err == nil && validProcExit(fin):
		rec.State = nodev1.ProcessState_PROCESS_STATE_EXITED
		rec.Exit = &nodev1.ExecEvent_Exit{
			ExitCode:   fin.ExitCode,
			Signal:     fin.Signal,
			DurationMs: fin.DurationMs,
		}
		rec.FinishedAtUnixMs = fin.FinishedAtUnixMs
	case errors.Is(err, fs.ErrNotExist):
		// Still running when this service last stopped: LOST.
	default:
		s.log.Warn("guest: unreadable exit record; reporting LOST", "process_id", id, "err", err)
	}
	// The private directory was only needed to read the records; later
	// reads go through the shared processes pin, so no handle is held.
	dir.Close()
	p := &detachedProc{
		rec:   rec,
		ready: make(chan struct{}),
		done:  make(chan struct{}),
	}
	close(p.ready)
	close(p.done)
	s.procMu.Lock()
	defer s.procMu.Unlock()
	if s.procs[id] != nil {
		return // a duplicate cannot happen on one id namespace, but stay safe
	}
	s.procs[id] = p
	s.log.Info("recovered process", "process_id", id, "state", rec.State.String())
}

// validProcMeta checks a recovered identity: this service's record
// version, the id it lives under, and a plausible start. Pid may be 0: the
// crash window between fork and the pid's durable write.
func validProcMeta(m procMeta, id uint32) bool {
	return m.Version == procStateVersion && m.ProcessID == id && m.Pid >= 0 &&
		m.StartedAtUnixMs > 0 && m.Cwd != ""
}

// validProcExit checks a recovered final status: exactly one of an exit
// code in range or a signal name, and a plausible finish time.
func validProcExit(e procExit) bool {
	if e.Version != procStateVersion || e.FinishedAtUnixMs <= 0 {
		return false
	}
	if (e.ExitCode == nil) == (e.Signal == "") {
		return false // exactly one of the two
	}
	return e.ExitCode == nil || (*e.ExitCode >= 0 && *e.ExitCode <= 255)
}

// --- no-follow openat plumbing -----------------------------------------------

// openAt opens name under dir with the given flags, never following a
// symlink at name and never leaking the descriptor into children.
func openAt(dir *os.File, name string, flags int, mode uint32) (*os.File, error) {
	fd, err := unix.Openat(int(dir.Fd()), name, flags|unix.O_CLOEXEC|unix.O_NOFOLLOW, mode)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), dir.Name()+"/"+name), nil
}

// mkdirAt creates a directory under dir, chowning it to the sandbox user
// when newly created, and reports whether it created it. An existing entry
// is fine: callers verify it is a directory by opening it no-follow right
// after.
func mkdirAt(dir *os.File, name string, uid, gid int) (bool, error) {
	if err := unix.Mkdirat(int(dir.Fd()), name, 0o755); err != nil {
		if errors.Is(err, unix.EEXIST) {
			return false, nil
		}
		return false, err
	}
	if err := unix.Fchownat(int(dir.Fd()), name, uid, gid, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return false, err
	}
	return true, nil
}

// renameAt renames name to newName within dir.
func renameAt(dir *os.File, name, newName string) error {
	return unix.Renameat(int(dir.Fd()), name, int(dir.Fd()), newName)
}
