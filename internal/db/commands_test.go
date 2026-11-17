package db_test

// Real-Postgres tests for the Phase 1 detached command database slice (B2.1):
// atomic per-sandbox process id allocation, restart persistence, deletion and
// lifecycle fences, owner isolation, observation ordering and the recorded
// command text contract. They are gated by BBRAFT_TEST_DATABASE_URL through
// dbtest.Open and never report a skip as a pass.

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
)

// commandSandboxFor creates a ready sandbox for an existing owner.
func commandSandboxFor(t *testing.T, s *db.Store, own uuid.UUID) string {
	t.Helper()
	res := create(t, s, db.CreateSandboxParams{Owner: own})
	ready, err := s.TransitionSandbox(t.Context(), db.TransitionParams{
		SandboxID:       res.Sandbox.ID,
		Owner:           own,
		FromStates:      []db.SandboxState{db.StateProvisioning},
		ExpectedVersion: res.Sandbox.Version,
		To:              db.StateReady,
	})
	if err != nil {
		t.Fatalf("ready transition: %v", err)
	}
	return ready.ID
}

// commandReady registers capacity, creates an owner and gives it a ready
// sandbox.
func commandReady(t *testing.T, s *db.Store) (uuid.UUID, string) {
	t.Helper()
	node(t, s, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 40})
	own := owner(t, s)
	return own, commandSandboxFor(t, s, own)
}

// commandAllocate allocates one detached command and fails the test on error.
func commandAllocate(t *testing.T, s *db.Store, own uuid.UUID, sandboxID, command string) db.CommandRecord {
	t.Helper()
	rec, err := s.AllocateCommand(t.Context(), own, db.AllocateCommandParams{
		SandboxID: sandboxID,
		Command:   command,
	})
	if err != nil {
		t.Fatalf("allocate command %q: %v", command, err)
	}
	return rec
}

// commandReopen opens a second Store over the same schema, with fresh pool
// state and no shared in-memory allocation state.
func commandReopen(t *testing.T, s *db.Store) *db.Store {
	t.Helper()
	cfg := freshPoolConfig(t)
	var schema string
	if err := s.Pool().QueryRow(t.Context(), `SELECT current_schema()`).Scan(&schema); err != nil {
		t.Fatalf("current schema: %v", err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	other, err := db.OpenPool(t.Context(), cfg, db.Secrets{})
	if err != nil {
		t.Fatal("reopen store: connection failed")
	}
	t.Cleanup(other.Close)
	return other
}

// commandSameInstant compares at PostgreSQL's microsecond timestamp precision.
func commandSameInstant(a, b time.Time) bool { return a.UnixMicro() == b.UnixMicro() }

func TestCommandAllocateIdentityAndOwnerScope(t *testing.T) {
	s := dbtestOpen(t)
	own, sandboxID := commandReady(t, s)
	cwd := "/home/user/work"

	first, err := s.AllocateCommand(t.Context(), own, db.AllocateCommandParams{
		SandboxID: sandboxID,
		Command:   "echo hi",
		Cwd:       &cwd,
	})
	if err != nil {
		t.Fatalf("allocate first: %v", err)
	}
	if first.ProcessID != 1 {
		t.Fatalf("first process id = %d, want 1", first.ProcessID)
	}
	if !strings.HasPrefix(first.CommandID, "cmd_") || len(first.CommandID) != len("cmd_")+12 {
		t.Fatalf("command id %q does not match the pinned shape", first.CommandID)
	}
	if first.OwnerID != own || first.SandboxID != sandboxID {
		t.Fatalf("identity: owner %s sandbox %s", first.OwnerID, first.SandboxID)
	}
	if first.Mode != db.CommandModeDetached {
		t.Fatalf("mode = %q, want detached", first.Mode)
	}
	if first.Command != "echo hi" {
		t.Fatalf("command = %q", first.Command)
	}
	if first.Cwd == nil || *first.Cwd != cwd {
		t.Fatalf("cwd = %v, want %q", first.Cwd, cwd)
	}
	if first.Status != db.CommandStarting {
		t.Fatalf("status = %q, want internal starting", first.Status)
	}
	if first.PID != 0 || first.ExitCode != nil || first.Signal != nil || first.FinishedAt != nil {
		t.Fatalf("fresh record has observation fields: %+v", first)
	}
	if first.StartedAt.IsZero() || first.CreatedAt.IsZero() {
		t.Fatalf("timestamps missing: started %v created %v", first.StartedAt, first.CreatedAt)
	}
	if first.StdoutBytes != 0 || first.StderrBytes != 0 || first.OutputTruncated {
		t.Fatalf("fresh byte metadata: %+v", first)
	}

	second := commandAllocate(t, s, own, sandboxID, "echo two")
	if second.ProcessID != 2 {
		t.Fatalf("second process id = %d, want 2", second.ProcessID)
	}
	if second.CommandID == first.CommandID {
		t.Fatalf("duplicate command id %q", second.CommandID)
	}

	got, err := s.CommandRecord(t.Context(), own, sandboxID, first.ProcessID)
	if err != nil {
		t.Fatalf("read first: %v", err)
	}
	if got.CommandID != first.CommandID || got.Mode != db.CommandModeDetached ||
		got.Cwd == nil || *got.Cwd != cwd {
		t.Fatalf("read first = %+v, want %+v", got, first)
	}

	// Process ids are per sandbox: another sandbox of the same owner starts
	// again at 1 and stays readable under its own id.
	otherSandbox := commandSandboxFor(t, s, own)
	otherFirst := commandAllocate(t, s, own, otherSandbox, "echo other")
	if otherFirst.ProcessID != 1 {
		t.Fatalf("other sandbox first process id = %d, want 1", otherFirst.ProcessID)
	}
	if otherFirst.CommandID == first.CommandID {
		t.Fatalf("command ids are not unique across sandboxes")
	}
	if _, err := s.CommandRecord(t.Context(), own, otherSandbox, first.ProcessID); err != nil {
		t.Fatalf("other sandbox id 1 should exist: %v", err)
	}

	// A foreign owner cannot allocate, read or update any of it.
	foreign := owner(t, s)
	if _, err := s.AllocateCommand(t.Context(), foreign, db.AllocateCommandParams{SandboxID: sandboxID, Command: "x"}); !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("foreign allocate: %v, want ErrNotFound", err)
	}
	if _, err := s.CommandRecord(t.Context(), foreign, sandboxID, first.ProcessID); !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("foreign read: %v, want ErrNotFound", err)
	}
	if _, err := s.ObserveCommand(t.Context(), foreign, sandboxID, first.ProcessID, db.CommandObservation{Status: db.CommandRunning, PID: 1}); !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("foreign observe: %v, want ErrNotFound", err)
	}

	if _, err := s.CommandRecord(t.Context(), own, sandboxID, 0); !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("process id 0 read: %v, want ErrNotFound", err)
	}
	if _, err := s.CommandRecord(t.Context(), own, sandboxID, 99); !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("unknown process read: %v, want ErrNotFound", err)
	}
	if _, err := s.AllocateCommand(t.Context(), own, db.AllocateCommandParams{SandboxID: "bx_unknown2", Command: "x"}); !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("unknown sandbox allocate: %v, want ErrNotFound", err)
	}
}

func TestCommandAllocateConcurrent(t *testing.T) {
	s := dbtestOpen(t)
	own, sandboxID := commandReady(t, s)

	const n = 32
	var (
		wg   sync.WaitGroup
		recs = make([]db.CommandRecord, n)
		errs = make([]error, n)
	)
	for i := range n {
		wg.Go(func() {
			recs[i], errs[i] = s.AllocateCommand(t.Context(), own, db.AllocateCommandParams{
				SandboxID: sandboxID,
				Command:   "echo concurrent",
			})
		})
	}
	wg.Wait()

	seenProcess := make(map[uint32]bool, n)
	seenCommand := make(map[string]bool, n)
	for i := range n {
		if errs[i] != nil {
			t.Fatalf("allocate %d: %v", i, errs[i])
		}
		if seenProcess[recs[i].ProcessID] {
			t.Fatalf("process id %d allocated twice", recs[i].ProcessID)
		}
		if seenCommand[recs[i].CommandID] {
			t.Fatalf("command id %q allocated twice", recs[i].CommandID)
		}
		seenProcess[recs[i].ProcessID] = true
		seenCommand[recs[i].CommandID] = true
	}
	for id := uint32(1); id <= n; id++ {
		if !seenProcess[id] {
			t.Fatalf("process id %d missing from %v", id, seenProcess)
		}
	}
	var last int64
	if err := s.Pool().QueryRow(t.Context(), `
		SELECT last_process_id FROM command_process_counters WHERE sandbox_id = $1`,
		sandboxID).Scan(&last); err != nil {
		t.Fatalf("counter: %v", err)
	}
	if last != n {
		t.Fatalf("counter = %d, want %d", last, n)
	}
}

func TestCommandAllocateExhaustion(t *testing.T) {
	s := dbtestOpen(t)
	own, sandboxID := commandReady(t, s)
	if _, err := s.Pool().Exec(t.Context(), `
		INSERT INTO command_process_counters (sandbox_id, last_process_id)
		VALUES ($1, 4294967294)`, sandboxID); err != nil {
		t.Fatalf("seed counter: %v", err)
	}

	last, err := s.AllocateCommand(t.Context(), own, db.AllocateCommandParams{SandboxID: sandboxID, Command: "max"})
	if err != nil {
		t.Fatalf("allocate max: %v", err)
	}
	if last.ProcessID != 4294967295 {
		t.Fatalf("max process id = %d, want 4294967295", last.ProcessID)
	}

	for range 2 {
		if _, err := s.AllocateCommand(t.Context(), own, db.AllocateCommandParams{SandboxID: sandboxID, Command: "past max"}); !errors.Is(err, db.ErrCommandIDExhausted) {
			t.Fatalf("allocate past max: %v, want ErrCommandIDExhausted", err)
		}
	}
	var counter int64
	if err := s.Pool().QueryRow(t.Context(), `
		SELECT last_process_id FROM command_process_counters WHERE sandbox_id = $1`,
		sandboxID).Scan(&counter); err != nil {
		t.Fatalf("counter: %v", err)
	}
	if counter != 4294967295 {
		t.Fatalf("counter wrapped or changed: %d", counter)
	}
	if got := count(t, s, `SELECT count(*) FROM command_records WHERE sandbox_id = $1`, sandboxID); got != 1 {
		t.Fatalf("records after refusals = %d, want 1", got)
	}
	if _, err := s.CommandRecord(t.Context(), own, sandboxID, 4294967295); err != nil {
		t.Fatalf("read max record: %v", err)
	}
}

func TestCommandAllocateRollbackConsumesNoID(t *testing.T) {
	s := dbtestOpen(t)
	node(t, s, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 40})
	own := owner(t, s)
	res := create(t, s, db.CreateSandboxParams{Owner: own})
	sandboxID := res.Sandbox.ID

	// Provisioning is not a command-ready state: the allocation transaction
	// rolls back and hands out nothing.
	if _, err := s.AllocateCommand(t.Context(), own, db.AllocateCommandParams{SandboxID: sandboxID, Command: "too early"}); !errors.Is(err, db.ErrInvalidState) {
		t.Fatalf("provisioning allocate: %v, want ErrInvalidState", err)
	}
	if got := count(t, s, `SELECT count(*) FROM command_process_counters WHERE sandbox_id = $1`, sandboxID); got != 0 {
		t.Fatalf("counter rows after refused allocate = %d, want 0", got)
	}
	if got := count(t, s, `SELECT count(*) FROM command_records WHERE sandbox_id = $1`, sandboxID); got != 0 {
		t.Fatalf("records after refused allocate = %d, want 0", got)
	}

	ready, err := s.TransitionSandbox(t.Context(), db.TransitionParams{
		SandboxID:       sandboxID,
		Owner:           own,
		FromStates:      []db.SandboxState{db.StateProvisioning},
		ExpectedVersion: res.Sandbox.Version,
		To:              db.StateReady,
	})
	if err != nil {
		t.Fatalf("ready transition: %v", err)
	}
	if ready.State != db.StateReady {
		t.Fatalf("state = %q", ready.State)
	}

	// Invalid UTF-8 is rejected before the counter, so it consumes nothing.
	if _, err := s.AllocateCommand(t.Context(), own, db.AllocateCommandParams{
		SandboxID: sandboxID,
		Command:   string([]byte{0xff, 'x'}),
	}); !errors.Is(err, db.ErrInvalidCommand) {
		t.Fatalf("invalid UTF-8 allocate: %v, want ErrInvalidCommand", err)
	}
	if got := count(t, s, `SELECT count(*) FROM command_process_counters WHERE sandbox_id = $1`, sandboxID); got != 0 {
		t.Fatalf("counter rows after invalid text = %d, want 0", got)
	}

	first := commandAllocate(t, s, own, sandboxID, "ok")
	if first.ProcessID != 1 {
		t.Fatalf("first committed id = %d, want 1", first.ProcessID)
	}

	// A definite start failure keeps the committed id and the record; the
	// next allocation advances instead of reusing it.
	failed, err := s.ObserveCommand(t.Context(), own, sandboxID, first.ProcessID, db.CommandObservation{
		Status: db.CommandFailed,
	})
	if err != nil {
		t.Fatalf("observe failed start: %v", err)
	}
	if failed.Status != db.CommandFailed || failed.FinishedAt == nil {
		t.Fatalf("failed observation = %+v", failed)
	}
	second := commandAllocate(t, s, own, sandboxID, "next")
	if second.ProcessID != 2 {
		t.Fatalf("id after failed start = %d, want 2", second.ProcessID)
	}
	kept, err := s.CommandRecord(t.Context(), own, sandboxID, first.ProcessID)
	if err != nil || kept.CommandID != first.CommandID || kept.Status != db.CommandFailed {
		t.Fatalf("failed record: %+v err=%v", kept, err)
	}
}

func TestCommandAllocateDeletionFence(t *testing.T) {
	s := dbtestOpen(t)
	own, sandboxID := commandReady(t, s)
	rec := commandAllocate(t, s, own, sandboxID, "long lived")

	acceptDelete(t, s, own, sandboxID)
	if _, err := s.AllocateCommand(t.Context(), own, db.AllocateCommandParams{SandboxID: sandboxID, Command: "after delete"}); !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("allocate after delete: %v, want ErrNotFound", err)
	}
	if _, err := s.CommandRecord(t.Context(), own, sandboxID, rec.ProcessID); !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("read after delete: %v, want ErrNotFound", err)
	}
	if _, err := s.ObserveCommand(t.Context(), own, sandboxID, rec.ProcessID, db.CommandObservation{
		Status: db.CommandLost,
	}); !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("observe after delete: %v, want ErrNotFound", err)
	}
}

func TestCommandAllocateSerializesWithDeletion(t *testing.T) {
	s := dbtestOpen(t)
	own, sandboxID := commandReady(t, s)

	tx, err := s.Pool().Begin(t.Context())
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	var locked int
	if err := tx.QueryRow(t.Context(), `
		SELECT 1 FROM sandboxes WHERE id = $1 FOR UPDATE`, sandboxID).Scan(&locked); err != nil {
		t.Fatalf("lock sandbox: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := s.AllocateCommand(t.Context(), own, db.AllocateCommandParams{SandboxID: sandboxID, Command: "races delete"})
		done <- err
	}()
	committed, joined := false, false
	defer func() {
		if !committed {
			_ = tx.Rollback(context.WithoutCancel(t.Context()))
		}
		if joined {
			return
		}
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("allocation goroutine did not finish after rollback")
		}
	}()

	commandWaitForSandboxLock(t, s)
	select {
	case err := <-done:
		joined = true
		t.Fatalf("allocation passed a locked sandbox: %v", err)
	default:
	}

	// The deletion acceptance holds the same sandbox lock; soft-delete and
	// commit, then the waiting allocation must observe the deletion.
	if _, err := tx.Exec(t.Context(), `UPDATE sandboxes SET deleted_at = now() WHERE id = $1`, sandboxID); err != nil {
		t.Fatalf("soft delete: %v", err)
	}
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatalf("commit: %v", err)
	}
	committed = true
	select {
	case err := <-done:
		joined = true
		if !errors.Is(err, db.ErrNotFound) {
			t.Fatalf("allocation after committed delete: %v, want ErrNotFound", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("allocation did not finish after the delete committed")
	}
	if got := count(t, s, `SELECT count(*) FROM command_records WHERE sandbox_id = $1`, sandboxID); got != 0 {
		t.Fatalf("records after fenced allocation = %d, want 0", got)
	}
}

// commandWaitForSandboxLock blocks until a backend of this test's own store is
// waiting for a row lock on a sandboxes FOR UPDATE query. The caller holds
// that lock, so this proves the competing store call reached its fence
// instead of relying on a sleep. datname and the per-store application_name
// keep concurrent tests on other schemas from satisfying the barrier.
func commandWaitForSandboxLock(t *testing.T, s *db.Store) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		var waiting int
		if err := s.Pool().QueryRow(t.Context(), `
			SELECT count(*) FROM pg_stat_activity
			WHERE wait_event_type = 'Lock'
				AND datname = current_database()
				AND application_name = current_setting('application_name')
				AND query ILIKE '%FROM sandboxes%'
				AND query ILIKE '%FOR UPDATE%'`).Scan(&waiting); err != nil {
			t.Fatalf("inspect lock waits: %v", err)
		}
		if waiting > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("no backend waited for the sandbox row lock")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestCommandObservationOrdering(t *testing.T) {
	s := dbtestOpen(t)
	own, sandboxID := commandReady(t, s)
	rec := commandAllocate(t, s, own, sandboxID, "echo observe")

	started := time.Now().Add(-time.Minute)
	canonicalCwd := "/home/user/project"
	running, err := s.ObserveCommand(t.Context(), own, sandboxID, rec.ProcessID, db.CommandObservation{
		Status:      db.CommandRunning,
		PID:         4321,
		StartedAt:   &started,
		Cwd:         &canonicalCwd,
		LogPath:     "/home/user/.bbraft/processes/4321.log",
		ErrLogPath:  "/home/user/.bbraft/processes/4321.err.log",
		StdoutBytes: 10,
		StderrBytes: 4,
	})
	if err != nil {
		t.Fatalf("observe running: %v", err)
	}
	if running.Status != db.CommandRunning || running.PID != 4321 {
		t.Fatalf("running = %+v", running)
	}
	if !commandSameInstant(running.StartedAt, started) {
		t.Fatalf("started_at = %v, want node time %v", running.StartedAt, started)
	}
	if running.Cwd == nil || *running.Cwd != canonicalCwd {
		t.Fatalf("cwd = %v, want canonical %q", running.Cwd, canonicalCwd)
	}
	if running.LogPath != "/home/user/.bbraft/processes/4321.log" ||
		running.ErrLogPath != "/home/user/.bbraft/processes/4321.err.log" {
		t.Fatalf("log paths = %q %q", running.LogPath, running.ErrLogPath)
	}
	if running.StdoutBytes != 10 || running.StderrBytes != 4 {
		t.Fatalf("byte counts = %d %d", running.StdoutBytes, running.StderrBytes)
	}

	finished := time.Now().Add(-time.Second)
	exitCode := int32(7)
	exited, err := s.ObserveCommand(t.Context(), own, sandboxID, rec.ProcessID, db.CommandObservation{
		Status:          db.CommandExited,
		PID:             4321,
		ExitCode:        &exitCode,
		FinishedAt:      &finished,
		StdoutBytes:     120,
		StderrBytes:     9,
		OutputTruncated: true,
	})
	if err != nil {
		t.Fatalf("observe exited: %v", err)
	}
	if exited.Status != db.CommandExited || exited.ExitCode == nil || *exited.ExitCode != 7 {
		t.Fatalf("exited = %+v", exited)
	}
	if exited.FinishedAt == nil || !commandSameInstant(*exited.FinishedAt, finished) {
		t.Fatalf("finished_at = %v, want %v", exited.FinishedAt, finished)
	}
	if exited.LogPath == "" || exited.ErrLogPath == "" {
		t.Fatalf("final observation dropped log paths: %+v", exited)
	}
	if exited.StdoutBytes != 120 || exited.StderrBytes != 9 || !exited.OutputTruncated {
		t.Fatalf("final byte metadata = %+v", exited)
	}

	// An out-of-order running observation must not regress the final state,
	// pid, exit, start/finish time or cwd; smaller counts never shrink the
	// record.
	staleStarted := started.Add(30 * time.Second)
	staleCwd := "/tmp/stale"
	stale, err := s.ObserveCommand(t.Context(), own, sandboxID, rec.ProcessID, db.CommandObservation{
		Status:      db.CommandRunning,
		PID:         9999,
		StartedAt:   &staleStarted,
		Cwd:         &staleCwd,
		StdoutBytes: 5,
		StderrBytes: 2,
	})
	if err != nil {
		t.Fatalf("observe stale running: %v", err)
	}
	if stale.Status != db.CommandExited || stale.PID != 4321 || stale.ExitCode == nil || *stale.ExitCode != 7 {
		t.Fatalf("stale running regressed state: %+v", stale)
	}
	if stale.FinishedAt == nil || !commandSameInstant(*stale.FinishedAt, finished) {
		t.Fatalf("stale running changed finished_at: %v", stale.FinishedAt)
	}
	if !commandSameInstant(stale.StartedAt, started) {
		t.Fatalf("stale running changed started_at: %v", stale.StartedAt)
	}
	if stale.Cwd == nil || *stale.Cwd != canonicalCwd {
		t.Fatalf("stale running changed cwd: %v", stale.Cwd)
	}
	if stale.StdoutBytes != 120 || stale.StderrBytes != 9 || !stale.OutputTruncated {
		t.Fatalf("stale running shrank metadata: %+v", stale)
	}

	// A conflicting later final observation keeps the first terminal state
	// but still merges larger byte counts.
	lostFinal := time.Now()
	merged, err := s.ObserveCommand(t.Context(), own, sandboxID, rec.ProcessID, db.CommandObservation{
		Status:      db.CommandLost,
		FinishedAt:  &lostFinal,
		StdoutBytes: 130,
		StderrBytes: 1,
	})
	if err != nil {
		t.Fatalf("observe stale final: %v", err)
	}
	if merged.Status != db.CommandExited {
		t.Fatalf("second final status = %q, want sticky exited", merged.Status)
	}
	if merged.FinishedAt == nil || !commandSameInstant(*merged.FinishedAt, finished) {
		t.Fatalf("second final changed finished_at: %v", merged.FinishedAt)
	}
	if merged.StdoutBytes != 130 || merged.StderrBytes != 9 {
		t.Fatalf("merged counts = %d %d", merged.StdoutBytes, merged.StderrBytes)
	}

	read, err := s.CommandRecord(t.Context(), own, sandboxID, rec.ProcessID)
	if err != nil {
		t.Fatalf("read final: %v", err)
	}
	if read.Status != db.CommandExited || read.StdoutBytes != 130 {
		t.Fatalf("read final = %+v", read)
	}
}

func TestCommandObservationLostAndInactiveSandbox(t *testing.T) {
	s := dbtestOpen(t)
	own, sandboxID := commandReady(t, s)
	rec := commandAllocate(t, s, own, sandboxID, "sleep 100")

	if _, err := s.ObserveCommand(t.Context(), own, sandboxID, rec.ProcessID, db.CommandObservation{
		Status: db.CommandRunning, PID: 777,
	}); err != nil {
		t.Fatalf("observe running: %v", err)
	}

	archived, err := s.TransitionSandbox(t.Context(), db.TransitionParams{
		SandboxID:       sandboxID,
		Owner:           own,
		FromStates:      []db.SandboxState{db.StateReady},
		ExpectedVersion: 2,
		To:              db.StateArchived,
	})
	if err != nil {
		t.Fatalf("archive transition: %v", err)
	}
	if archived.State != db.StateArchived {
		t.Fatalf("state = %q", archived.State)
	}
	if _, err := s.AllocateCommand(t.Context(), own, db.AllocateCommandParams{SandboxID: sandboxID, Command: "while archived"}); !errors.Is(err, db.ErrInvalidState) {
		t.Fatalf("allocate while archived: %v, want ErrInvalidState", err)
	}

	// The record stays readable while stopped, and a late LOST observation
	// from the node still lands.
	if _, err := s.CommandRecord(t.Context(), own, sandboxID, rec.ProcessID); err != nil {
		t.Fatalf("read while archived: %v", err)
	}
	lost, err := s.ObserveCommand(t.Context(), own, sandboxID, rec.ProcessID, db.CommandObservation{
		Status: db.CommandLost,
	})
	if err != nil {
		t.Fatalf("late lost observation: %v", err)
	}
	if lost.Status != db.CommandLost || lost.ExitCode != nil {
		t.Fatalf("lost = %+v", lost)
	}
	// Process.finished_at_unix_ms is 0 for LOST: an unknown finish time
	// stays NULL instead of being fabricated.
	if lost.FinishedAt != nil {
		t.Fatalf("lost finished_at = %v, want NULL", lost.FinishedAt)
	}
	if lost.PID != 777 {
		t.Fatalf("lost pid = %d, want 777", lost.PID)
	}

	readLost, err := s.CommandRecord(t.Context(), own, sandboxID, rec.ProcessID)
	if err != nil {
		t.Fatalf("read lost: %v", err)
	}
	if readLost.Status != db.CommandLost || readLost.FinishedAt != nil {
		t.Fatalf("read lost = %+v, want lost with NULL finished_at", readLost)
	}

	// A stale running observation cannot invent a finish time on the final
	// LOST record.
	stale, err := s.ObserveCommand(t.Context(), own, sandboxID, rec.ProcessID, db.CommandObservation{
		Status: db.CommandRunning, PID: 778,
	})
	if err != nil {
		t.Fatalf("stale running after lost: %v", err)
	}
	if stale.Status != db.CommandLost || stale.PID != 777 || stale.FinishedAt != nil {
		t.Fatalf("stale running changed lost record: %+v", stale)
	}
}

func TestCommandObservationLostKnownFinish(t *testing.T) {
	s := dbtestOpen(t)
	own, sandboxID := commandReady(t, s)
	rec := commandAllocate(t, s, own, sandboxID, "finished lost")

	finished := time.Now().Add(-time.Second)
	lost, err := s.ObserveCommand(t.Context(), own, sandboxID, rec.ProcessID, db.CommandObservation{
		Status:     db.CommandLost,
		FinishedAt: &finished,
	})
	if err != nil {
		t.Fatalf("lost with known finish: %v", err)
	}
	if lost.Status != db.CommandLost || lost.FinishedAt == nil || !commandSameInstant(*lost.FinishedAt, finished) {
		t.Fatalf("lost known finish = %+v, want %v", lost, finished)
	}
	read, err := s.CommandRecord(t.Context(), own, sandboxID, rec.ProcessID)
	if err != nil {
		t.Fatalf("read lost: %v", err)
	}
	if read.FinishedAt == nil || !commandSameInstant(*read.FinishedAt, finished) {
		t.Fatalf("read finished_at = %v, want %v", read.FinishedAt, finished)
	}
}

func TestCommandObservationValidation(t *testing.T) {
	s := dbtestOpen(t)
	own, sandboxID := commandReady(t, s)
	rec := commandAllocate(t, s, own, sandboxID, "validate")

	for _, tc := range []struct {
		name string
		obs  db.CommandObservation
		want error
	}{
		{name: "starting is not observable", obs: db.CommandObservation{Status: db.CommandStarting}, want: db.ErrInvalidCommandStatus},
		{name: "unknown status", obs: db.CommandObservation{Status: "bogus"}, want: db.ErrInvalidCommandStatus},
		{name: "negative pid", obs: db.CommandObservation{Status: db.CommandRunning, PID: -1}, want: db.ErrInvalidCommand},
		{name: "negative stdout", obs: db.CommandObservation{Status: db.CommandRunning, StdoutBytes: -1}, want: db.ErrInvalidCommand},
		{name: "nul cwd", obs: db.CommandObservation{Status: db.CommandRunning, Cwd: new("/home/user/a\x00b")}, want: db.ErrInvalidCommand},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := s.ObserveCommand(t.Context(), own, sandboxID, rec.ProcessID, tc.obs); !errors.Is(err, tc.want) {
				t.Fatalf("observe: %v, want %v", err, tc.want)
			}
		})
	}
	if _, err := s.ObserveCommand(t.Context(), own, sandboxID, rec.ProcessID, db.CommandObservation{
		Status: db.CommandRunning, PID: 11,
	}); err != nil {
		t.Fatalf("valid observation after rejections: %v", err)
	}
	if _, err := s.ObserveCommand(t.Context(), own, sandboxID, 99, db.CommandObservation{Status: db.CommandRunning}); !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("unknown process observation: %v, want ErrNotFound", err)
	}
}

func TestCommandAllocationSurvivesStoreRestart(t *testing.T) {
	s := dbtestOpen(t)
	own, sandboxID := commandReady(t, s)
	first := commandAllocate(t, s, own, sandboxID, "before restart")
	if _, err := s.ObserveCommand(t.Context(), own, sandboxID, first.ProcessID, db.CommandObservation{
		Status: db.CommandRunning, PID: 1,
	}); err != nil {
		t.Fatalf("observe before restart: %v", err)
	}

	restarted := commandReopen(t, s)
	read, err := restarted.CommandRecord(t.Context(), own, sandboxID, first.ProcessID)
	if err != nil {
		t.Fatalf("read after restart: %v", err)
	}
	if read.CommandID != first.CommandID || read.Status != db.CommandRunning {
		t.Fatalf("record after restart = %+v", read)
	}
	second := commandAllocate(t, restarted, own, sandboxID, "after restart")
	if second.ProcessID != 2 {
		t.Fatalf("id after restart = %d, want 2", second.ProcessID)
	}
	third := commandAllocate(t, s, own, sandboxID, "back on first store")
	if third.ProcessID != 3 {
		t.Fatalf("id on first store after restart = %d, want 3", third.ProcessID)
	}
}

func TestCommandRecordedTextContract(t *testing.T) {
	longASCII := strings.Repeat("a", 70000)
	multibyte := "a" + strings.Repeat("é", 40000)

	tests := []struct {
		name       string
		command    string
		cwd        *string
		wantStored int
		wantErr    error
	}{
		{name: "empty command", command: "", wantStored: 0},
		{name: "exact limit", command: strings.Repeat("b", 65536), wantStored: 65536},
		{name: "long ascii", command: longASCII, wantStored: 65536},
		{name: "multibyte boundary", command: multibyte, wantStored: 65535},
		{name: "invalid utf8", command: string([]byte{0xff}), wantErr: db.ErrInvalidCommand},
		{name: "nul command", command: "echo\x00hi", wantErr: db.ErrInvalidCommand},
		{name: "invalid utf8 cwd", command: "ok", cwd: new(string([]byte{0xfe})), wantErr: db.ErrInvalidCommand},
		{name: "nul cwd", command: "ok", cwd: new("/home/user/a\x00b"), wantErr: db.ErrInvalidCommand},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := dbtestOpen(t)
			own, sandboxID := commandReady(t, s)
			rec, err := s.AllocateCommand(t.Context(), own, db.AllocateCommandParams{
				SandboxID: sandboxID,
				Command:   tc.command,
				Cwd:       tc.cwd,
			})
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("allocate: %v, want %v", err, tc.wantErr)
				}
				if got := count(t, s, `SELECT count(*) FROM command_records WHERE sandbox_id = $1`, sandboxID); got != 0 {
					t.Fatalf("records after rejected text = %d, want 0", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("allocate: %v", err)
			}
			if len(rec.Command) != tc.wantStored {
				t.Fatalf("stored length = %d, want %d", len(rec.Command), tc.wantStored)
			}
			if !strings.HasPrefix(tc.command, rec.Command) {
				t.Fatalf("stored command is not a prefix of the original")
			}
			if !utf8.ValidString(rec.Command) {
				t.Fatal("stored command is not valid UTF-8")
			}
			read, err := s.CommandRecord(t.Context(), own, sandboxID, rec.ProcessID)
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			if read.Command != rec.Command {
				t.Fatalf("read command differs from allocated command")
			}
		})
	}

	// A cwd is stored as supplied, without an invented path-length cap.
	s := dbtestOpen(t)
	own, sandboxID := commandReady(t, s)
	longCwd := "/home/user/" + strings.Repeat("d", 10000)
	rec, err := s.AllocateCommand(t.Context(), own, db.AllocateCommandParams{
		SandboxID: sandboxID,
		Command:   "pwd",
		Cwd:       &longCwd,
	})
	if err != nil {
		t.Fatalf("allocate long cwd: %v", err)
	}
	if rec.Cwd == nil || *rec.Cwd != longCwd {
		t.Fatalf("long cwd = %v", rec.Cwd)
	}
}

func TestCommandAllocateTransactionRollbackAfterCounter(t *testing.T) {
	s := dbtestOpen(t)
	own, sandboxID := commandReady(t, s)

	// The counter advances before the record insert. A database failure at
	// the insert must roll that advance back, not leak an id.
	if _, err := s.Pool().Exec(t.Context(), `
		CREATE OR REPLACE FUNCTION command_reject_record_insert()
		RETURNS trigger LANGUAGE plpgsql AS $fn$
		BEGIN
			RAISE EXCEPTION 'test rejects command record insert';
		END
		$fn$`); err != nil {
		t.Fatalf("create trigger function: %v", err)
	}
	if _, err := s.Pool().Exec(t.Context(), `
		CREATE TRIGGER command_reject_record_insert
		BEFORE INSERT ON command_records
		FOR EACH ROW EXECUTE FUNCTION command_reject_record_insert()`); err != nil {
		t.Fatalf("create trigger: %v", err)
	}

	_, err := s.AllocateCommand(t.Context(), own, db.AllocateCommandParams{SandboxID: sandboxID, Command: "rolled back"})
	if err == nil {
		t.Fatal("allocation succeeded despite the rejecting trigger")
	}
	if !strings.Contains(err.Error(), "insert command record") {
		t.Fatalf("allocation failed before the record insert: %v", err)
	}
	if got := count(t, s, `SELECT count(*) FROM command_process_counters WHERE sandbox_id = $1`, sandboxID); got != 0 {
		t.Fatalf("counter rows after rollback = %d, want 0", got)
	}
	if got := count(t, s, `SELECT count(*) FROM command_records WHERE sandbox_id = $1`, sandboxID); got != 0 {
		t.Fatalf("records after rollback = %d, want 0", got)
	}

	if _, err := s.Pool().Exec(t.Context(), `DROP TRIGGER command_reject_record_insert ON command_records`); err != nil {
		t.Fatalf("drop trigger: %v", err)
	}
	rec := commandAllocate(t, s, own, sandboxID, "after rollback")
	if rec.ProcessID != 1 {
		t.Fatalf("first committed id after rollback = %d, want 1", rec.ProcessID)
	}
}

func TestCommandObserveSerializesWithDeletion(t *testing.T) {
	s := dbtestOpen(t)
	own, sandboxID := commandReady(t, s)
	rec := commandAllocate(t, s, own, sandboxID, "observe race")
	if _, err := s.ObserveCommand(t.Context(), own, sandboxID, rec.ProcessID, db.CommandObservation{
		Status: db.CommandRunning, PID: 55,
	}); err != nil {
		t.Fatalf("observe running: %v", err)
	}

	tx, err := s.Pool().Begin(t.Context())
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	var locked int
	if err := tx.QueryRow(t.Context(), `
		SELECT 1 FROM sandboxes WHERE id = $1 FOR UPDATE`, sandboxID).Scan(&locked); err != nil {
		t.Fatalf("lock sandbox: %v", err)
	}

	exitCode := int32(0)
	done := make(chan error, 1)
	go func() {
		_, err := s.ObserveCommand(t.Context(), own, sandboxID, rec.ProcessID, db.CommandObservation{
			Status:   db.CommandExited,
			ExitCode: &exitCode,
		})
		done <- err
	}()
	committed, joined := false, false
	defer func() {
		if !committed {
			_ = tx.Rollback(context.WithoutCancel(t.Context()))
		}
		if joined {
			return
		}
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("observation goroutine did not finish after rollback")
		}
	}()

	commandWaitForSandboxLock(t, s)
	select {
	case err := <-done:
		joined = true
		t.Fatalf("observation passed a locked sandbox: %v", err)
	default:
	}

	if _, err := tx.Exec(t.Context(), `UPDATE sandboxes SET deleted_at = now() WHERE id = $1`, sandboxID); err != nil {
		t.Fatalf("soft delete: %v", err)
	}
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatalf("commit: %v", err)
	}
	committed = true
	select {
	case err := <-done:
		joined = true
		if !errors.Is(err, db.ErrNotFound) {
			t.Fatalf("observation after committed delete: %v, want ErrNotFound", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("observation did not finish after the delete committed")
	}

	var status db.CommandStatus
	if err := s.Pool().QueryRow(t.Context(), `
		SELECT status FROM command_records
		WHERE owner_id = $1 AND sandbox_id = $2 AND process_id = $3`,
		own, sandboxID, rec.ProcessID).Scan(&status); err != nil {
		t.Fatalf("read raw status: %v", err)
	}
	if status != db.CommandRunning {
		t.Fatalf("fenced observation changed status to %q", status)
	}
}

func TestCommandMigrationRollbackRefusal(t *testing.T) {
	s := dbtestOpen(t)
	own, sandboxID := commandReady(t, s)
	rec := commandAllocate(t, s, own, sandboxID, "kept")

	provider := phase1Provider(t, s)
	if _, err := provider.DownTo(t.Context(), 4); err == nil {
		t.Fatal("rollback dropped allocated command ids")
	} else if !strings.Contains(err.Error(), "cannot roll back phase 1 commands: command records exist") {
		t.Fatalf("rollback error = %v", err)
	}
	if got := phase1SchemaVersion(t, s); got != 5 {
		t.Fatalf("schema version after refused rollback = %d, want 5", got)
	}
	after, err := s.CommandRecord(t.Context(), own, sandboxID, rec.ProcessID)
	if err != nil {
		t.Fatalf("record after refused rollback: %v", err)
	}
	if after.CommandID != rec.CommandID || after.ProcessID != rec.ProcessID || after.Status != rec.Status {
		t.Fatalf("record changed across refused rollback: %+v, want %+v", after, rec)
	}
	var last int64
	if err := s.Pool().QueryRow(t.Context(), `
		SELECT last_process_id FROM command_process_counters WHERE sandbox_id = $1`,
		sandboxID).Scan(&last); err != nil {
		t.Fatalf("counter after refused rollback: %v", err)
	}
	if last != 1 {
		t.Fatalf("counter after refused rollback = %d, want 1", last)
	}
}

// TestCommandMigrationCounterOnlyRefusal covers a counter that handed out ids
// without a surviving record row: rolling back would reissue them.
func TestCommandMigrationCounterOnlyRefusal(t *testing.T) {
	s := dbtestOpen(t)
	own, sandboxID := commandReady(t, s)
	if _, err := s.Pool().Exec(t.Context(), `
		INSERT INTO command_process_counters (sandbox_id, last_process_id)
		VALUES ($1, 4294967295)`, sandboxID); err != nil {
		t.Fatalf("seed counter: %v", err)
	}
	if got := count(t, s, `SELECT count(*) FROM command_records WHERE sandbox_id = $1`, sandboxID); got != 0 {
		t.Fatalf("records before rollback = %d, want 0", got)
	}

	provider := phase1Provider(t, s)
	if _, err := provider.DownTo(t.Context(), 4); err == nil {
		t.Fatal("rollback dropped an exhausted process id counter")
	} else if !strings.Contains(err.Error(), "cannot roll back phase 1 commands: process ids were allocated") {
		t.Fatalf("rollback error = %v", err)
	}
	if got := phase1SchemaVersion(t, s); got != 5 {
		t.Fatalf("schema version after refused rollback = %d, want 5", got)
	}
	var last int64
	if err := s.Pool().QueryRow(t.Context(), `
		SELECT last_process_id FROM command_process_counters WHERE sandbox_id = $1`,
		sandboxID).Scan(&last); err != nil {
		t.Fatalf("counter after refused rollback: %v", err)
	}
	if last != 4294967295 {
		t.Fatalf("counter after refused rollback = %d, want 4294967295", last)
	}
	// Restore later migrations before the allocation re-reads the sandbox
	// row: 00006 adds the SSH metadata columns sandboxCols scans.
	if _, err := provider.Up(t.Context()); err != nil {
		t.Fatalf("re-apply later migrations: %v", err)
	}
	if _, err := s.AllocateCommand(t.Context(), own, db.AllocateCommandParams{SandboxID: sandboxID, Command: "still exhausted"}); !errors.Is(err, db.ErrCommandIDExhausted) {
		t.Fatalf("allocation after refused rollback: %v, want ErrCommandIDExhausted", err)
	}
}

func TestCommandMigrationEmptyRoundTrip(t *testing.T) {
	s := dbtestOpen(t)
	provider := phase1Provider(t, s)
	if _, err := provider.DownTo(t.Context(), 4); err != nil {
		t.Fatalf("empty rollback: %v", err)
	}
	var present *string
	if err := s.Pool().QueryRow(t.Context(), `SELECT to_regclass('command_records')::text`).Scan(&present); err != nil {
		t.Fatalf("command_records after rollback: %v", err)
	}
	if present != nil {
		t.Fatalf("command_records still present after rollback: %s", *present)
	}
	if _, err := provider.UpTo(t.Context(), 5); err != nil {
		t.Fatalf("re-apply through 5: %v", err)
	}
	if got := phase1SchemaVersion(t, s); got != 5 {
		t.Fatalf("schema version after 00005 re-apply = %d, want 5", got)
	}
	if err := s.Pool().QueryRow(t.Context(), `SELECT to_regclass('command_records')::text`).Scan(&present); err != nil {
		t.Fatalf("command_records after 00005 re-apply: %v", err)
	}
	if present == nil {
		t.Fatal("command_records missing after 00005 re-apply")
	}
	// Restore every later migration before fixtures read the sandbox row:
	// 00006 adds the SSH metadata columns that sandboxCols scans.
	if _, err := provider.Up(t.Context()); err != nil {
		t.Fatalf("re-apply later migrations: %v", err)
	}
	if got := phase1SchemaVersion(t, s); got != 6 {
		t.Fatalf("schema version after later migrations = %d, want 6", got)
	}
	own, sandboxID := commandReady(t, s)
	rec := commandAllocate(t, s, own, sandboxID, "fresh schema")
	if rec.ProcessID != 1 {
		t.Fatalf("first id after re-apply = %d, want 1", rec.ProcessID)
	}
}
