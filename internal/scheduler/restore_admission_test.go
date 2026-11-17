package scheduler_test

// Create-from admission order and budget: replay first, resolve inside the
// same failFast budget, and a bounded keyed recheck before any failure is
// reported. These are deterministic unit tests with a fake store; they do not
// need PostgreSQL or a real node.

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/scheduler"
)

type restoreStoreFake struct {
	replay  func(call int) (db.CreateSandboxResult, bool, error)
	calls   int
	created []db.CreateSandboxParams
}

func (s *restoreStoreFake) NodesWithCapacity(context.Context, db.Resources, time.Duration) ([]db.Node, error) {
	return nil, errors.New("restore must not list fresh nodes")
}

func (s *restoreStoreFake) CreateSandbox(_ context.Context, p db.CreateSandboxParams) (db.CreateSandboxResult, error) {
	s.created = append(s.created, p)
	sandbox := db.Sandbox{ID: p.ID, OwnerID: p.Owner, MachineType: p.MachineType}
	if p.NodeID != "" {
		node := p.NodeID
		sandbox.NodeID = &node
	}
	if p.Restore != nil {
		sandbox.FromSnapshotID = p.Restore.ArtifactID
	}
	return db.CreateSandboxResult{Sandbox: sandbox}, nil
}

func (s *restoreStoreFake) ReplayCreateSandbox(context.Context, uuid.UUID, string, []byte) (db.CreateSandboxResult, bool, error) {
	s.calls++
	return s.replay(s.calls)
}

// Sandbox and AcceptSandboxResume satisfy the resume-admission surface; the
// restore-admission tests never resume.
func (s *restoreStoreFake) Sandbox(context.Context, uuid.UUID, string) (db.Sandbox, error) {
	return db.Sandbox{}, db.ErrNotFound
}

func (s *restoreStoreFake) AcceptSandboxResume(context.Context, db.ResumeParams) (db.ResumeAcceptance, error) {
	return db.ResumeAcceptance{}, db.ErrInvalidState
}

type restoreResolverFake struct {
	source    db.SnapshotSource
	machine   string
	resources db.Resources
	err       error
	delay     time.Duration
	calls     int
}

func (r *restoreResolverFake) ResolveRestore(ctx context.Context, _ uuid.UUID, _ string, _ bool, _ string) (db.SnapshotSource, string, db.Resources, error) {
	r.calls++
	if r.delay > 0 {
		timer := time.NewTimer(r.delay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			return db.SnapshotSource{}, "", db.Resources{}, ctx.Err()
		}
	}
	return r.source, r.machine, r.resources, r.err
}

func restoreDialError(string) (scheduler.NodeClient, io.Closer, error) {
	return nil, nil, errors.New("node unavailable")
}

func newRestoreScheduler(t *testing.T, store scheduler.Store, resolver scheduler.RestoreResolver, budget time.Duration) *scheduler.Scheduler {
	t.Helper()
	s, err := scheduler.New(scheduler.Options{
		Store: store, Dial: restoreDialError, Log: schedQuiet(),
		Restore: resolver, FailFastBudget: budget,
	})
	if err != nil {
		t.Fatalf("scheduler.New: %v", err)
	}
	return s
}

func restoreAdmitRequest(owner uuid.UUID) scheduler.AdmitRequest {
	return scheduler.AdmitRequest{
		ID: "bx_new", Owner: owner, MachineType: "large", Name: "restored",
		Reserve: db.Resources{VCPUs: 8, MemoryMiB: 16 << 10, DiskGiB: 111},
		From:    "base", IdempotencyKey: "key",
	}
}

func TestRestoreResolveMissThenReplayWins(t *testing.T) {
	owner := uuid.NewV7()
	winner := db.CreateSandboxResult{Sandbox: db.Sandbox{ID: "bx_winner", OwnerID: owner}, Replayed: true}
	store := &restoreStoreFake{replay: func(call int) (db.CreateSandboxResult, bool, error) {
		if call == 1 {
			return db.CreateSandboxResult{}, false, nil
		}
		return winner, true, nil
	}}
	resolver := &restoreResolverFake{err: db.ErrSnapshotNotFound}
	admit := newRestoreScheduler(t, store, resolver, time.Second)

	result, err := admit.Admit(t.Context(), restoreAdmitRequest(owner))
	if err != nil {
		t.Fatalf("resolve miss then keyed winner: %v", err)
	}
	if result.Sandbox.ID != "bx_winner" || !result.Replayed {
		t.Fatalf("result id %q replayed=%v", result.Sandbox.ID, result.Replayed)
	}
	if store.calls != 2 || len(store.created) != 0 {
		t.Fatalf("replay calls = %d created = %d", store.calls, len(store.created))
	}
}

func TestRestoreFinalReplayErrorsPropagate(t *testing.T) {
	owner := uuid.NewV7()
	source := db.SnapshotSource{
		RowID: uuid.NewV7(), OwnerID: owner, Name: "base",
		ArtifactID: "snap-" + strings.Repeat("a", 32), NodeID: "node-a",
		MachineType: "large", DiskGiB: 111, Generation: 1,
	}
	tests := []struct {
		name string
		err  error
	}{
		{name: "idempotency reused", err: db.ErrIdempotencyReused},
		{name: "idempotency in progress", err: db.ErrIdempotencyInProgress},
		{name: "db fault", err: errors.New("db fault")},
		{name: "caller canceled", err: context.Canceled},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := &restoreStoreFake{replay: func(call int) (db.CreateSandboxResult, bool, error) {
				if call == 1 {
					return db.CreateSandboxResult{}, false, nil
				}
				return db.CreateSandboxResult{}, false, tc.err
			}}
			resolver := &restoreResolverFake{
				source: source, machine: "large", resources: db.Resources{VCPUs: 8, MemoryMiB: 16 << 10, DiskGiB: 111},
			}
			admit := newRestoreScheduler(t, store, resolver, time.Second)
			_, err := admit.Admit(t.Context(), restoreAdmitRequest(owner))
			if !errors.Is(err, tc.err) {
				t.Fatalf("err = %v, want %v", err, tc.err)
			}
			if len(store.created) != 0 {
				t.Fatalf("created %d sandboxes after a failed final replay", len(store.created))
			}
		})
	}
}

func TestRestoreProbeFailureFinalReplayWins(t *testing.T) {
	owner := uuid.NewV7()
	winner := db.CreateSandboxResult{Sandbox: db.Sandbox{ID: "bx_winner", OwnerID: owner}, Replayed: true}
	store := &restoreStoreFake{replay: func(call int) (db.CreateSandboxResult, bool, error) {
		if call == 1 {
			return db.CreateSandboxResult{}, false, nil
		}
		return winner, true, nil
	}}
	source := db.SnapshotSource{
		RowID: uuid.NewV7(), OwnerID: owner, Name: "base",
		ArtifactID: "snap-" + strings.Repeat("a", 32), NodeID: "node-a",
		MachineType: "large", DiskGiB: 111, Generation: 1,
	}
	resolver := &restoreResolverFake{
		source: source, machine: "large", resources: db.Resources{VCPUs: 8, MemoryMiB: 16 << 10, DiskGiB: 111},
	}
	admit := newRestoreScheduler(t, store, resolver, time.Second)
	result, err := admit.Admit(t.Context(), restoreAdmitRequest(owner))
	if err != nil {
		t.Fatalf("probe failure with keyed winner: %v", err)
	}
	if result.Sandbox.ID != "bx_winner" || !result.Replayed {
		t.Fatalf("result id %q replayed=%v", result.Sandbox.ID, result.Replayed)
	}
}

func TestRestoreFailFastBudgetCoversResolution(t *testing.T) {
	owner := uuid.NewV7()
	// The first replay misses, then the resolver consumes the whole budget.
	// The bounded final replay still runs and returns the budget error.
	store := &restoreStoreFake{replay: func(call int) (db.CreateSandboxResult, bool, error) {
		return db.CreateSandboxResult{}, false, nil
	}}
	resolver := &restoreResolverFake{delay: 2 * time.Second}
	admit := newRestoreScheduler(t, store, resolver, 50*time.Millisecond)
	req := restoreAdmitRequest(owner)
	req.FailFast = true

	start := time.Now()
	_, err := admit.Admit(t.Context(), req)
	elapsed := time.Since(start)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("budgeted resolution error = %v, want DeadlineExceeded", err)
	}
	if elapsed >= time.Second {
		t.Fatalf("budget elapsed = %v, want well below the 2s resolver delay", elapsed)
	}
	if resolver.calls != 1 || store.calls != 2 {
		t.Fatalf("resolver calls = %d replay calls = %d", resolver.calls, store.calls)
	}
	if len(store.created) != 0 {
		t.Fatalf("created %d sandboxes after budget expiry", len(store.created))
	}
}
