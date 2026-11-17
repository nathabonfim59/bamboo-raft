package scheduler_test

// Same-node resume admission tests (B1.2): the scheduler probes the node that
// holds the retained disk through the real gateway, pins the resume there,
// and lets the store do the atomic admission. Real PostgreSQL, real gateway,
// nodefake nodes. Gated by BBRAFT_TEST_DATABASE_URL.

import (
	"context"
	"errors"
	"testing"
	"time"

	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/ids"
	"github.com/nathabonfim59/bamboo-raft/internal/scheduler"
)

// stoppedSandbox drives one sandbox through the durable stop path (create,
// ready, accepted stop, finalized archival) and returns its row with the
// compute part released and the disk retained on nodeID.
func stoppedSandbox(t *testing.T, e *schedEnv, own uuid.UUID, nodeID string) db.Sandbox {
	t.Helper()
	res, err := e.store.CreateSandbox(t.Context(), db.CreateSandboxParams{
		ID: ids.Sandbox(), Owner: own, MachineType: "small", Name: "resume-test",
		NodeID: nodeID, Reserve: db.Resources{VCPUs: 1, MemoryMiB: 512, DiskGiB: 1},
		MaxAttempts: 2,
	})
	if err != nil {
		t.Fatalf("create sandbox: %v", err)
	}
	ready, err := e.store.TransitionSandbox(t.Context(), db.TransitionParams{
		SandboxID: res.Sandbox.ID, Owner: own,
		FromStates: []db.SandboxState{db.StateProvisioning}, ExpectedVersion: res.Sandbox.Version,
		To: db.StateReady,
	})
	if err != nil {
		t.Fatalf("ready transition: %v", err)
	}
	op, _, _, created, err := e.store.AcceptSandboxStop(t.Context(), own, ready.ID, false)
	if err != nil || !created {
		t.Fatalf("accept stop: created=%v err=%v", created, err)
	}
	// Claim past the provision job to the stop job.
	var stopJob db.Job
	for {
		job, err := e.store.ClaimJob(t.Context(), "stop-fixture", time.Minute)
		if err != nil {
			t.Fatalf("claim: %v", err)
		}
		if job.Kind == db.JobStop {
			stopJob = job
			break
		}
		if err := e.store.CompleteJob(t.Context(), job.ID, "stop-fixture", job.LeaseToken); err != nil {
			t.Fatalf("complete %s: %v", job.Kind, err)
		}
	}
	stopped, _, err := e.store.FinalizeSandboxStop(t.Context(), stopJob.ID, "stop-fixture", stopJob.LeaseToken,
		db.FinalizeStopParams{Owner: own, SandboxID: ready.ID, StopID: op.ID})
	if err != nil {
		t.Fatalf("finalize stop: %v", err)
	}
	if err := e.store.CompleteJob(t.Context(), stopJob.ID, "stop-fixture", stopJob.LeaseToken); err != nil {
		t.Fatal(err)
	}
	if stopped.State != db.StateArchived {
		t.Fatalf("fixture state: %s", stopped.State)
	}
	return stopped
}

func TestResumeAdmittedOnPinnedNode(t *testing.T) {
	e := newSchedEnv(t)
	e.registerNode(t, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 40})
	e.registerNode(t, "node-b", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 40})
	e.startNode(t, "node-a")
	e.startNode(t, "node-b")
	own := e.owner(t)
	stopped := stoppedSandbox(t, e, own, "node-a")
	desired := stopped.Desired
	if desired.DiskGiB == 0 {
		t.Fatal("fixture lost the retained disk reservation")
	}

	s := e.scheduler(scheduler.Options{})
	out, err := s.Resume(t.Context(), scheduler.ResumeRequest{Owner: own, SandboxID: stopped.ID})
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if out.Sandbox.State != db.StateProvisioning || out.Sandbox.LifecycleGeneration != 1 {
		t.Fatalf("resumed row: %+v", out.Sandbox)
	}
	if out.Sandbox.NodeID == nil || *out.Sandbox.NodeID != "node-a" {
		t.Fatalf("resume moved the sandbox to %v", out.Sandbox.NodeID)
	}
	if out.Job.Kind != db.JobResume {
		t.Fatalf("resume job kind: %s", out.Job.Kind)
	}
	// The pinned node re-reserved exactly the desired shape; the other node
	// reserved nothing.
	if node, err := e.store.Node(t.Context(), "node-a"); err != nil || node.Reserved != desired {
		t.Fatalf("node-a reservation: %+v (%v)", node.Reserved, err)
	}
	if node, err := e.store.Node(t.Context(), "node-b"); err != nil || node.Reserved != (db.Resources{}) {
		t.Fatalf("node-b reservation: %+v (%v)", node.Reserved, err)
	}
}

func TestResumeRefusedWhenPinnedNodeUnreachable(t *testing.T) {
	e := newSchedEnv(t)
	e.registerNode(t, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 40})
	e.registerNode(t, "node-b", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 40})
	e.startNode(t, "node-b") // healthy alternative, but the disk is on node-a
	own := e.owner(t)
	stopped := stoppedSandbox(t, e, own, "node-a")

	s := e.scheduler(scheduler.Options{})
	_, err := s.Resume(t.Context(), scheduler.ResumeRequest{Owner: own, SandboxID: stopped.ID, FailFast: true})
	if !errors.Is(err, scheduler.ErrNoNode) {
		t.Fatalf("resume with unreachable pinned node: %v", err)
	}
	// Pinned: nothing was reserved anywhere and no fallback boot happened.
	row, rerr := e.store.Sandbox(t.Context(), own, stopped.ID)
	if rerr != nil || row.State != db.StateArchived || row.LifecycleGeneration != 0 {
		t.Fatalf("row after refusal: %+v (%v)", row, rerr)
	}
	for _, id := range []string{"node-a", "node-b"} {
		if node, err := e.store.Node(t.Context(), id); err != nil || node.Reserved.VCPUs != 0 || node.Reserved.MemoryMiB != 0 {
			t.Fatalf("node %s reserved compute after refusal: %+v (%v)", id, node.Reserved, err)
		}
	}
}

func TestResumePreservesCallerCancellation(t *testing.T) {
	e := newSchedEnv(t)
	e.registerNode(t, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 40})
	e.startNode(t, "node-a")
	own := e.owner(t)
	stopped := stoppedSandbox(t, e, own, "node-a")

	s := e.scheduler(scheduler.Options{})
	callerCtx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := s.Resume(callerCtx, scheduler.ResumeRequest{Owner: own, SandboxID: stopped.ID, FailFast: true})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("caller cancellation not preserved: %v", err)
	}
	if row, rerr := e.store.Sandbox(t.Context(), own, stopped.ID); rerr != nil || row.State != db.StateArchived {
		t.Fatalf("row after cancelled resume: %+v (%v)", row, rerr)
	}
}

func TestResumeCapacityRefusalPassesThrough(t *testing.T) {
	e := newSchedEnv(t)
	// Room for exactly one sandbox of the fixture shape.
	e.registerNode(t, "node-a", db.Resources{VCPUs: 2, MemoryMiB: 2048, DiskGiB: 40})
	e.startNode(t, "node-a")
	own := e.owner(t)
	stopped := stoppedSandbox(t, e, own, "node-a")

	// Consume the freed compute with a second live sandbox on the same node.
	live, err := e.store.CreateSandbox(t.Context(), db.CreateSandboxParams{
		ID: ids.Sandbox(), Owner: own, MachineType: "small", Name: "live",
		NodeID: "node-a", Reserve: db.Resources{VCPUs: 2, MemoryMiB: 2048, DiskGiB: 1},
		MaxAttempts: 2,
	})
	if err != nil {
		t.Fatalf("create live sandbox: %v", err)
	}
	_ = live

	s := e.scheduler(scheduler.Options{})
	_, err = s.Resume(t.Context(), scheduler.ResumeRequest{Owner: own, SandboxID: stopped.ID})
	if !errors.Is(err, db.ErrNoCapacity) {
		t.Fatalf("capacity refusal not passed through: %v", err)
	}
	if row, rerr := e.store.Sandbox(t.Context(), own, stopped.ID); rerr != nil || row.State != db.StateArchived {
		t.Fatalf("row after capacity refusal: %+v (%v)", row, rerr)
	}
}

func TestResumeUnknownAndForeignSandboxes(t *testing.T) {
	e := newSchedEnv(t)
	e.registerNode(t, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 40})
	e.startNode(t, "node-a")
	own := e.owner(t)
	foreign := e.owner(t)
	stopped := stoppedSandbox(t, e, own, "node-a")

	s := e.scheduler(scheduler.Options{})
	if _, err := s.Resume(t.Context(), scheduler.ResumeRequest{Owner: own, SandboxID: "bx_missing00"}); !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("unknown sandbox: %v", err)
	}
	if _, err := s.Resume(t.Context(), scheduler.ResumeRequest{Owner: foreign, SandboxID: stopped.ID}); !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("foreign sandbox: %v", err)
	}
}
