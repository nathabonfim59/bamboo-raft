package lifecycle_test

// Fork integration tests: PostgreSQL, nodefake through nodelink and one shared
// memstore. They skip unless BBRAFT_TEST_DATABASE_URL is set (dbtest.Open).

import (
	"context"
	"encoding/json/v2"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/ids"
	"github.com/nathabonfim59/bamboo-raft/internal/lifecycle"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// forkAdmit admits one fork of source onto nodeID the way the API admission
// does: the source's shape and the default one-hour TTL.
func forkAdmit(t *testing.T, e *lifecycleEnv, own uuid.UUID, source db.Sandbox, nodeID string) db.Sandbox {
	t.Helper()
	ttl := 3600
	result, err := e.store.AdmitFork(t.Context(), db.ForkAdmitParams{
		ID: ids.Sandbox(), Owner: own, SourceID: source.ID,
		MachineType: source.MachineType, Name: source.Name + " fork",
		InheritEnv: true, TTLSeconds: &ttl, NodeID: nodeID,
		Reserve: source.Reserved,
	})
	if err != nil {
		t.Fatalf("admit fork: %v", err)
	}
	return result.Sandbox
}

// forkNodeReserved reads one node's stored reservation.
func forkNodeReserved(t *testing.T, e *lifecycleEnv, nodeID string) db.Resources {
	t.Helper()
	n, err := e.store.Node(t.Context(), nodeID)
	if err != nil {
		t.Fatalf("node %s: %v", nodeID, err)
	}
	return n.Reserved
}

// forkProvisionJob reads the fork's hand-off and its payload.
func forkProvisionJob(t *testing.T, e *lifecycleEnv, own uuid.UUID, forkID string) (db.Job, db.ProvisionPayload) {
	t.Helper()
	job, err := e.store.LatestSandboxJob(t.Context(), own, forkID, db.JobProvision)
	if err != nil {
		t.Fatalf("fork provision job: %v", err)
	}
	var payload db.ProvisionPayload
	if err := json.Unmarshal([]byte(job.Payload), &payload); err != nil {
		t.Fatalf("provision payload: %v", err)
	}
	return job, payload
}

// writeHomeFile writes one file into a fake VM's home, as a sandbox would.
func writeHomeFile(t *testing.T, n *genNode, sandboxID, name, content string) {
	t.Helper()
	path := n.homeFile(sandboxID, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

// TestForkReadySourceCutsThenBootsOnAnotherNode forks a ready source on node A
// onto node B: the source is snapshotted once under its guard, the fork's
// provision job restores that manifest on B, the file is there and the source
// keeps running untouched.
func TestForkReadySourceCutsThenBootsOnAnotherNode(t *testing.T) {
	e, a, mem := genSetup(t, true)
	own := e.owner(t)
	w := genWorker(t, e, e.store, mem, lifecycle.Options{})
	source := genCreate(t, e, w, own, nil)
	writeHomeFile(t, a, source.ID, "fork-me.txt", "kept by the fork")
	genRound(t, w) // one earlier generation, so the fork is not the first

	e.registerNode(t, "node-b", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 20})
	b := genStartNode(t, e, "node-b", mem)

	fork := forkAdmit(t, e, own, source, "node-b")
	cutsBefore := a.snapshotCalls()
	if !mustProcess(t, w) {
		t.Fatal("fork job not claimed")
	}
	if got := a.snapshotCalls() - cutsBefore; got != 1 {
		t.Fatalf("SourceVM calls during the fork = %d, want 1", got)
	}
	if got := e.sandbox(t, own, source.ID); got.State != db.StateReady {
		t.Fatalf("source state after the fork = %s, want ready", got.State)
	}
	gen, ok, err := e.store.LatestGeneration(t.Context(), own, source.ID)
	if err != nil || !ok || gen.Number != 1 || gen.Status != db.GenerationCompleted {
		t.Fatalf("source latest generation = %+v/%t (%v), want completed 1", gen, ok, err)
	}

	job, payload := forkProvisionJob(t, e, own, fork.ID)
	if payload.FromManifest != gen.ManifestKey || payload.Image != gen.Image ||
		payload.NodeID != "node-b" {
		t.Fatalf("provision payload = %+v, want manifest %q on node-b", payload, gen.ManifestKey)
	}
	if job.SourceGenerationID == nil || *job.SourceGenerationID != gen.ID {
		t.Fatalf("provision source generation = %v, want %s", job.SourceGenerationID, gen.ID)
	}

	// The boot itself: the fork becomes ready on B with the file.
	if !mustProcess(t, w) {
		t.Fatal("fork provision job not claimed")
	}
	built := e.sandbox(t, own, fork.ID)
	if built.State != db.StateReady {
		t.Fatalf("fork state = %s, want ready", built.State)
	}
	if built.ChainID == source.ChainID {
		t.Fatal("fork continued the source's chain; a fork starts a new one")
	}
	got, err := os.ReadFile(b.homeFile(fork.ID, "fork-me.txt"))
	if err != nil || string(got) != "kept by the fork" {
		t.Fatalf("fork file = %q (%v), want the source's file", got, err)
	}
	// The source VM is untouched: its own copy still exists and it never
	// moved.
	if _, err := os.Stat(a.homeFile(source.ID, "fork-me.txt")); err != nil {
		t.Fatalf("source file after the fork: %v", err)
	}
	if sourceNow := e.sandbox(t, own, source.ID); sourceNow.NodeID == nil || *sourceNow.NodeID != "node-a" {
		t.Fatalf("source moved to %v", sourceNow.NodeID)
	}
}

// TestForkStoppedSourceUsesItsGenerationWithoutSnapshot forks an archived
// source: no SnapshotVM call, the pinned latest generation is restored.
func TestForkStoppedSourceUsesItsGenerationWithoutSnapshot(t *testing.T) {
	e, a, mem := genSetup(t, true)
	own := e.owner(t)
	w := genWorker(t, e, e.store, mem, lifecycle.Options{})
	source := genCreate(t, e, w, own, nil)
	writeHomeFile(t, a, source.ID, "stopped.txt", "kept while stopped")
	e.acceptStop(t, own, source.ID, false)
	if !mustProcess(t, w) {
		t.Fatal("stop job not claimed")
	}
	stopped := e.sandbox(t, own, source.ID)
	if stopped.State != db.StateArchived {
		t.Fatalf("source state = %s, want archived", stopped.State)
	}
	latest, ok, err := e.store.LatestGeneration(t.Context(), own, source.ID)
	if err != nil || !ok {
		t.Fatalf("stopped source generation = %+v/%t (%v)", latest, ok, err)
	}

	ttl := 3600
	result, err := e.store.AdmitFork(t.Context(), db.ForkAdmitParams{
		ID: ids.Sandbox(), Owner: own, SourceID: source.ID,
		MachineType: stopped.MachineType, Name: "stopped fork",
		InheritEnv: true, TTLSeconds: &ttl, NodeID: "node-a",
		Reserve:            stopped.Reserved,
		SourceGenerationID: &latest.ID,
	})
	if err != nil {
		t.Fatalf("admit fork: %v", err)
	}
	cutsBefore := a.snapshotCalls()
	if !mustProcess(t, w) {
		t.Fatal("fork job not claimed")
	}
	if got := a.snapshotCalls() - cutsBefore; got != 0 {
		t.Fatalf("SnapshotVM calls for a stopped source = %d, want 0", got)
	}
	_, payload := forkProvisionJob(t, e, own, result.Sandbox.ID)
	if payload.FromManifest != latest.ManifestKey {
		t.Fatalf("provision manifest = %q, want the stopped source's %q", payload.FromManifest, latest.ManifestKey)
	}
	if !mustProcess(t, w) {
		t.Fatal("fork provision job not claimed")
	}
	if got := e.sandbox(t, own, result.Sandbox.ID); got.State != db.StateReady {
		t.Fatalf("fork state = %s, want ready", got.State)
	}
	restored, err := os.ReadFile(a.homeFile(result.Sandbox.ID, "stopped.txt"))
	if err != nil || string(restored) != "kept while stopped" {
		t.Fatalf("fork file = %q (%v)", restored, err)
	}
}

// TestForkRefusedCutEndsTheForkInError makes SnapshotVM refuse: the fork ends
// in error with no provision job and its reservation is released.
func TestForkRefusedCutEndsTheForkInError(t *testing.T) {
	e, a, mem := genSetup(t, true)
	own := e.owner(t)
	w := genWorker(t, e, e.store, mem, lifecycle.Options{})
	source := genCreate(t, e, w, own, nil)
	fork := forkAdmit(t, e, own, source, "node-a")
	reservedBefore := forkNodeReserved(t, e, "node-a")

	a.setSnapHook(func(ctx context.Context, req *nodev1.SnapshotVMRequest) (*nodev1.Generation, error) {
		return nil, status.Error(codes.FailedPrecondition, "no object store")
	})
	if !mustProcess(t, w) {
		t.Fatal("fork job not claimed")
	}
	got := e.sandbox(t, own, fork.ID)
	if got.State != db.StateError || got.ErrorReason != "fork_failed" {
		t.Fatalf("fork after a refused cut = %s/%q, want error/fork_failed", got.State, got.ErrorReason)
	}
	if _, err := e.store.LatestSandboxJob(t.Context(), own, fork.ID, db.JobProvision); err == nil {
		t.Fatal("a refused fork still enqueued a provision job")
	}
	if after := forkNodeReserved(t, e, "node-a"); after.VCPUs != reservedBefore.VCPUs-1 ||
		after.DiskGiB != reservedBefore.DiskGiB-1 {
		t.Fatalf("reservation after the failed fork = %+v, want compute and disk released", after)
	}
	if src := e.sandbox(t, own, source.ID); src.State != db.StateReady {
		t.Fatalf("source state = %s, want ready", src.State)
	}
}

// TestReconcileFailedForkReleasesTheReservation covers the reaper's backstop:
// a fork job that failed terminally without recording its own outcome ends the
// fork in error and gives the capacity back.
func TestReconcileFailedForkReleasesTheReservation(t *testing.T) {
	e, _, mem := genSetup(t, true)
	own := e.owner(t)
	w := genWorker(t, e, e.store, mem, lifecycle.Options{})
	source := genCreate(t, e, w, own, nil)
	fork := forkAdmit(t, e, own, source, "node-a")

	job, err := e.store.ClaimJob(t.Context(), "fork-worker", time.Minute)
	if err != nil || job.Kind != db.JobFork {
		t.Fatalf("claim fork job = %+v (%v)", job, err)
	}
	if err := e.store.FailJob(t.Context(), job.ID, "fork-worker", job.LeaseToken, "worker crashed"); err != nil {
		t.Fatalf("fail fork job: %v", err)
	}
	w.ReapOnce(t.Context())
	got := e.sandbox(t, own, fork.ID)
	if got.State != db.StateError || got.ErrorReason != "fork_failed" {
		t.Fatalf("fork after reconciliation = %s/%q, want error/fork_failed", got.State, got.ErrorReason)
	}
	if reserved := forkNodeReserved(t, e, "node-a"); reserved.VCPUs != 1 || reserved.MemoryMiB != source.Reserved.MemoryMiB {
		t.Fatalf("node reservation = %+v, want only the source's", reserved)
	}
}
