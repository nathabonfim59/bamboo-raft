package db_test

// Resume on another node: the admission transaction, its fences, the
// cross-node finalizations and the vm_evictions store. PostgreSQL only: the
// tests skip unless BBRAFT_TEST_DATABASE_URL is set (dbtest.Open).

import (
	"encoding/json/v2"
	"errors"
	"testing"
	"time"

	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
)

// rsmArchived is an archived sandbox on node-a with one completed generation
// of diskGiB, and node-b and node-c registered.
type rsmArchived struct {
	s   *db.Store
	own uuid.UUID
	sbx db.Sandbox
	gen db.Generation
}

func rsmSetup(t *testing.T, diskGiB uint64) rsmArchived {
	t.Helper()
	s := dbtestOpen(t)
	own := owner(t, s)
	for _, id := range []string{"node-a", "node-b", "node-c"} {
		node(t, s, id, db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})
	}
	sbx := phase1CreateReady(t, s, own, db.CreateSandboxParams{})
	g := genBegin(t, s, own, sbx.ID)
	res := genResult(time.Now())
	res.DiskBytes = diskGiB << 30
	gen, err := s.CompleteGeneration(t.Context(), g.ID, res, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("complete generation: %v", err)
	}
	stopped, _ := phase1Stopped(t, s, own, sbx)
	return rsmArchived{s: s, own: own, sbx: stopped, gen: gen}
}

func (r rsmArchived) params(to string) db.ResumeElsewhereParams {
	return db.ResumeElsewhereParams{
		ResumeParams: db.ResumeParams{Owner: r.own, SandboxID: r.sbx.ID},
		ToNode:       to,
		GenerationID: r.gen.ID,
		ManifestKey:  r.gen.ManifestKey,
	}
}

func rsmOpenEvictions(t *testing.T, r rsmArchived) []string {
	t.Helper()
	nodes, err := r.s.OpenEvictionNodes(t.Context(), r.own, r.sbx.ID)
	if err != nil {
		t.Fatal(err)
	}
	return nodes
}

// TestResumeElsewhereAdmission accepts a resume on node-b: node-a's retained
// disk reservation is released, node-b holds the full shape with the disk
// grown to the generation's, the row is provisioning on node-b with no local
// disk and a new lifecycle generation, node-a's copy is an open eviction and
// the job carries the manifest, the evicted node and the source generation.
func TestResumeElsewhereAdmission(t *testing.T) {
	r := rsmSetup(t, 15)
	if got := nodeReserved(t, r.s, "node-a"); got.DiskGiB != 10 {
		t.Fatalf("node-a before: %+v", got)
	}
	acc, err := r.s.AcceptSandboxResumeElsewhere(t.Context(), r.params("node-b"))
	if err != nil {
		t.Fatalf("accept: %v", err)
	}
	got := acc.Sandbox
	switch {
	case got.State != db.StateProvisioning:
		t.Fatalf("state = %s", got.State)
	case got.NodeID == nil || *got.NodeID != "node-b" || got.DiskNodeID != nil:
		t.Fatalf("node=%v disk=%v", got.NodeID, got.DiskNodeID)
	case got.LifecycleGeneration != r.sbx.LifecycleGeneration+1:
		t.Fatalf("generation = %d", got.LifecycleGeneration)
	case got.Desired != (db.Resources{VCPUs: 1, MemoryMiB: 1024, DiskGiB: 15}) || got.Reserved != got.Desired:
		t.Fatalf("desired=%+v reserved=%+v", got.Desired, got.Reserved)
	case got.ChainID != r.sbx.ChainID:
		t.Fatal("chain changed")
	}
	if a := nodeReserved(t, r.s, "node-a"); a != (db.Resources{}) {
		t.Fatalf("node-a after: %+v", a)
	}
	if b := nodeReserved(t, r.s, "node-b"); b != got.Desired {
		t.Fatalf("node-b after: %+v", b)
	}
	if ev := rsmOpenEvictions(t, r); len(ev) != 1 || ev[0] != "node-a" {
		t.Fatalf("open evictions = %v", ev)
	}
	var p db.ResumePayload
	if err := json.Unmarshal([]byte(acc.Job.Payload), &p); err != nil {
		t.Fatal(err)
	}
	if p.NodeID != "node-b" || p.FromManifest != r.gen.ManifestKey || p.Image != r.gen.Image ||
		p.EvictNode != "node-a" || p.Generation != got.LifecycleGeneration || p.Resources != got.Desired {
		t.Fatalf("payload = %+v", p)
	}
	if acc.Job.Kind != db.JobResume || acc.Job.SourceGenerationID == nil || *acc.Job.SourceGenerationID != r.gen.ID {
		t.Fatalf("job kind=%s source=%v", acc.Job.Kind, acc.Job.SourceGenerationID)
	}
}

// TestResumeElsewhereAdmissionRefusals: every refusal leaves the row, the
// reservations and the evictions untouched.
func TestResumeElsewhereAdmissionRefusals(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(t *testing.T, r *rsmArchived, p *db.ResumeElsewhereParams)
		want   error
	}{
		{"disk node", func(t *testing.T, r *rsmArchived, p *db.ResumeElsewhereParams) { p.ToNode = "node-a" }, db.ErrConflict},
		{"open eviction on target", func(t *testing.T, r *rsmArchived, p *db.ResumeElsewhereParams) {
			rsmExec(t, r.s, `INSERT INTO vm_evictions (id, node_id, sandbox_id, reason) VALUES ($1, 'node-b', $2, 'test')`,
				uuid.NewV7(), r.sbx.ID)
		}, db.ErrConflict},
		{"wrong manifest", func(t *testing.T, r *rsmArchived, p *db.ResumeElsewhereParams) {
			p.ManifestKey = "manifests/x/0.json.zst"
		}, db.ErrSnapshotNotFound},
		{"other sandbox's generation", func(t *testing.T, r *rsmArchived, p *db.ResumeElsewhereParams) {
			other := phase1CreateReady(t, r.s, r.own, db.CreateSandboxParams{})
			g := genComplete(t, r.s, r.own, other.ID, time.Now())
			p.GenerationID, p.ManifestKey = g.ID, g.ManifestKey
		}, db.ErrSnapshotNotFound},
		{"purged generation", func(t *testing.T, r *rsmArchived, p *db.ResumeElsewhereParams) {
			rsmExec(t, r.s, `UPDATE snapshots SET purged_at = now() WHERE id = $1`, r.gen.ID)
		}, db.ErrSnapshotNotFound},
		{"data expired", func(t *testing.T, r *rsmArchived, p *db.ResumeElsewhereParams) {
			rsmExec(t, r.s, `UPDATE sandboxes SET data_expired_at = now() WHERE id = $1`, r.sbx.ID)
		}, db.ErrSnapshotDataExpired},
		{"snapshots off", func(t *testing.T, r *rsmArchived, p *db.ResumeElsewhereParams) {
			rsmExec(t, r.s, `UPDATE sandboxes SET snapshots_enabled = false WHERE id = $1`, r.sbx.ID)
		}, db.ErrSnapshotsDisabled},
		{"target full", func(t *testing.T, r *rsmArchived, p *db.ResumeElsewhereParams) {
			rsmExec(t, r.s, `UPDATE nodes SET reserved_vcpus = total_vcpus WHERE id = 'node-b'`)
		}, db.ErrNoCapacity},
		{"unknown target", func(t *testing.T, r *rsmArchived, p *db.ResumeElsewhereParams) { p.ToNode = "node-z" }, db.ErrNodeUnavailable},
		{"foreign owner", func(t *testing.T, r *rsmArchived, p *db.ResumeElsewhereParams) { p.Owner = uuid.NewV7() }, db.ErrNotFound},
		{"shrink", func(t *testing.T, r *rsmArchived, p *db.ResumeElsewhereParams) {
			p.MachineType, p.Resources = "small", db.Resources{VCPUs: 1, MemoryMiB: 512, DiskGiB: 1}
		}, db.ErrTypeTooSmall},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := rsmSetup(t, 10)
			p := r.params("node-b")
			tc.mutate(t, &r, &p)
			before := nodeReserved(t, r.s, "node-a")
			evBefore := rsmOpenEvictions(t, r)
			_, err := r.s.AcceptSandboxResumeElsewhere(t.Context(), p)
			if !errors.Is(err, tc.want) {
				t.Fatalf("accept = %v, want %v", err, tc.want)
			}
			cur, err := r.s.SandboxAny(t.Context(), r.own, r.sbx.ID)
			if err != nil {
				t.Fatal(err)
			}
			if cur.State != db.StateArchived || cur.Version != r.sbx.Version {
				t.Fatalf("row changed: %s v%d", cur.State, cur.Version)
			}
			if got := nodeReserved(t, r.s, "node-a"); got != before {
				t.Fatalf("node-a reserved %+v, want %+v", got, before)
			}
			if got := rsmOpenEvictions(t, r); len(got) != len(evBefore) {
				t.Fatalf("evictions %v, want %v", got, evBefore)
			}
		})
	}
}

// TestResumeElsewhereRefusesArchivingSandbox: a stop still in flight keeps
// the disk node's full reservation; only the same-node path may take it over.
func TestResumeElsewhereRefusesArchivingSandbox(t *testing.T) {
	r := rsmSetup(t, 10)
	acc, err := r.s.AcceptSandboxResume(t.Context(), db.ResumeParams{Owner: r.own, SandboxID: r.sbx.ID})
	if err != nil {
		t.Fatal(err)
	}
	ready := phase1Ready(t, r.s, r.own, acc.Sandbox)
	if _, _, _, _, err := r.s.AcceptSandboxStop(t.Context(), r.own, ready.ID, false); err != nil {
		t.Fatal(err)
	}
	if _, err := r.s.AcceptSandboxResumeElsewhere(t.Context(), r.params("node-b")); !errors.Is(err, db.ErrInvalidState) {
		t.Fatalf("accept while archiving = %v, want ErrInvalidState", err)
	}
}

// rsmAdmitted is a cross-node resume admitted to node-b with its job claimed.
func rsmAdmitted(t *testing.T) (rsmArchived, db.Sandbox, db.Job) {
	t.Helper()
	r := rsmSetup(t, 10)
	acc, err := r.s.AcceptSandboxResumeElsewhere(t.Context(), r.params("node-b"))
	if err != nil {
		t.Fatal(err)
	}
	job := phase1ClaimKind(t, r.s, "rsm-worker", db.JobResume)
	return r, acc.Sandbox, job
}

// TestResumeElsewhereFinalizeReady records the disk on node-b, hydrated_at
// and the sandbox.hydrated event.
func TestResumeElsewhereFinalizeReady(t *testing.T) {
	r, admitted, job := rsmAdmitted(t)
	got, err := r.s.FinalizeSandboxResume(t.Context(), job.ID, "rsm-worker", job.LeaseToken, db.FinalizeResumeParams{
		Owner: r.own, SandboxID: admitted.ID, Generation: admitted.LifecycleGeneration, Version: admitted.Version,
	})
	if err != nil {
		t.Fatalf("finalize: %v", err)
	}
	if got.State != db.StateReady || got.DiskNodeID == nil || *got.DiskNodeID != "node-b" || got.HydratedAt == nil {
		t.Fatalf("state=%s disk=%v hydrated=%v", got.State, got.DiskNodeID, got.HydratedAt)
	}
	if n := count(t, r.s, `SELECT count(*) FROM sandbox_events WHERE sandbox_id = $1 AND type = $2`,
		admitted.ID, db.EventSandboxHydrated); n != 1 {
		t.Fatalf("hydrated events = %d", n)
	}
}

// TestSameNodeResumeFinalizeKeepsHydration: a same-node resume neither moves
// disk_node_id nor records hydration.
func TestSameNodeResumeFinalizeKeepsHydration(t *testing.T) {
	r := rsmSetup(t, 10)
	acc, err := r.s.AcceptSandboxResume(t.Context(), db.ResumeParams{Owner: r.own, SandboxID: r.sbx.ID})
	if err != nil {
		t.Fatal(err)
	}
	job := phase1ClaimKind(t, r.s, "rsm-worker", db.JobResume)
	got, err := r.s.FinalizeSandboxResume(t.Context(), job.ID, "rsm-worker", job.LeaseToken, db.FinalizeResumeParams{
		Owner: r.own, SandboxID: r.sbx.ID, Generation: acc.Sandbox.LifecycleGeneration, Version: acc.Sandbox.Version,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.DiskNodeID == nil || *got.DiskNodeID != "node-a" || got.HydratedAt != nil {
		t.Fatalf("disk=%v hydrated=%v", got.DiskNodeID, got.HydratedAt)
	}
}

// TestResumeElsewhereFailureLeavesNoLocalDisk returns the sandbox to archived
// with nothing reserved anywhere and disk_node_id NULL, so the next resume
// places it again from the store.
func TestResumeElsewhereFailureLeavesNoLocalDisk(t *testing.T) {
	r, admitted, job := rsmAdmitted(t)
	got, err := r.s.FinalizeSandboxResumeElsewhereFailure(t.Context(), job.ID, "rsm-worker", job.LeaseToken,
		db.FinalizeResumeFailureParams{
			Owner: r.own, SandboxID: admitted.ID, Generation: admitted.LifecycleGeneration, Version: admitted.Version,
		}, "snapshot_not_found")
	if err != nil {
		t.Fatalf("finalize failure: %v", err)
	}
	if got.State != db.StateArchived || got.ErrorReason != "snapshot_not_found" || got.DiskNodeID != nil ||
		got.Reserved != (db.Resources{}) {
		t.Fatalf("row = %s %q disk=%v reserved=%+v", got.State, got.ErrorReason, got.DiskNodeID, got.Reserved)
	}
	if b := nodeReserved(t, r.s, "node-b"); b != (db.Resources{}) {
		t.Fatalf("node-b reserved: %+v", b)
	}
	if _, err := r.s.AcceptSandboxResumeElsewhere(t.Context(), r.params("node-c")); err != nil {
		t.Fatalf("second resume: %v", err)
	}
}

// TestResumeElsewhereSameNodeFinalizeFailureRefused: the cross-node failure
// finalization refuses a same-node resume claim.
func TestResumeElsewhereSameNodeFinalizeFailureRefused(t *testing.T) {
	r := rsmSetup(t, 10)
	acc, err := r.s.AcceptSandboxResume(t.Context(), db.ResumeParams{Owner: r.own, SandboxID: r.sbx.ID})
	if err != nil {
		t.Fatal(err)
	}
	job := phase1ClaimKind(t, r.s, "rsm-worker", db.JobResume)
	_, err = r.s.FinalizeSandboxResumeElsewhereFailure(t.Context(), job.ID, "rsm-worker", job.LeaseToken,
		db.FinalizeResumeFailureParams{
			Owner: r.own, SandboxID: r.sbx.ID, Generation: acc.Sandbox.LifecycleGeneration, Version: acc.Sandbox.Version,
		}, "boot_failed")
	if !errors.Is(err, db.ErrInvalidJobPayload) {
		t.Fatalf("finalize = %v, want ErrInvalidJobPayload", err)
	}
}

// TestMoveResume moves the claimed cross-node resume from node-b to node-c:
// reservations follow, a new resume job keeps the manifest, evicted node and
// source generation, and the old job is done. node-a (the evicted copy)
// is refused as a target.
func TestMoveResume(t *testing.T) {
	r, admitted, job := rsmAdmitted(t)
	params := db.MoveResumeParams{
		Owner: r.own, SandboxID: admitted.ID, Generation: admitted.LifecycleGeneration,
		Version: admitted.Version, FromNode: "node-b", ToNode: "node-a",
	}
	if _, _, err := r.s.MoveResume(t.Context(), job.ID, "rsm-worker", job.LeaseToken, params); !errors.Is(err, db.ErrConflict) {
		t.Fatalf("move to the evicted node = %v, want ErrConflict", err)
	}
	params.ToNode = "node-c"
	moved, next, err := r.s.MoveResume(t.Context(), job.ID, "rsm-worker", job.LeaseToken, params)
	if err != nil {
		t.Fatalf("move: %v", err)
	}
	if moved.NodeID == nil || *moved.NodeID != "node-c" || moved.State != db.StateProvisioning ||
		moved.LifecycleGeneration != admitted.LifecycleGeneration {
		t.Fatalf("moved row: %s on %v gen %d", moved.State, moved.NodeID, moved.LifecycleGeneration)
	}
	if b, c := nodeReserved(t, r.s, "node-b"), nodeReserved(t, r.s, "node-c"); b != (db.Resources{}) || c != admitted.Reserved {
		t.Fatalf("node-b=%+v node-c=%+v", b, c)
	}
	var p db.ResumePayload
	if err := json.Unmarshal([]byte(next.Payload), &p); err != nil {
		t.Fatal(err)
	}
	if p.NodeID != "node-c" || p.FromManifest != r.gen.ManifestKey || p.EvictNode != "node-a" ||
		next.SourceGenerationID == nil || *next.SourceGenerationID != r.gen.ID {
		t.Fatalf("next job payload=%+v source=%v", p, next.SourceGenerationID)
	}
	var state db.JobState
	if err := r.s.Pool().QueryRow(t.Context(), `SELECT state FROM jobs WHERE id = $1`, job.ID).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != db.JobDone {
		t.Fatalf("old job = %s, want done", state)
	}
}

// TestStopDuringCrossNodeResumeRecordsDiskNode: a stop of a cross-node resume
// (disk_node_id NULL while the restore runs on node-b) finalizes with the
// disk on node-b, the node StopVM ran on.
func TestStopDuringCrossNodeResumeRecordsDiskNode(t *testing.T) {
	r := rsmSetup(t, 10)
	acc, err := r.s.AcceptSandboxResumeElsewhere(t.Context(), r.params("node-b"))
	if err != nil {
		t.Fatal(err)
	}
	stopped, _ := phase1Stopped(t, r.s, r.own, acc.Sandbox)
	if stopped.DiskNodeID == nil || *stopped.DiskNodeID != "node-b" {
		t.Fatalf("disk node after the stop = %v, want node-b", stopped.DiskNodeID)
	}
}

// TestEvictionsStore covers OpenEvictions with the Current flag, closing a
// row idempotently and a second eviction of the same copy after the first
// closed.
func TestEvictionsStore(t *testing.T) {
	r := rsmSetup(t, 10)
	if _, err := r.s.AcceptSandboxResumeElsewhere(t.Context(), r.params("node-b")); err != nil {
		t.Fatal(err)
	}
	evs, err := r.s.OpenEvictions(t.Context(), "node-a")
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 1 || evs[0].SandboxID != r.sbx.ID || evs[0].OwnerID != r.own || evs[0].Current ||
		evs[0].Reason != db.EvictionReasonResumedElsewhere {
		t.Fatalf("open evictions = %+v", evs)
	}
	if other, err := r.s.OpenEvictions(t.Context(), "node-b"); err != nil || len(other) != 0 {
		t.Fatalf("node-b evictions = %+v, %v", other, err)
	}
	for range 2 {
		if err := r.s.MarkEvictionDone(t.Context(), "node-a", r.sbx.ID); err != nil {
			t.Fatal(err)
		}
	}
	if evs, err := r.s.OpenEvictions(t.Context(), "node-a"); err != nil || len(evs) != 0 {
		t.Fatalf("after done = %+v, %v", evs, err)
	}
	// A later eviction of the same copy is a new row.
	rsmExec(t, r.s, `INSERT INTO vm_evictions (id, node_id, sandbox_id, reason) VALUES ($1, 'node-a', $2, 'test')`,
		uuid.NewV7(), r.sbx.ID)
	if n := count(t, r.s, `SELECT count(*) FROM vm_evictions WHERE sandbox_id = $1`, r.sbx.ID); n != 2 {
		t.Fatalf("eviction rows = %d, want 2", n)
	}
}

// TestEvictionsMigrationDownGuard: the Down refuses while an eviction is
// open, works once it is done, and Up applies again.
func TestEvictionsMigrationDownGuard(t *testing.T) {
	r := rsmSetup(t, 10)
	if _, err := r.s.AcceptSandboxResumeElsewhere(t.Context(), r.params("node-b")); err != nil {
		t.Fatal(err)
	}
	if err := snapshotMigrateDownTo(t, r.s, 8); err == nil {
		t.Fatal("down with an open eviction succeeded")
	}
	if err := r.s.MarkEvictionDone(t.Context(), "node-a", r.sbx.ID); err != nil {
		t.Fatal(err)
	}
	if err := snapshotMigrateDownTo(t, r.s, 8); err != nil {
		t.Fatalf("down: %v", err)
	}
	if err := r.s.Migrate(t.Context()); err != nil {
		t.Fatalf("up again: %v", err)
	}
	if n := count(t, r.s, `SELECT count(*) FROM vm_evictions`); n != 0 {
		t.Fatalf("rows after up = %d", n)
	}
}

func rsmExec(t *testing.T, s *db.Store, sql string, args ...any) {
	t.Helper()
	if _, err := s.Pool().Exec(t.Context(), sql, args...); err != nil {
		t.Fatal(err)
	}
}
