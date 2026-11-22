package db_test

// PostgreSQL tests of the chunk collector's store: the restore source every
// manifest restore records, the liveness rules, the purge guard and the sweep
// fence. They skip unless BBRAFT_TEST_DATABASE_URL is set (dbtest.Open).

import (
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/snapformat"
)

// gcRestoreSource reads sandboxes.restore_source_generation_id.
func gcRestoreSource(t *testing.T, s *db.Store, sandboxID string) *uuid.UUID {
	t.Helper()
	var src *uuid.UUID
	if err := s.Pool().QueryRow(t.Context(),
		`SELECT restore_source_generation_id FROM sandboxes WHERE id = $1`, sandboxID).Scan(&src); err != nil {
		t.Fatalf("read restore source: %v", err)
	}
	return src
}

// gcClaimFor claims jobs until one belongs to sandboxID, completing the
// others (the fixtures' own provision jobs).
func gcClaimFor(t *testing.T, s *db.Store, worker, sandboxID string) db.Job {
	t.Helper()
	for range 20 {
		job, err := s.ClaimJob(t.Context(), worker, time.Minute)
		if err != nil {
			t.Fatalf("claim job for %s: %v", sandboxID, err)
		}
		if job.SandboxID != nil && *job.SandboxID == sandboxID {
			return job
		}
		if err := s.CompleteJob(t.Context(), job.ID, worker, job.LeaseToken); err != nil {
			t.Fatalf("complete unrelated job: %v", err)
		}
	}
	t.Fatalf("no job for %s appeared", sandboxID)
	return db.Job{}
}

// TestRestoreSourceRecordedOnProvision: a manifest restore reaching READY
// through the provision job writes the job's source generation in the same
// move as hydrated_at; a restore the job did not source from a generation, and
// a plain boot, leave it empty.
func TestRestoreSourceRecordedOnProvision(t *testing.T) {
	s, own := genStore(t)
	src := genSandbox(t, s, own, db.StateReady)
	gen := genComplete(t, s, own, src.ID, time.Now())

	res := create(t, s, db.CreateSandboxParams{
		Owner: own, FromManifest: gen.ManifestKey, SourceGenerationID: &gen.ID,
	})
	if got := gcRestoreSource(t, s, res.Sandbox.ID); got != nil {
		t.Fatalf("restore source before READY = %v, want none", got)
	}
	job := gcClaimFor(t, s, "gc-worker", res.Sandbox.ID)
	ready, err := s.TransitionSandboxJob(t.Context(), job.ID, "gc-worker", job.LeaseToken, db.TransitionParams{
		SandboxID: res.Sandbox.ID, Owner: own, FromStates: []db.SandboxState{db.StateProvisioning},
		ExpectedVersion: res.Sandbox.Version, To: db.StateReady, Hydrated: true,
	})
	if err != nil {
		t.Fatalf("transition: %v", err)
	}
	if ready.HydratedAt == nil {
		t.Fatal("hydrated_at not set")
	}
	if got := gcRestoreSource(t, s, res.Sandbox.ID); got == nil || *got != gen.ID {
		t.Fatalf("restore source = %v, want %s", got, gen.ID)
	}

	// A plain boot has no source and does not hydrate.
	plain := create(t, s, db.CreateSandboxParams{Owner: own})
	pjob := gcClaimFor(t, s, "gc-worker", plain.Sandbox.ID)
	if _, err := s.TransitionSandboxJob(t.Context(), pjob.ID, "gc-worker", pjob.LeaseToken, db.TransitionParams{
		SandboxID: plain.Sandbox.ID, Owner: own, FromStates: []db.SandboxState{db.StateProvisioning},
		ExpectedVersion: plain.Sandbox.Version, To: db.StateReady,
	}); err != nil {
		t.Fatalf("plain transition: %v", err)
	}
	if got := gcRestoreSource(t, s, plain.Sandbox.ID); got != nil {
		t.Fatalf("plain boot restore source = %v", got)
	}
}

// TestRestoreSourceRecordedOnResumeElsewhere: a cross-node resume records its
// job's source generation when it finalizes; a same-node resume records none.
func TestRestoreSourceRecordedOnResumeElsewhere(t *testing.T) {
	r, admitted, job := rsmAdmitted(t)
	if got := gcRestoreSource(t, r.s, admitted.ID); got != nil {
		t.Fatalf("restore source before finalize = %v", got)
	}
	if _, err := r.s.FinalizeSandboxResume(t.Context(), job.ID, "rsm-worker", job.LeaseToken, db.FinalizeResumeParams{
		Owner: r.own, SandboxID: admitted.ID, Generation: admitted.LifecycleGeneration, Version: admitted.Version,
	}); err != nil {
		t.Fatalf("finalize: %v", err)
	}
	if got := gcRestoreSource(t, r.s, admitted.ID); got == nil || *got != r.gen.ID {
		t.Fatalf("restore source = %v, want %s", got, r.gen.ID)
	}

	same := rsmSetup(t, 10)
	acc, err := same.s.AcceptSandboxResume(t.Context(), db.ResumeParams{Owner: same.own, SandboxID: same.sbx.ID})
	if err != nil {
		t.Fatal(err)
	}
	sjob := phase1ClaimKind(t, same.s, "rsm-worker", db.JobResume)
	if _, err := same.s.FinalizeSandboxResume(t.Context(), sjob.ID, "rsm-worker", sjob.LeaseToken, db.FinalizeResumeParams{
		Owner: same.own, SandboxID: same.sbx.ID, Generation: acc.Sandbox.LifecycleGeneration, Version: acc.Sandbox.Version,
	}); err != nil {
		t.Fatal(err)
	}
	if got := gcRestoreSource(t, same.s, same.sbx.ID); got != nil {
		t.Fatalf("same-node resume restore source = %v", got)
	}
}

// TestRestoreSourceRecordedOnNodeLoss: a sandbox moved off a lost node records
// the generation it resumes from.
func TestRestoreSourceRecordedOnNodeLoss(t *testing.T) {
	f := nlSetup(t)
	gen := f.nlGeneration(t, 10)
	moved, _, err := f.s.MoveLostSandbox(t.Context(), f.params(t), db.NodeLossMove{
		ToNode: "node-b", GenerationID: gen.ID, ManifestKey: gen.ManifestKey,
	})
	if err != nil {
		t.Fatalf("move: %v", err)
	}
	job := phase1ClaimKind(t, f.s, "nl-worker", db.JobResume)
	if _, err := f.s.FinalizeSandboxResume(t.Context(), job.ID, "nl-worker", job.LeaseToken, db.FinalizeResumeParams{
		Owner: f.own, SandboxID: moved.ID, Generation: moved.LifecycleGeneration, Version: moved.Version,
	}); err != nil {
		t.Fatalf("finalize: %v", err)
	}
	if got := gcRestoreSource(t, f.s, moved.ID); got == nil || *got != gen.ID {
		t.Fatalf("restore source = %v, want %s", got, gen.ID)
	}
}

// gcFixture is a store with one owner whose generations the liveness tests
// write directly.
type gcFixture struct {
	s   *db.Store
	own uuid.UUID
}

func gcSetup(t *testing.T) gcFixture {
	t.Helper()
	s, own := genStore(t)
	return gcFixture{s: s, own: own}
}

// sandbox creates a ready sandbox and returns it with its chain.
func (f gcFixture) sandbox(t *testing.T) db.Sandbox {
	t.Helper()
	return genSandbox(t, f.s, f.own, db.StateReady)
}

// gen inserts one generation row of sbx's chain: a completed one cut at cut,
// or a row of status failed or pending (cut is ignored).
func (f gcFixture) gen(t *testing.T, sbx db.Sandbox, number uint32, status string, cut time.Time) db.Generation {
	t.Helper()
	id := uuid.NewV7()
	key := snapformat.ManifestKey(sbx.ChainID.String(), number)
	kind := "incremental"
	if number == 0 {
		kind = "base"
	}
	if status == "completed" {
		genExec(t, f.s, `
			INSERT INTO snapshots (id, owner_id, sandbox_id, chain_id, generation, kind, status,
				manifest_key, node_id, image, disk_bytes, chunks, chunks_uploaded, bytes_uploaded,
				content_bytes, frozen, cut_at, started_at, completed_at, deadline_at)
			VALUES ($1, $2, $3, $4, $5, $6, 'completed', $7, 'node-a', 'img-1', 1, 1, 1, 1, 1,
				true, $8, $8, $8, $8)`,
			id, f.own, sbx.ID, sbx.ChainID, int64(number), kind, key, cut)
	} else {
		genExec(t, f.s, `
			INSERT INTO snapshots (id, owner_id, sandbox_id, chain_id, generation, kind, status,
				manifest_key, node_id, started_at, deadline_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'node-a', now(), now() + interval '1 hour')`,
			id, f.own, sbx.ID, sbx.ChainID, int64(number), kind, status, key)
	}
	return db.Generation{ID: id, SandboxID: sbx.ID, ChainID: sbx.ChainID, Number: number, ManifestKey: key}
}

// live returns the manifest keys of the live set for a history cutoff.
func (f gcFixture) live(t *testing.T, cutoff time.Time) []string {
	t.Helper()
	gens, err := f.s.GCLiveGenerations(t.Context(), cutoff)
	if err != nil {
		t.Fatalf("live generations: %v", err)
	}
	keys := make([]string, len(gens))
	for i, g := range gens {
		keys[i] = g.ManifestKey
	}
	slices.Sort(keys)
	return keys
}

// want asserts the live set is exactly the given generations.
func (f gcFixture) want(t *testing.T, cutoff time.Time, why string, gens ...db.Generation) {
	t.Helper()
	want := make([]string, len(gens))
	for i, g := range gens {
		want[i] = g.ManifestKey
	}
	slices.Sort(want)
	if got := f.live(t, cutoff); !slices.Equal(got, want) {
		t.Fatalf("%s: live = %v, want %v", why, got, want)
	}
}

// TestGCLiveLatestAndHistory: rule 1. The latest completed generation is
// live whatever its age, completed ones cut after the history cutoff are
// live, older ones are not.
func TestGCLiveLatestAndHistory(t *testing.T) {
	f := gcSetup(t)
	sbx := f.sandbox(t)
	now := time.Now()
	f.gen(t, sbx, 0, "completed", now.Add(-10*time.Hour))
	g1 := f.gen(t, sbx, 1, "completed", now.Add(-3*time.Hour))
	g2 := f.gen(t, sbx, 2, "completed", now.Add(-time.Hour))
	g3 := f.gen(t, sbx, 3, "completed", now.Add(-30*time.Minute))
	f.want(t, now.Add(-2*time.Hour), "history window", g2, g3)
	f.want(t, now.Add(-4*time.Hour), "wider window", g1, g2, g3)
	f.want(t, now.Add(-time.Minute), "only the latest", g3)
}

// TestGCLiveLatestSurvivesAge: the latest completed generation of a sandbox
// that never snapshots again stays live however old it is.
func TestGCLiveLatestSurvivesAge(t *testing.T) {
	f := gcSetup(t)
	sbx := f.sandbox(t)
	old := f.gen(t, sbx, 0, "completed", time.Now().Add(-90*24*time.Hour))
	f.want(t, time.Now().Add(-time.Hour), "ancient latest", old)
}

// TestGCLiveNewerThanLatestCompleted: rule 3. A failed or abandoned attempt
// newer than the latest completed generation is live; one older than it is
// not.
func TestGCLiveNewerThanLatestCompleted(t *testing.T) {
	f := gcSetup(t)
	now := time.Now()
	cutoff := now.Add(-time.Minute)

	sbx := f.sandbox(t)
	done := f.gen(t, sbx, 0, "completed", now.Add(-48*time.Hour))
	failed := f.gen(t, sbx, 1, "failed", time.Time{})
	abandoned := f.gen(t, sbx, 2, "failed", time.Time{})
	f.want(t, cutoff, "failed attempts above the latest completed", done, failed, abandoned)

	// A later completed generation retires them.
	latest := f.gen(t, sbx, 3, "completed", now.Add(-47*time.Hour))
	f.want(t, cutoff, "a newer completed generation retires the failed attempts", latest)

	// With nothing completed, every attempt of the chain is newer than it.
	bare := f.sandbox(t)
	first := f.gen(t, bare, 0, "failed", time.Time{})
	second := f.gen(t, bare, 1, "failed", time.Time{})
	got := f.live(t, cutoff)
	for _, g := range []db.Generation{first, second} {
		if !slices.Contains(got, g.ManifestKey) {
			t.Fatalf("attempt %d of a chain with nothing completed is not live: %v", g.Number, got)
		}
	}
}

// TestGCLiveDeletedAndExpiredSandboxes: a deleted or expired sandbox's chain
// is not live (rules 1 to 3), except what is pending or pinned.
func TestGCLiveDeletedAndExpiredSandboxes(t *testing.T) {
	f := gcSetup(t)
	now := time.Now()
	cutoff := now.Add(-time.Hour)

	deleted := f.sandbox(t)
	dDone := f.gen(t, deleted, 0, "completed", now.Add(-time.Minute))
	f.gen(t, deleted, 1, "failed", time.Time{})
	genExec(t, f.s, `UPDATE sandboxes SET deleted_at = now() WHERE id = $1`, deleted.ID)
	f.want(t, cutoff, "deleted sandbox")

	expired := f.sandbox(t)
	f.gen(t, expired, 0, "completed", now.Add(-time.Minute))
	genExec(t, f.s, `UPDATE sandboxes SET data_expired_at = now() WHERE id = $1`, expired.ID)
	f.want(t, cutoff, "expired sandbox")

	// A pending attempt is live for every sandbox: it may still be writing.
	pending := f.gen(t, expired, 1, "pending", time.Time{})
	f.want(t, cutoff, "pending on an expired sandbox", pending)
	genExec(t, f.s, `UPDATE snapshots SET status = 'failed' WHERE id = $1`, pending.ID)

	// A saved snapshot pins a generation of a deleted sandbox.
	f.pinSaved(t, deleted, dDone, "ready", false, false)
	f.want(t, cutoff, "saved pin on a deleted sandbox", dDone)
}

// pinSaved inserts a generation-backed saved snapshot of sbx pinning gen.
func (f gcFixture) pinSaved(t *testing.T, sbx db.Sandbox, gen db.Generation, status string, deleted, retired bool) {
	t.Helper()
	artifact := "snap-" + hex32()
	genExec(t, f.s, `
		INSERT INTO saved_snapshots (id, owner_id, name, sandbox_id, status, artifact_id, generation,
			generation_id, is_current, deleted_at, retired_at)
		VALUES ($1, $2, $3, $4, $5, $6, 1, $7, true,
			CASE WHEN $8 THEN now() END, CASE WHEN $9 THEN now() END)`,
		uuid.NewV7(), f.own, "pin-"+hex32(), sbx.ID, status, artifact, gen.ID, deleted, retired)
}

// hex32 is 32 lowercase hex digits for artifact ids and names.
func hex32() string { return strings.ReplaceAll(uuid.NewV7().String(), "-", "") }

// TestGCLiveSavedSnapshotPins: rule 4. Only a saved snapshot that is not
// deleted, not retired and saving or ready pins its generation.
func TestGCLiveSavedSnapshotPins(t *testing.T) {
	f := gcSetup(t)
	now := time.Now()
	cutoff := now.Add(-time.Hour)
	gone := f.sandbox(t)
	genExec(t, f.s, `UPDATE sandboxes SET deleted_at = now() WHERE id = $1`, gone.ID)

	for _, tc := range []struct {
		name                     string
		status                   string
		deleted, retired, pinned bool
	}{
		{"ready", "ready", false, false, true},
		{"saving", "saving", false, false, true},
		{"deleted name", "ready", true, false, false},
		{"retired by a newer save", "ready", false, true, false},
		{"failed save", "failed", false, false, false},
	} {
		sbx := f.sandbox(t)
		gen := f.gen(t, sbx, 0, "completed", now.Add(-24*time.Hour))
		// The pinned sandbox is gone, so only the pin can keep the generation.
		genExec(t, f.s, `UPDATE sandboxes SET deleted_at = now() WHERE id = $1`, sbx.ID)
		f.pinSaved(t, sbx, gen, tc.status, tc.deleted, tc.retired)
		if got := slices.Contains(f.live(t, cutoff), gen.ManifestKey); got != tc.pinned {
			t.Errorf("%s: live = %v, want %v", tc.name, got, tc.pinned)
		}
	}
}

// TestGCLiveJobSourcePins: rule 4. A queued or running job's source
// generation is live; a finished job's is not.
func TestGCLiveJobSourcePins(t *testing.T) {
	f := gcSetup(t)
	now := time.Now()
	cutoff := now.Add(-time.Hour)
	for _, tc := range []struct {
		state  string
		pinned bool
	}{
		{"queued", true}, {"running", true}, {"done", false}, {"failed", false}, {"cancelled", false},
	} {
		src := f.sandbox(t)
		gen := f.gen(t, src, 0, "completed", now.Add(-24*time.Hour))
		genExec(t, f.s, `UPDATE sandboxes SET deleted_at = now() WHERE id = $1`, src.ID)
		target := f.sandbox(t)
		genExec(t, f.s, `
			INSERT INTO jobs (id, kind, owner_id, sandbox_id, state, source_generation_id)
			VALUES ($1, 'sandbox.provision', $2, $3, $4, $5)`,
			uuid.NewV7(), f.own, target.ID, tc.state, gen.ID)
		if got := slices.Contains(f.live(t, cutoff), gen.ManifestKey); got != tc.pinned {
			t.Errorf("job %s: live = %v, want %v", tc.state, got, tc.pinned)
		}
	}
}

// TestGCLiveRestoreSource: rule 2. The generation a sandbox was restored from
// stays live until the sandbox has a completed generation cut after the
// restore.
func TestGCLiveRestoreSource(t *testing.T) {
	f := gcSetup(t)
	now := time.Now()
	cutoff := now.Add(-time.Hour)

	src := f.sandbox(t)
	srcGen := f.gen(t, src, 0, "completed", now.Add(-24*time.Hour))
	genExec(t, f.s, `UPDATE sandboxes SET deleted_at = now() WHERE id = $1`, src.ID)

	fork := f.sandbox(t)
	genExec(t, f.s, `UPDATE sandboxes SET restore_source_generation_id = $2, hydrated_at = $3 WHERE id = $1`,
		fork.ID, srcGen.ID, now.Add(-time.Hour))
	f.want(t, cutoff, "restored, no generation of its own yet", srcGen)

	// A generation cut before the restore does not release the source.
	f.gen(t, fork, 0, "completed", now.Add(-2*time.Hour))
	if !slices.Contains(f.live(t, cutoff), srcGen.ManifestKey) {
		t.Fatal("a generation cut before the restore released the restore source")
	}
	// One cut after it does.
	f.gen(t, fork, 1, "completed", now.Add(-10*time.Minute))
	if slices.Contains(f.live(t, cutoff), srcGen.ManifestKey) {
		t.Fatal("restore source still live after the sandbox has its own generation")
	}

	// A deleted or expired restored sandbox keeps nothing.
	other := f.sandbox(t)
	genExec(t, f.s, `UPDATE sandboxes SET restore_source_generation_id = $2, hydrated_at = now(),
		deleted_at = now() WHERE id = $1`, other.ID, srcGen.ID)
	// The fork above released the source, so only this sandbox could keep it.
	if slices.Contains(f.live(t, cutoff), srcGen.ManifestKey) {
		t.Fatal("deleted sandbox kept its restore source alive")
	}
}

// TestGCLivePurgedRowsAreNeverLive: a purged generation has no manifest to
// keep, whatever else names it.
func TestGCLivePurgedRowsAreNeverLive(t *testing.T) {
	f := gcSetup(t)
	sbx := f.sandbox(t)
	g := f.gen(t, sbx, 0, "completed", time.Now())
	genExec(t, f.s, `UPDATE snapshots SET purged_at = now() WHERE id = $1`, g.ID)
	f.want(t, time.Now().Add(-time.Hour), "purged latest")
}

// TestGCPurgeGenerations purges dead rows, declines live ones and treats a
// missing or already purged row as purged.
func TestGCPurgeGenerations(t *testing.T) {
	f := gcSetup(t)
	now := time.Now()
	cutoff := now.Add(-time.Hour)

	sbx := f.sandbox(t)
	old := f.gen(t, sbx, 0, "completed", now.Add(-48*time.Hour))
	live := f.gen(t, sbx, 1, "completed", now.Add(-time.Minute))
	gone := f.sandbox(t)
	dead := f.gen(t, gone, 0, "completed", now.Add(-time.Minute))
	genExec(t, f.s, `UPDATE sandboxes SET deleted_at = now() WHERE id = $1`, gone.ID)
	purged := f.gen(t, f.sandbox(t), 0, "completed", now)
	genExec(t, f.s, `UPDATE snapshots SET purged_at = now() - interval '1 hour' WHERE id = $1`, purged.ID)
	missing := snapformat.ManifestKey(uuid.NewV7().String(), 0)

	declined, err := f.s.GCPurgeGenerations(t.Context(), cutoff,
		[]string{old.ManifestKey, live.ManifestKey, dead.ManifestKey, purged.ManifestKey, missing})
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	if !slices.Equal(declined, []string{live.ManifestKey}) {
		t.Fatalf("declined = %v, want only the live latest", declined)
	}
	for _, tc := range []struct {
		g      db.Generation
		purged bool
	}{{old, true}, {live, false}, {dead, true}, {purged, true}} {
		var at *time.Time
		if err := f.s.Pool().QueryRow(t.Context(), `SELECT purged_at FROM snapshots WHERE id = $1`, tc.g.ID).Scan(&at); err != nil {
			t.Fatal(err)
		}
		if (at != nil) != tc.purged {
			t.Errorf("generation %d purged_at = %v, want purged %v", tc.g.Number, at, tc.purged)
		}
	}
	if _, err := f.s.GCPurgeGenerations(t.Context(), cutoff, []string{"manifests/not-a-key"}); err == nil {
		t.Fatal("purge of a malformed manifest key succeeded")
	}
	if got, err := f.s.GCPurgeGenerations(t.Context(), cutoff, nil); err != nil || got != nil {
		t.Fatalf("purge of nothing = %v, %v", got, err)
	}
}

// TestGCPurgeSeesAdmissionThatCommitsFirst: a job pinning the generation is
// inserted and its transaction holds the generation row FOR SHARE (as fork
// admission does) while the purge starts; the purge waits for the row and,
// once the admission commits, declines it. A purge that took its liveness
// snapshot before the wait would purge a generation a job is about to read.
func TestGCPurgeSeesAdmissionThatCommitsFirst(t *testing.T) {
	f := gcSetup(t)
	now := time.Now()
	src := f.sandbox(t)
	gen := f.gen(t, src, 0, "completed", now.Add(-24*time.Hour))
	genExec(t, f.s, `UPDATE sandboxes SET deleted_at = now() WHERE id = $1`, src.ID)
	target := f.sandbox(t)

	ctx := t.Context()
	tx, err := f.s.Pool().Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op after commit
	if _, err := tx.Exec(ctx, `SELECT id FROM snapshots WHERE id = $1 FOR SHARE`, gen.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO jobs (id, kind, owner_id, sandbox_id, state, source_generation_id)
		VALUES ($1, 'sandbox.provision', $2, $3, 'queued', $4)`,
		uuid.NewV7(), f.own, target.ID, gen.ID); err != nil {
		t.Fatal(err)
	}

	type result struct {
		declined []string
		err      error
	}
	done := make(chan result, 1)
	go func() {
		d, err := f.s.GCPurgeGenerations(ctx, now.Add(-time.Hour), []string{gen.ManifestKey})
		done <- result{d, err}
	}()
	select {
	case r := <-done:
		t.Fatalf("purge finished while the admission held the row: %+v", r)
	case <-time.After(300 * time.Millisecond):
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	r := <-done
	if r.err != nil {
		t.Fatalf("purge: %v", r.err)
	}
	if !slices.Equal(r.declined, []string{gen.ManifestKey}) {
		t.Fatalf("declined = %v, want the pinned generation", r.declined)
	}
	if n := count(t, f.s, `SELECT count(*) FROM snapshots WHERE id = $1 AND purged_at IS NOT NULL`, gen.ID); n != 0 {
		t.Fatal("the pinned generation was purged")
	}
}

// TestGCSweepFence: the flag is claimed by one sweep, refuses snapshot
// attempts while set, is released by its end and a flag older than the stale
// bound is taken over by a new sweep or cleared by recovery.
func TestGCSweepFence(t *testing.T) {
	s, own := genStore(t)
	ctx := t.Context()
	sbx := genSandbox(t, s, own, db.StateReady)

	if ok, err := s.GCBeginSweep(ctx, time.Hour); err != nil || !ok {
		t.Fatalf("first begin = %v, %v", ok, err)
	}
	if ok, err := s.GCBeginSweep(ctx, time.Hour); err != nil || ok {
		t.Fatalf("second begin while sweeping = %v, %v; want refused", ok, err)
	}
	job := genJob(t, s, own, sbx.ID)
	_, err := s.BeginGeneration(ctx, db.BeginGenerationParams{
		Owner: own, SandboxID: sbx.ID, Timeout: time.Minute, JobID: job.ID, LeaseToken: job.LeaseToken,
	})
	if !errors.Is(err, db.ErrGCSweeping) {
		t.Fatalf("begin generation while sweeping = %v, want ErrGCSweeping", err)
	}
	if cleared, err := s.GCRecoverStaleSweep(ctx, time.Hour); err != nil || cleared {
		t.Fatalf("recover of a fresh flag = %v, %v; want left alone", cleared, err)
	}
	if err := s.GCEndSweep(ctx); err != nil {
		t.Fatal(err)
	}
	if sweeping, err := s.GCSweeping(ctx); err != nil || sweeping {
		t.Fatalf("sweeping after end = %v, %v", sweeping, err)
	}
	genBeginAs(t, s, own, sbx.ID, job)

	// A crashed sweep: the flag stays set and ages.
	if ok, err := s.GCBeginSweep(ctx, time.Hour); err != nil || !ok {
		t.Fatalf("begin = %v, %v", ok, err)
	}
	genExec(t, s, `UPDATE gc_state SET sweep_started_at = now() - interval '2 hours'`)
	if cleared, err := s.GCRecoverStaleSweep(ctx, time.Hour); err != nil || !cleared {
		t.Fatalf("recover of a stale flag = %v, %v; want cleared", cleared, err)
	}
	if sweeping, _ := s.GCSweeping(ctx); sweeping {
		t.Fatal("stale flag still set after recovery")
	}

	// A new sweep takes a stale flag over.
	if ok, _ := s.GCBeginSweep(ctx, time.Hour); !ok {
		t.Fatal("begin failed")
	}
	genExec(t, s, `UPDATE gc_state SET sweep_started_at = now() - interval '2 hours'`)
	if ok, err := s.GCBeginSweep(ctx, time.Hour); err != nil || !ok {
		t.Fatalf("begin over a stale flag = %v, %v; want taken over", ok, err)
	}
}

// gcInFlightIDs returns the ids the sweep fence lists, in query order.
func gcInFlightIDs(t *testing.T, s *db.Store, margin time.Duration) []uuid.UUID {
	t.Helper()
	gens, err := s.GCInFlightGenerations(t.Context(), margin)
	if err != nil {
		t.Fatalf("in-flight generations: %v", err)
	}
	ids := make([]uuid.UUID, len(gens))
	for i, g := range gens {
		ids[i] = g.ID
	}
	return ids
}

// gcWantInFlight asserts the sweep fence lists exactly the given attempts.
func gcWantInFlight(t *testing.T, s *db.Store, margin time.Duration, why string, want ...uuid.UUID) {
	t.Helper()
	got := gcInFlightIDs(t, s, margin)
	if len(got) != len(want) {
		t.Fatalf("%s: in-flight = %v, want %v", why, got, want)
	}
	for _, id := range want {
		if !slices.Contains(got, id) {
			t.Fatalf("%s: in-flight = %v, want %v", why, got, want)
		}
	}
}

// TestGCInFlightGenerationsHoldsFenceForFailedAttempts: the sweep fence waits
// for every attempt that is not completed while its deadline plus margin is
// ahead, including one failed before its deadline by a takeover or by
// FailGeneration, because only the attempt's deadline proves its call ended.
// A completed row is never listed, and an attempt past its deadline no longer
// holds the fence.
func TestGCInFlightGenerationsHoldsFenceForFailedAttempts(t *testing.T) {
	f := gcSetup(t)
	sbx := f.sandbox(t)

	// A live attempt sits ahead of its deadline.
	first := genBegin(t, f.s, f.own, sbx.ID)

	// Its owner loses the lease: a new attempt takes the row over, which
	// marks it failed as 'abandoned' while its call may still be running.
	genExec(t, f.s, `UPDATE jobs SET leased_until = now() - interval '1 minute' WHERE id = $1`, first.JobID)
	second := genBegin(t, f.s, f.own, sbx.ID)
	if n := count(t, f.s, `SELECT count(*) FROM snapshots
		WHERE id = $1 AND status = 'failed' AND error = 'abandoned'`, first.ID); n != 1 {
		t.Fatalf("taken-over row = %d failed rows, want 1", n)
	}
	gens, err := f.s.GCInFlightGenerations(t.Context(), time.Minute)
	if err != nil {
		t.Fatalf("in-flight generations: %v", err)
	}
	if len(gens) != 2 || gens[0].ID != first.ID || gens[0].SandboxID != sbx.ID ||
		gens[0].Number != first.Number || gens[0].DeadlineAt.IsZero() {
		t.Fatalf("in-flight after takeover = %+v, %v; want the taken-over attempt and the new one", gens, err)
	}

	// The taken-over attempt still holds the fence until its deadline plus
	// the margin, not until it was marked failed.
	genExec(t, f.s, `UPDATE snapshots SET deadline_at = now() - interval '30 seconds' WHERE id = $1`, first.ID)
	gcWantInFlight(t, f.s, time.Minute, "taken-over attempt inside the margin", first.ID, second.ID)
	gcWantInFlight(t, f.s, 10*time.Second, "taken-over attempt past the margin", second.ID)

	// Past its deadline it no longer holds the fence.
	genExec(t, f.s, `UPDATE snapshots SET deadline_at = now() - interval '5 minutes' WHERE id = $1`, first.ID)
	gcWantInFlight(t, f.s, time.Minute, "taken-over attempt past its deadline", second.ID)

	// An attempt failed through FailGeneration before its deadline holds the
	// fence the same way.
	if _, err := f.s.FailGeneration(t.Context(), second.ID, "snapshot failed", time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("fail generation: %v", err)
	}
	if n := count(t, f.s, `SELECT count(*) FROM snapshots WHERE id = $1 AND status = 'failed'`, second.ID); n != 1 {
		t.Fatalf("failed row = %d, want 1", n)
	}
	gcWantInFlight(t, f.s, time.Minute, "attempt failed before its deadline", second.ID)
	genExec(t, f.s, `UPDATE snapshots SET deadline_at = now() - interval '2 minutes' WHERE id = $1`, second.ID)
	gcWantInFlight(t, f.s, time.Minute, "attempt failed past its deadline")
}

// TestGCInFlightGenerationsIgnoresCompletedAndExpired: a completed row and an
// attempt past its deadline are not listed, whatever its status.
func TestGCInFlightGenerationsIgnoresCompletedAndExpired(t *testing.T) {
	f := gcSetup(t)
	sbx := f.sandbox(t)
	done := genComplete(t, f.s, f.own, sbx.ID, time.Now())
	expiredPending := f.gen(t, f.sandbox(t), 0, "pending", time.Time{})
	expiredFailed := f.gen(t, f.sandbox(t), 0, "failed", time.Time{})
	for _, g := range []db.Generation{expiredPending, expiredFailed} {
		genExec(t, f.s, `UPDATE snapshots SET deadline_at = now() - interval '5 minutes' WHERE id = $1`, g.ID)
	}

	gcWantInFlight(t, f.s, time.Minute, "completed and expired rows")
	if n := count(t, f.s, `SELECT count(*) FROM snapshots WHERE id = $1 AND status = 'completed'`, done.ID); n != 1 {
		t.Fatalf("completed row = %d, want 1", n)
	}
}

// TestGCMigrationDownUp: the migration reverses and applies again.
func TestGCMigrationDownUp(t *testing.T) {
	s, _ := genStore(t)
	provider := phase1Provider(t, s)
	latest := phase2LatestMigration(t)
	if _, err := provider.DownTo(t.Context(), latest-1); err != nil {
		t.Fatalf("down: %v", err)
	}
	if n := count(t, s, `SELECT count(*) FROM information_schema.columns
		WHERE table_schema = current_schema() AND column_name = 'restore_source_generation_id'`); n != 0 {
		t.Fatal("restore_source_generation_id survived the down migration")
	}
	if _, err := provider.Up(t.Context()); err != nil {
		t.Fatalf("up: %v", err)
	}
}
