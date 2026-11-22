package db_test

// PostgreSQL tests of the fork admission and its lease-fenced hand-off. They
// skip unless BBRAFT_TEST_DATABASE_URL is set; every test creates and drops
// its own schema.

import (
	"bytes"
	"crypto/sha256"
	"encoding/json/v2"
	"errors"
	"maps"
	"testing"
	"time"

	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/ids"
)

// forkSourceReady creates a ready owner sandbox on node-a with env and labels
// and one completed generation.
func forkSourceReady(t *testing.T, s *db.Store, own uuid.UUID) (db.Sandbox, db.Generation) {
	t.Helper()
	ttl := 900
	res := create(t, s, db.CreateSandboxParams{
		Owner: own, Name: "source", TTLSeconds: &ttl,
		Env:    map[string]string{"SOURCE_ONLY": "1"},
		Labels: map[string]string{"source": "label"},
	})
	genExec(t, s, `UPDATE sandboxes SET state = 'ready' WHERE id = $1`, res.Sandbox.ID)
	source, err := s.Sandbox(t.Context(), own, res.Sandbox.ID)
	if err != nil {
		t.Fatalf("reload source: %v", err)
	}
	gen := genComplete(t, s, own, source.ID, time.Now().UTC())
	return source, gen
}

// forkAdmitParams is one fork admission against node-a with defaults.
func forkAdmitParams(own uuid.UUID, sourceID string) db.ForkAdmitParams {
	ttl := 3600
	hash := sha256.Sum256([]byte("{}"))
	return db.ForkAdmitParams{
		ID: ids.Sandbox(), Owner: own, SourceID: sourceID,
		MachineType: "default", Name: "source fork",
		Labels:         map[string]string{"fork": "label"},
		InheritEnv:     true,
		TTLSeconds:     &ttl,
		NodeID:         "node-a",
		Reserve:        db.Resources{VCPUs: 1, MemoryMiB: 1024, DiskGiB: 10},
		IdempotencyKey: "fork-key",
		RequestSHA256:  hash[:],
	}
}

// TestForkAdmissionWritesANewChainSandbox checks the admission transaction:
// a new chain, the request's labels only, the source's sealed env, the
// default TTL, the reservation and the fork job.
func TestForkAdmissionWritesANewChainSandbox(t *testing.T) {
	s := dbtestOpen(t)
	own := owner(t, s)
	node(t, s, "node-a", db.Resources{VCPUs: 64, MemoryMiB: 65536, DiskGiB: 1000})
	source, gen := forkSourceReady(t, s, own)
	before := nodeReserved(t, s, "node-a")

	p := forkAdmitParams(own, source.ID)
	result, err := s.AdmitFork(t.Context(), p)
	if err != nil {
		t.Fatalf("admit fork: %v", err)
	}
	fork := result.Sandbox
	if fork.ID != p.ID || fork.OwnerID != own {
		t.Fatalf("fork row = %+v", fork)
	}
	if fork.State != db.StateProvisioning {
		t.Fatalf("fork state = %s, want provisioning", fork.State)
	}
	if fork.ChainID == uuid.Nil() || fork.ChainID == source.ChainID {
		t.Fatalf("fork chain = %s, source chain = %s; want a new chain", fork.ChainID, source.ChainID)
	}
	if !fork.SnapshotsEnabled {
		t.Fatal("fork must inherit snapshots on (its source has them)")
	}
	if fork.TTLSeconds == nil || *fork.TTLSeconds != 3600 {
		t.Fatalf("fork ttl = %v, want the 3600 default (not the source's)", fork.TTLSeconds)
	}
	if len(fork.Labels) != 1 || fork.Labels["fork"] != "label" {
		t.Fatalf("fork labels = %v, want the request's labels only", fork.Labels)
	}
	if fork.Reserved != p.Reserve || fork.Desired != p.Reserve {
		t.Fatalf("fork reservation = %+v/%+v, want %+v", fork.Reserved, fork.Desired, p.Reserve)
	}

	var sourceSealed, forkSealed []byte
	if err := s.Pool().QueryRow(t.Context(),
		`SELECT env_sealed FROM sandboxes WHERE id = $1`, source.ID).Scan(&sourceSealed); err != nil {
		t.Fatal(err)
	}
	if err := s.Pool().QueryRow(t.Context(),
		`SELECT env_sealed FROM sandboxes WHERE id = $1`, fork.ID).Scan(&forkSealed); err != nil {
		t.Fatal(err)
	}
	// The ciphertext is bound to its sandbox id, so the inherited copy must
	// differ from the source's bytes while decrypting to the same values.
	if bytes.Equal(sourceSealed, forkSealed) {
		t.Fatal("the fork reused the source's env ciphertext; it is bound to the sandbox id")
	}
	if got, err := s.DecryptSandboxEnvForNode(t.Context(), own, fork.ID); err != nil ||
		!maps.Equal(got, map[string]string{"SOURCE_ONLY": "1"}) {
		t.Fatalf("inherited env = %v (%v), want the source's values", got, err)
	}

	if got := nodeReserved(t, s, "node-a"); got != (db.Resources{
		VCPUs: before.VCPUs + 1, MemoryMiB: before.MemoryMiB + 1024, DiskGiB: before.DiskGiB + 10,
	}) {
		t.Fatalf("node reservation = %+v, want the fork added to %+v", got, before)
	}

	if result.Job.Kind != db.JobFork || result.Job.SandboxID == nil || *result.Job.SandboxID != fork.ID {
		t.Fatalf("fork job = %+v", result.Job)
	}
	var payload db.ForkPayload
	if err := json.Unmarshal([]byte(result.Job.Payload), &payload); err != nil {
		t.Fatalf("fork payload: %v", err)
	}
	if payload.SourceID != source.ID || payload.NodeID != "node-a" {
		t.Fatalf("fork payload = %+v", payload)
	}
	if result.Job.SourceGenerationID != nil {
		t.Fatalf("ready-source fork pinned generation %v; the job cuts a fresh one", result.Job.SourceGenerationID)
	}

	// A replay returns the same fork and writes nothing.
	replay, err := s.AdmitFork(t.Context(), p)
	if err != nil {
		t.Fatalf("replay fork: %v", err)
	}
	if !replay.Replayed || replay.Sandbox.ID != fork.ID {
		t.Fatalf("replay = %+v, want the original fork %s", replay, fork.ID)
	}
	if n := count(t, s, `SELECT count(*) FROM sandboxes WHERE id = $1`, fork.ID); n != 1 {
		t.Fatalf("fork rows after replay = %d, want 1", n)
	}
	originalHash := p.RequestSHA256
	if again, found, err := s.ReplayFork(t.Context(), own, "fork-key", originalHash); err != nil || !found ||
		again.Sandbox.ID != fork.ID {
		t.Fatalf("ReplayFork = %+v/%t/%v, want the fork %s", again, found, err, fork.ID)
	}
	if _, found, err := s.ReplayFork(t.Context(), own, "no-such-key", originalHash); err != nil || found {
		t.Fatalf("unknown-key replay = %t/%v, want not found", found, err)
	}
	// The same key with another body is refused.
	p.RequestSHA256 = []byte("other")
	if _, err := s.AdmitFork(t.Context(), p); !errors.Is(err, db.ErrIdempotencyReused) {
		t.Fatalf("reused key = %v, want ErrIdempotencyReused", err)
	}

	// The same key value on a create is a different operation: it must not
	// replay the fork.
	created := create(t, s, db.CreateSandboxParams{
		Owner: own, IdempotencyKey: "fork-key", RequestSHA256: []byte("create-body"),
	})
	if created.Sandbox.ID == fork.ID {
		t.Fatal("a create replayed a fork's idempotency key")
	}

	// Generation numbers stay on the source's chain: the fork starts empty.
	latest, ok, err := s.LatestGeneration(t.Context(), own, fork.ID)
	if err != nil || ok {
		t.Fatalf("fork generations = %+v/%t/%v, want none", latest, ok, err)
	}
	if gen.ChainID != source.ChainID {
		t.Fatalf("source generation chain = %s, want %s", gen.ChainID, source.ChainID)
	}
}

// TestForkEnvRebindsToTheForkSandbox checks the env handling: env ciphertext
// is bound to its sandbox id, so an inherited env is re-sealed for the fork and
// decrypts to the source's values, while a request env replaces it and a
// source without env inherits none.
func TestForkEnvRebindsToTheForkSandbox(t *testing.T) {
	s := dbtestOpen(t)
	own := owner(t, s)
	node(t, s, "node-a", db.Resources{VCPUs: 64, MemoryMiB: 65536, DiskGiB: 1000})

	res := create(t, s, db.CreateSandboxParams{
		Owner: own, Env: map[string]string{"SOURCE_ONLY": "one", "SHARED": "source"},
	})
	genExec(t, s, `UPDATE sandboxes SET state = 'ready' WHERE id = $1`, res.Sandbox.ID)
	source, err := s.Sandbox(t.Context(), own, res.Sandbox.ID)
	if err != nil {
		t.Fatalf("reload source: %v", err)
	}
	wantSource := map[string]string{"SOURCE_ONLY": "one", "SHARED": "source"}
	if got, err := s.DecryptSandboxEnvForNode(t.Context(), own, source.ID); err != nil ||
		!maps.Equal(got, wantSource) {
		t.Fatalf("source env = %v (%v), want %v", got, err, wantSource)
	}

	inherited := forkAdmitParams(own, source.ID)
	inherited.InheritEnv = true
	inherited.IdempotencyKey = ""
	fork, err := s.AdmitFork(t.Context(), inherited)
	if err != nil {
		t.Fatalf("admit inherited-env fork: %v", err)
	}
	got, err := s.DecryptSandboxEnvForNode(t.Context(), own, fork.Sandbox.ID)
	if err != nil {
		t.Fatalf("decrypt inherited env: %v", err)
	}
	if !maps.Equal(got, wantSource) {
		t.Fatalf("inherited env = %v, want the source's %v", got, wantSource)
	}

	replaced := forkAdmitParams(own, source.ID)
	replaced.InheritEnv = false
	replaced.Env = map[string]string{"REPLACED": "two"}
	replaced.IdempotencyKey = ""
	fork2, err := s.AdmitFork(t.Context(), replaced)
	if err != nil {
		t.Fatalf("admit replaced-env fork: %v", err)
	}
	got2, err := s.DecryptSandboxEnvForNode(t.Context(), own, fork2.Sandbox.ID)
	if err != nil {
		t.Fatalf("decrypt replaced env: %v", err)
	}
	if want := map[string]string{"REPLACED": "two"}; !maps.Equal(got2, want) {
		t.Fatalf("replaced env = %v, want %v", got2, want)
	}

	// A source with no env inherits none.
	bare := create(t, s, db.CreateSandboxParams{Owner: own})
	genExec(t, s, `UPDATE sandboxes SET state = 'ready' WHERE id = $1`, bare.Sandbox.ID)
	bareRow, err := s.Sandbox(t.Context(), own, bare.Sandbox.ID)
	if err != nil {
		t.Fatalf("reload bare source: %v", err)
	}
	noEnv := forkAdmitParams(own, bareRow.ID)
	noEnv.InheritEnv = true
	noEnv.IdempotencyKey = ""
	fork3, err := s.AdmitFork(t.Context(), noEnv)
	if err != nil {
		t.Fatalf("admit no-env fork: %v", err)
	}
	got3, err := s.DecryptSandboxEnvForNode(t.Context(), own, fork3.Sandbox.ID)
	if err != nil || len(got3) != 0 {
		t.Fatalf("no-env fork env = %v (%v), want empty", got3, err)
	}
}

// TestForkStartHandsOffOnce checks the lease-fenced hand-off: the provision
// job carries the source manifest and generation, a retried attempt returns
// the same provision job instead of enqueuing a second one, and a stale claim
// writes nothing.
func TestForkStartHandsOffOnce(t *testing.T) {
	s := dbtestOpen(t)
	own := owner(t, s)
	node(t, s, "node-a", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})
	source, gen := forkSourceReady(t, s, own)
	result, err := s.AdmitFork(t.Context(), forkAdmitParams(own, source.ID))
	if err != nil {
		t.Fatalf("admit fork: %v", err)
	}
	fork := result.Sandbox

	job := phase1ClaimKind(t, s, "fork-worker", db.JobFork)
	start := db.StartForkParams{
		ForkID: fork.ID, Owner: own, SourceID: source.ID, NodeID: "node-a", GenerationID: gen.ID,
	}
	provision, err := s.StartFork(t.Context(), job.ID, "fork-worker", job.LeaseToken, start)
	if err != nil {
		t.Fatalf("start fork: %v", err)
	}
	if provision.Kind != db.JobProvision || provision.SandboxID == nil || *provision.SandboxID != fork.ID {
		t.Fatalf("provision job = %+v", provision)
	}
	if provision.SourceGenerationID == nil || *provision.SourceGenerationID != gen.ID {
		t.Fatalf("provision source generation = %v, want %s", provision.SourceGenerationID, gen.ID)
	}
	var payload db.ProvisionPayload
	if err := json.Unmarshal([]byte(provision.Payload), &payload); err != nil {
		t.Fatalf("provision payload: %v", err)
	}
	if payload.FromManifest != gen.ManifestKey || payload.Image != gen.Image ||
		payload.NodeID != "node-a" || payload.Resources != fork.Reserved {
		t.Fatalf("provision payload = %+v", payload)
	}
	handed, err := s.Sandbox(t.Context(), own, fork.ID)
	if err != nil || handed.FromManifestKey != gen.ManifestKey {
		t.Fatalf("fork manifest = %q (%v), want %q", handed.FromManifestKey, err, gen.ManifestKey)
	}
	if state := phase1JobState(t, s, job.ID); state != db.JobDone {
		t.Fatalf("fork job state = %s, want done", state)
	}
	if n := count(t, s, `SELECT count(*) FROM jobs WHERE kind = $1 AND sandbox_id = $2`,
		string(db.JobProvision), fork.ID); n != 1 {
		t.Fatalf("provision jobs for the fork = %d, want 1", n)
	}

	// A stale claim (the job was redelivered) cannot hand off a second time.
	if _, err := s.Pool().Exec(t.Context(), `
		UPDATE jobs SET state = 'queued', leased_by = '', leased_until = NULL, lease_token = NULL
		WHERE id = $1`, job.ID); err != nil {
		t.Fatal(err)
	}
	retried, err := s.ClaimJob(t.Context(), "fork-worker-2", time.Minute)
	if err != nil || retried.ID != job.ID {
		t.Fatalf("reclaim fork job = %+v (%v)", retried, err)
	}
	if _, err := s.StartFork(t.Context(), job.ID, "fork-worker", job.LeaseToken, start); !errors.Is(err, db.ErrJobNotLeased) {
		t.Fatalf("stale claim = %v, want ErrJobNotLeased", err)
	}
	if n := count(t, s, `SELECT count(*) FROM jobs WHERE kind = $1 AND sandbox_id = $2`,
		string(db.JobProvision), fork.ID); n != 1 {
		t.Fatalf("provision jobs after a stale claim = %d, want 1", n)
	}
	// The redelivered attempt is idempotent: same provision job, no new one.
	again, err := s.StartFork(t.Context(), job.ID, "fork-worker-2", retried.LeaseToken, start)
	if err != nil {
		t.Fatalf("redelivered start fork: %v", err)
	}
	if again.ID != provision.ID {
		t.Fatalf("redelivered hand-off = %s, want %s", again.ID, provision.ID)
	}
	if n := count(t, s, `SELECT count(*) FROM jobs WHERE kind = $1 AND sandbox_id = $2`,
		string(db.JobProvision), fork.ID); n != 1 {
		t.Fatalf("provision jobs after a redelivered attempt = %d, want 1", n)
	}
}

// TestForkAdmissionRefusals checks the owner-scoped and capacity refusals and
// the archived-source generation pin.
func TestForkAdmissionRefusals(t *testing.T) {
	s := dbtestOpen(t)
	own := owner(t, s)
	// Every subtest leaves its sandboxes behind, so the fixture node must
	// hold all of them at once; the full-node case below asks for more than
	// the totals.
	node(t, s, "node-a", db.Resources{VCPUs: 64, MemoryMiB: 65536, DiskGiB: 1000})

	t.Run("snapshots off", func(t *testing.T) {
		res := create(t, s, db.CreateSandboxParams{Owner: own, SnapshotsEnabled: new(false)})
		genExec(t, s, `UPDATE sandboxes SET state = 'ready' WHERE id = $1`, res.Sandbox.ID)
		_, err := s.AdmitFork(t.Context(), forkAdmitParams(own, res.Sandbox.ID))
		if !errors.Is(err, db.ErrSnapshotsDisabled) {
			t.Fatalf("fork of a snapshots-off source = %v", err)
		}
	})

	t.Run("data expired", func(t *testing.T) {
		res := create(t, s, db.CreateSandboxParams{Owner: own})
		genExec(t, s, `UPDATE sandboxes SET state = 'archived', data_expired_at = now()
			WHERE id = $1`, res.Sandbox.ID)
		_, err := s.AdmitFork(t.Context(), forkAdmitParams(own, res.Sandbox.ID))
		if !errors.Is(err, db.ErrSnapshotDataExpired) {
			t.Fatalf("fork of an expired source = %v", err)
		}
	})

	t.Run("foreign source", func(t *testing.T) {
		other := owner(t, s)
		res := create(t, s, db.CreateSandboxParams{Owner: other})
		_, err := s.AdmitFork(t.Context(), forkAdmitParams(own, res.Sandbox.ID))
		if !errors.Is(err, db.ErrNotFound) {
			t.Fatalf("foreign source = %v", err)
		}
	})

	t.Run("unknown node", func(t *testing.T) {
		res, _ := forkSourceReady(t, s, own)
		p := forkAdmitParams(own, res.ID)
		p.NodeID = "node-gone"
		if _, err := s.AdmitFork(t.Context(), p); !errors.Is(err, db.ErrNodeGone) {
			t.Fatalf("missing node = %v", err)
		}
	})

	t.Run("full node", func(t *testing.T) {
		res, _ := forkSourceReady(t, s, own)
		p := forkAdmitParams(own, res.ID)
		p.Reserve = db.Resources{VCPUs: 10_000, MemoryMiB: 100, DiskGiB: 10}
		if _, err := s.AdmitFork(t.Context(), p); !errors.Is(err, db.ErrNoCapacity) {
			t.Fatalf("full node = %v", err)
		}
	})

	t.Run("archived source pins its generation", func(t *testing.T) {
		res, gen := forkSourceReady(t, s, own)
		genExec(t, s, `UPDATE sandboxes SET state = 'archived' WHERE id = $1`, res.ID)
		p := forkAdmitParams(own, res.ID)
		p.IdempotencyKey = ""
		p.SourceGenerationID = &gen.ID
		out, err := s.AdmitFork(t.Context(), p)
		if err != nil {
			t.Fatalf("admit archived fork: %v", err)
		}
		if out.Job.SourceGenerationID == nil || *out.Job.SourceGenerationID != gen.ID {
			t.Fatalf("pinned generation = %v, want %s", out.Job.SourceGenerationID, gen.ID)
		}
		// A generation that is not this source's is refused.
		otherRes := create(t, s, db.CreateSandboxParams{Owner: own})
		genExec(t, s, `UPDATE sandboxes SET state = 'ready' WHERE id = $1`, otherRes.Sandbox.ID)
		foreign := genComplete(t, s, own, otherRes.Sandbox.ID, time.Now().UTC())
		p.ID = ids.Sandbox()
		p.SourceGenerationID = &foreign.ID
		if _, err := s.AdmitFork(t.Context(), p); !errors.Is(err, db.ErrSnapshotNotFound) {
			t.Fatalf("foreign generation = %v", err)
		}
		// The generation's disk must fit the reservation.
		p.ID = ids.Sandbox()
		p.SourceGenerationID = &gen.ID
		p.Reserve = db.Resources{VCPUs: 1, MemoryMiB: 1024, DiskGiB: 1}
		if _, err := s.AdmitFork(t.Context(), p); !errors.Is(err, db.ErrSnapshotDiskTooSmall) {
			t.Fatalf("small reservation = %v", err)
		}
	})
}

// TestStartForkOfADeletedFork checks the deleted-fork refusal and that the
// claim is left to finish normally.
func TestStartForkOfADeletedFork(t *testing.T) {
	s := dbtestOpen(t)
	own := owner(t, s)
	node(t, s, "node-a", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})
	source, gen := forkSourceReady(t, s, own)
	result, err := s.AdmitFork(t.Context(), forkAdmitParams(own, source.ID))
	if err != nil {
		t.Fatalf("admit fork: %v", err)
	}
	job := phase1ClaimKind(t, s, "fork-worker", db.JobFork)
	genExec(t, s, `UPDATE sandboxes SET deleted_at = now() WHERE id = $1`, result.Sandbox.ID)
	_, err = s.StartFork(t.Context(), job.ID, "fork-worker", job.LeaseToken, db.StartForkParams{
		ForkID: result.Sandbox.ID, Owner: own, SourceID: source.ID,
		NodeID: "node-a", GenerationID: gen.ID,
	})
	if !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("start fork of a deleted row = %v", err)
	}
	if n := count(t, s, `SELECT count(*) FROM jobs WHERE kind = $1 AND sandbox_id = $2`,
		string(db.JobProvision), result.Sandbox.ID); n != 0 {
		t.Fatalf("provision jobs for a deleted fork = %d, want 0", n)
	}
}

// TestStartForkRejectsAForgedPayload keeps the hand-off anchored to the
// claimed job's payload.
func TestStartForkRejectsAForgedPayload(t *testing.T) {
	s := dbtestOpen(t)
	own := owner(t, s)
	node(t, s, "node-a", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})
	source, gen := forkSourceReady(t, s, own)
	result, err := s.AdmitFork(t.Context(), forkAdmitParams(own, source.ID))
	if err != nil {
		t.Fatalf("admit fork: %v", err)
	}
	job := phase1ClaimKind(t, s, "fork-worker", db.JobFork)
	_, err = s.StartFork(t.Context(), job.ID, "fork-worker", job.LeaseToken, db.StartForkParams{
		ForkID: result.Sandbox.ID, Owner: own, SourceID: ids.Sandbox(),
		NodeID: "node-a", GenerationID: gen.ID,
	})
	if !errors.Is(err, db.ErrInvalidJobPayload) {
		t.Fatalf("forged source = %v, want ErrInvalidJobPayload", err)
	}
}
