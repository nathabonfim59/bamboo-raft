package db_test

// Real-Postgres tests for the Phase 1 SSH metadata slice (B4.1). They are
// gated by BBRAFT_TEST_DATABASE_URL through dbtest.Open and never report a
// skip as a pass.

import (
	"errors"
	"strings"
	"testing"

	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
)

const (
	sshEndpointIPv4 = "203.0.113.10:22001"
	sshEndpointIPv6 = "[2001:db8::10]:22001"
	sshEndpointName = "sandbox.example:2222"
	sshHostKeyA     = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAITestHostKeyA user@example"
	sshHostKeyB     = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAITestHostKeyB user@example"
)

// sshNode registers the node every SSH fixture runs on.
func sshNode(t *testing.T, s *db.Store) {
	t.Helper()
	node(t, s, "node-a", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})
}

// sshNodeID returns a sandbox's node id or fails the test.
func sshNodeID(t *testing.T, sbx db.Sandbox) string {
	t.Helper()
	if sbx.NodeID == nil {
		t.Fatalf("sandbox %s has no node", sbx.ID)
	}
	return *sbx.NodeID
}

// sshSet stores a pair for the boot the sandbox row identifies.
func sshSet(t *testing.T, s *db.Store, own uuid.UUID, sbx db.Sandbox, endpoint, hostKey string) db.Sandbox {
	t.Helper()
	out, err := s.SetSandboxSSHAccess(t.Context(), db.SetSSHAccessParams{
		Owner:      own,
		SandboxID:  sbx.ID,
		NodeID:     sshNodeID(t, sbx),
		Generation: sbx.LifecycleGeneration,
		Endpoint:   endpoint,
		HostKey:    hostKey,
	})
	if err != nil {
		t.Fatalf("set ssh access: %v", err)
	}
	return out
}

// sshCurrent reads the owner-visible row for a fixture.
func sshCurrent(t *testing.T, s *db.Store, own uuid.UUID, sandboxID string) db.Sandbox {
	t.Helper()
	row, err := s.Sandbox(t.Context(), own, sandboxID)
	if err != nil {
		t.Fatalf("read sandbox: %v", err)
	}
	return row
}

// sshWant asserts the exact pair is present.
func sshWant(t *testing.T, row db.Sandbox, endpoint, hostKey string) {
	t.Helper()
	if row.SSH == nil {
		t.Fatalf("ssh metadata absent, want %q", endpoint)
	}
	if row.SSH.Endpoint != endpoint || row.SSH.HostKey != hostKey {
		t.Fatalf("ssh metadata = %+v, want %q %q", *row.SSH, endpoint, hostKey)
	}
}

// sshWantAbsent asserts the pair is absent.
func sshWantAbsent(t *testing.T, row db.Sandbox) {
	t.Helper()
	if row.SSH != nil {
		t.Fatalf("ssh metadata = %+v, want absent", *row.SSH)
	}
}

// sshRawSet writes a pair directly, simulating metadata that a stale writer
// left in a state the public method refuses. Finalizers must still clear it.
func sshRawSet(t *testing.T, s *db.Store, sandboxID, endpoint, hostKey string) {
	t.Helper()
	if _, err := s.Pool().Exec(t.Context(), `
		UPDATE sandboxes SET ssh_endpoint = $2, ssh_host_key = $3 WHERE id = $1`,
		sandboxID, endpoint, hostKey); err != nil {
		t.Fatalf("raw ssh metadata: %v", err)
	}
}

func TestSSHAccessPersistReadAndRefresh(t *testing.T) {
	s := dbtestOpen(t)
	own := owner(t, s)
	sshNode(t, s)
	sbx := phase1CreateReady(t, s, own, db.CreateSandboxParams{})
	version := sbx.Version

	stored := sshSet(t, s, own, sbx, sshEndpointIPv4, sshHostKeyA)
	sshWant(t, stored, sshEndpointIPv4, sshHostKeyA)
	if stored.Version != version || stored.LifecycleGeneration != sbx.LifecycleGeneration {
		t.Fatalf("metadata write moved version/generation: %+v", stored)
	}

	read := sshCurrent(t, s, own, sbx.ID)
	sshWant(t, read, sshEndpointIPv4, sshHostKeyA)

	list, _, err := s.ListSandboxes(t.Context(), db.SandboxListQuery{Owner: own, Limit: 10})
	if err != nil {
		t.Fatalf("list sandboxes: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("listed %d sandboxes, want 1", len(list))
	}
	sshWant(t, list[0], sshEndpointIPv4, sshHostKeyA)

	secrets, err := db.NewSecrets([]byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatalf("secrets: %v", err)
	}
	restarted, err := openSameSchema(t, s, secrets)
	if err != nil {
		t.Fatalf("restart store: %v", err)
	}
	t.Cleanup(restarted.Close)
	afterRestart, err := restarted.Sandbox(t.Context(), own, sbx.ID)
	if err != nil {
		t.Fatalf("read after restart: %v", err)
	}
	sshWant(t, afterRestart, sshEndpointIPv4, sshHostKeyA)

	// An idempotent repeat writes nothing: updated_at, version and generation
	// stay exactly as they were.
	repeat := sshSet(t, s, own, sbx, sshEndpointIPv4, sshHostKeyA)
	sshWant(t, repeat, sshEndpointIPv4, sshHostKeyA)
	if !repeat.UpdatedAt.Equal(stored.UpdatedAt) {
		t.Fatalf("idempotent repeat moved updated_at: %v -> %v", stored.UpdatedAt, repeat.UpdatedAt)
	}
	if repeat.Version != stored.Version || repeat.LifecycleGeneration != stored.LifecycleGeneration {
		t.Fatalf("idempotent repeat moved version/generation: %+v", repeat)
	}

	// A changed pair refreshes both values without bumping version or the
	// lifecycle generation.
	changed := sshSet(t, s, own, sbx, sshEndpointIPv6, sshHostKeyB)
	sshWant(t, changed, sshEndpointIPv6, sshHostKeyB)
	if changed.Version != stored.Version || changed.LifecycleGeneration != stored.LifecycleGeneration {
		t.Fatalf("refresh moved version/generation: %+v", changed)
	}
}

func TestSSHAccessValidation(t *testing.T) {
	s := dbtestOpen(t)
	own := owner(t, s)
	sshNode(t, s)
	sbx := phase1CreateReady(t, s, own, db.CreateSandboxParams{})

	valid := []struct {
		name     string
		endpoint string
		hostKey  string
	}{
		{"ipv4", sshEndpointIPv4, sshHostKeyA},
		{"ipv6", sshEndpointIPv6, sshHostKeyA},
		{"hostname", sshEndpointName, sshHostKeyA},
		{"hostkey-comment", sshEndpointIPv4, "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQTest comment with spaces"},
	}
	for _, tc := range valid {
		t.Run("valid_"+tc.name, func(t *testing.T) {
			got := sshSet(t, s, own, sbx, tc.endpoint, tc.hostKey)
			sshWant(t, got, tc.endpoint, tc.hostKey)
		})
	}

	stored := sshSet(t, s, own, sbx, sshEndpointIPv4, sshHostKeyA)
	badEndpoints := []struct {
		name     string
		endpoint string
	}{
		{"empty", ""},
		{"no-port", "203.0.113.10"},
		{"empty-host", ":22"},
		{"port-zero", "203.0.113.10:0"},
		{"port-too-big", "203.0.113.10:65536"},
		{"port-not-numeric", "203.0.113.10:ssh"},
		{"port-plus", "203.0.113.10:+22"},
		{"space", "203.0.113.10 :22"},
		{"newline", "203.0.113.10:22\n"},
		{"unbracketed-ipv6", "2001:db8::10:22"},
	}
	for _, tc := range badEndpoints {
		t.Run("invalid_endpoint_"+tc.name, func(t *testing.T) {
			_, err := s.SetSandboxSSHAccess(t.Context(), db.SetSSHAccessParams{
				Owner: own, SandboxID: sbx.ID, NodeID: sshNodeID(t, sbx),
				Generation: sbx.LifecycleGeneration, Endpoint: tc.endpoint, HostKey: sshHostKeyA,
			})
			if !errors.Is(err, db.ErrInvalidSSHAccess) {
				t.Fatalf("endpoint %q: %v, want ErrInvalidSSHAccess", tc.endpoint, err)
			}
			after := sshCurrent(t, s, own, sbx.ID)
			sshWant(t, after, stored.SSH.Endpoint, stored.SSH.HostKey)
			if after.Version != stored.Version || after.LifecycleGeneration != stored.LifecycleGeneration ||
				!after.UpdatedAt.Equal(stored.UpdatedAt) {
				t.Fatalf("rejected endpoint changed the row: %+v", after)
			}
		})
	}

	badHostKeys := []struct {
		name    string
		hostKey string
	}{
		{"empty", ""},
		{"blank", "   "},
		{"newline", "ssh-ed25519 AAAA user@example\nssh-rsa BBBB user@example"},
		{"tab", "ssh-ed25519\tAAAA user@example"},
		{"nul", "ssh-ed25519 AAAA\x00 user@example"},
	}
	for _, tc := range badHostKeys {
		t.Run("invalid_hostkey_"+tc.name, func(t *testing.T) {
			_, err := s.SetSandboxSSHAccess(t.Context(), db.SetSSHAccessParams{
				Owner: own, SandboxID: sbx.ID, NodeID: sshNodeID(t, sbx),
				Generation: sbx.LifecycleGeneration, Endpoint: sshEndpointIPv4, HostKey: tc.hostKey,
			})
			if !errors.Is(err, db.ErrInvalidSSHAccess) {
				t.Fatalf("host key %q: %v, want ErrInvalidSSHAccess", tc.hostKey, err)
			}
			after := sshCurrent(t, s, own, sbx.ID)
			sshWant(t, after, stored.SSH.Endpoint, stored.SSH.HostKey)
			if after.Version != stored.Version || after.LifecycleGeneration != stored.LifecycleGeneration ||
				!after.UpdatedAt.Equal(stored.UpdatedAt) {
				t.Fatalf("rejected host key changed the row: %+v", after)
			}
		})
	}
}

func TestSSHAccessOwnerDeletedAndBootFences(t *testing.T) {
	s := dbtestOpen(t)
	own := owner(t, s)
	foreign := owner(t, s)
	sshNode(t, s)
	sbx := phase1CreateReady(t, s, own, db.CreateSandboxParams{})

	_, err := s.SetSandboxSSHAccess(t.Context(), db.SetSSHAccessParams{
		Owner: foreign, SandboxID: sbx.ID, NodeID: sshNodeID(t, sbx),
		Generation: sbx.LifecycleGeneration, Endpoint: sshEndpointIPv4, HostKey: sshHostKeyA,
	})
	if !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("foreign owner: %v, want ErrNotFound", err)
	}
	sshWantAbsent(t, sshCurrent(t, s, own, sbx.ID))
	if _, err := s.Sandbox(t.Context(), foreign, sbx.ID); !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("foreign owner read: %v, want ErrNotFound", err)
	}

	_, err = s.SetSandboxSSHAccess(t.Context(), db.SetSSHAccessParams{
		Owner: uuid.Nil(), SandboxID: sbx.ID, NodeID: sshNodeID(t, sbx),
		Generation: sbx.LifecycleGeneration, Endpoint: sshEndpointIPv4, HostKey: sshHostKeyA,
	})
	if !errors.Is(err, db.ErrNoOwner) {
		t.Fatalf("nil owner: %v, want ErrNoOwner", err)
	}
	_, err = s.SetSandboxSSHAccess(t.Context(), db.SetSSHAccessParams{
		Owner: own, SandboxID: "bx_doesnotexist", NodeID: sshNodeID(t, sbx),
		Generation: sbx.LifecycleGeneration, Endpoint: sshEndpointIPv4, HostKey: sshHostKeyA,
	})
	if !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("unknown sandbox: %v, want ErrNotFound", err)
	}

	_, err = s.SetSandboxSSHAccess(t.Context(), db.SetSSHAccessParams{
		Owner: own, SandboxID: sbx.ID, NodeID: "node-other",
		Generation: sbx.LifecycleGeneration, Endpoint: sshEndpointIPv4, HostKey: sshHostKeyA,
	})
	if !errors.Is(err, db.ErrSSHAccessStale) {
		t.Fatalf("wrong node: %v, want ErrSSHAccessStale", err)
	}
	_, err = s.SetSandboxSSHAccess(t.Context(), db.SetSSHAccessParams{
		Owner: own, SandboxID: sbx.ID, NodeID: sshNodeID(t, sbx),
		Generation: sbx.LifecycleGeneration + 1, Endpoint: sshEndpointIPv4, HostKey: sshHostKeyA,
	})
	if !errors.Is(err, db.ErrSSHAccessStale) {
		t.Fatalf("wrong generation: %v, want ErrSSHAccessStale", err)
	}
	fenced := sshCurrent(t, s, own, sbx.ID)
	sshWantAbsent(t, fenced)
	if fenced.Version != sbx.Version || fenced.LifecycleGeneration != sbx.LifecycleGeneration ||
		!fenced.UpdatedAt.Equal(sbx.UpdatedAt) {
		t.Fatalf("stale write changed the row: %+v", fenced)
	}

	provisioning := create(t, s, db.CreateSandboxParams{Owner: own, NodeID: "node-a"})
	_, err = s.SetSandboxSSHAccess(t.Context(), db.SetSSHAccessParams{
		Owner: own, SandboxID: provisioning.Sandbox.ID, NodeID: "node-a",
		Generation: provisioning.Sandbox.LifecycleGeneration,
		Endpoint:   sshEndpointIPv4, HostKey: sshHostKeyA,
	})
	if !errors.Is(err, db.ErrSSHAccessStale) {
		t.Fatalf("provisioning row: %v, want ErrSSHAccessStale", err)
	}
	sshWantAbsent(t, sshCurrent(t, s, own, provisioning.Sandbox.ID))

	sshSet(t, s, own, sbx, sshEndpointIPv4, sshHostKeyA)
	_, archiving, _, created, err := s.AcceptSandboxStop(t.Context(), own, sbx.ID, false)
	if err != nil || !created {
		t.Fatalf("accept stop: created=%v err=%v", created, err)
	}
	if archiving.SSH != nil {
		t.Fatalf("stop acceptance left ssh metadata: %+v", *archiving.SSH)
	}
	_, err = s.SetSandboxSSHAccess(t.Context(), db.SetSSHAccessParams{
		Owner: own, SandboxID: sbx.ID, NodeID: sshNodeID(t, sbx),
		Generation: sbx.LifecycleGeneration, Endpoint: sshEndpointIPv6, HostKey: sshHostKeyB,
	})
	if !errors.Is(err, db.ErrSSHAccessStale) {
		t.Fatalf("archiving row: %v, want ErrSSHAccessStale", err)
	}
	stillArchiving := sshCurrent(t, s, own, sbx.ID)
	sshWantAbsent(t, stillArchiving)
	if stillArchiving.Version != archiving.Version || !stillArchiving.UpdatedAt.Equal(archiving.UpdatedAt) {
		t.Fatalf("stale write changed archiving row: %+v", stillArchiving)
	}

	errored := phase1CreateReady(t, s, own, db.CreateSandboxParams{})
	sshSet(t, s, own, errored, sshEndpointIPv4, sshHostKeyA)
	errored, err = s.TransitionSandbox(t.Context(), db.TransitionParams{
		SandboxID: errored.ID, Owner: own,
		FromStates: []db.SandboxState{db.StateReady}, ExpectedVersion: errored.Version,
		To: db.StateError, ErrorReason: "vm_failed",
	})
	if err != nil {
		t.Fatalf("transition to error: %v", err)
	}
	sshWantAbsent(t, errored)
	_, err = s.SetSandboxSSHAccess(t.Context(), db.SetSSHAccessParams{
		Owner: own, SandboxID: errored.ID, NodeID: sshNodeID(t, errored),
		Generation: errored.LifecycleGeneration, Endpoint: sshEndpointIPv6, HostKey: sshHostKeyB,
	})
	if !errors.Is(err, db.ErrSSHAccessStale) {
		t.Fatalf("error row: %v, want ErrSSHAccessStale", err)
	}
	sshWantAbsent(t, sshCurrent(t, s, own, errored.ID))

	deleted := phase1CreateReady(t, s, own, db.CreateSandboxParams{})
	sshSet(t, s, own, deleted, sshEndpointIPv4, sshHostKeyA)
	if _, err := s.Pool().Exec(t.Context(),
		`UPDATE sandboxes SET deleted_at = now() WHERE id = $1`, deleted.ID); err != nil {
		t.Fatalf("soft delete: %v", err)
	}
	_, err = s.SetSandboxSSHAccess(t.Context(), db.SetSSHAccessParams{
		Owner: own, SandboxID: deleted.ID, NodeID: sshNodeID(t, deleted),
		Generation: deleted.LifecycleGeneration, Endpoint: sshEndpointIPv6, HostKey: sshHostKeyB,
	})
	if !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("deleted sandbox: %v, want ErrNotFound", err)
	}
	if _, err := s.Sandbox(t.Context(), own, deleted.ID); !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("deleted sandbox read: %v, want ErrNotFound", err)
	}
}

func TestSSHAccessPATCHSameBoot(t *testing.T) {
	s := dbtestOpen(t)
	own := owner(t, s)
	sshNode(t, s)
	sbx := phase1CreateReady(t, s, own, db.CreateSandboxParams{})
	stored := sshSet(t, s, own, sbx, sshEndpointIPv4, sshHostKeyA)

	name := "renamed while sshkey ran"
	patched, err := s.PatchSandbox(t.Context(), db.PatchSandboxParams{
		Owner: own, SandboxID: sbx.ID, Name: &name,
	})
	if err != nil {
		t.Fatalf("patch: %v", err)
	}
	if patched.Version != stored.Version+1 {
		t.Fatalf("patch version = %d, want %d", patched.Version, stored.Version+1)
	}
	sshWant(t, patched, sshEndpointIPv4, sshHostKeyA)

	// The metadata write uses the node and generation read before the RPC,
	// not the row version, so the PATCH cannot invalidate the same boot.
	refreshed := sshSet(t, s, own, sbx, sshEndpointIPv6, sshHostKeyB)
	sshWant(t, refreshed, sshEndpointIPv6, sshHostKeyB)
	if refreshed.Version != patched.Version {
		t.Fatalf("metadata after patch bumped version: %d, want %d", refreshed.Version, patched.Version)
	}
	if refreshed.LifecycleGeneration != sbx.LifecycleGeneration {
		t.Fatalf("metadata after patch moved generation: %d", refreshed.LifecycleGeneration)
	}
}

func TestSSHAccessStopAndResumeLifetime(t *testing.T) {
	s := dbtestOpen(t)
	own := owner(t, s)
	sshNode(t, s)
	sbx := phase1CreateReady(t, s, own, db.CreateSandboxParams{})
	sshSet(t, s, own, sbx, sshEndpointIPv4, sshHostKeyA)

	op, archiving, _, created, err := s.AcceptSandboxStop(t.Context(), own, sbx.ID, false)
	if err != nil || !created {
		t.Fatalf("accept stop: created=%v err=%v", created, err)
	}
	sshWantAbsent(t, archiving)
	_, err = s.SetSandboxSSHAccess(t.Context(), db.SetSSHAccessParams{
		Owner: own, SandboxID: sbx.ID, NodeID: sshNodeID(t, sbx),
		Generation: sbx.LifecycleGeneration, Endpoint: sshEndpointIPv6, HostKey: sshHostKeyB,
	})
	if !errors.Is(err, db.ErrSSHAccessStale) {
		t.Fatalf("persist during stop: %v, want ErrSSHAccessStale", err)
	}

	stopJob := phase1ClaimKind(t, s, "ssh-stop", db.JobStop)
	stopped, _, err := s.FinalizeSandboxStop(t.Context(), stopJob.ID, "ssh-stop", stopJob.LeaseToken, db.FinalizeStopParams{
		Owner: own, SandboxID: sbx.ID, StopID: op.ID, ExpectedVersion: archiving.Version,
	})
	if err != nil {
		t.Fatalf("finalize stop: %v", err)
	}
	sshWantAbsent(t, stopped)
	if err := s.CompleteJob(t.Context(), stopJob.ID, "ssh-stop", stopJob.LeaseToken); err != nil {
		t.Fatalf("complete stop job: %v", err)
	}

	_, err = s.SetSandboxSSHAccess(t.Context(), db.SetSSHAccessParams{
		Owner: own, SandboxID: sbx.ID, NodeID: sshNodeID(t, sbx),
		Generation: sbx.LifecycleGeneration, Endpoint: sshEndpointIPv6, HostKey: sshHostKeyB,
	})
	if !errors.Is(err, db.ErrSSHAccessStale) {
		t.Fatalf("persist while archived: %v, want ErrSSHAccessStale", err)
	}

	// Seed a pair directly on the archived row: resume acceptance itself must
	// clear it, not rely on the earlier stop having done so.
	sshRawSet(t, s, sbx.ID, sshEndpointIPv4, sshHostKeyA)
	resumed, err := s.AcceptSandboxResume(t.Context(), db.ResumeParams{Owner: own, SandboxID: sbx.ID})
	if err != nil {
		t.Fatalf("accept resume: %v", err)
	}
	sshWantAbsent(t, resumed.Sandbox)
	if resumed.Sandbox.LifecycleGeneration != sbx.LifecycleGeneration+1 {
		t.Fatalf("resume generation = %d, want %d", resumed.Sandbox.LifecycleGeneration, sbx.LifecycleGeneration+1)
	}
	_, err = s.SetSandboxSSHAccess(t.Context(), db.SetSSHAccessParams{
		Owner: own, SandboxID: sbx.ID, NodeID: sshNodeID(t, sbx),
		Generation: resumed.Sandbox.LifecycleGeneration, Endpoint: sshEndpointIPv6, HostKey: sshHostKeyB,
	})
	if !errors.Is(err, db.ErrSSHAccessStale) {
		t.Fatalf("persist while provisioning: %v, want ErrSSHAccessStale", err)
	}

	resumeJob := phase1ClaimKind(t, s, "ssh-resume", db.JobResume)
	ready, err := s.FinalizeSandboxResume(t.Context(), resumeJob.ID, "ssh-resume", resumeJob.LeaseToken, db.FinalizeResumeParams{
		Owner: own, SandboxID: sbx.ID,
		Generation: resumed.Sandbox.LifecycleGeneration, Version: resumed.Sandbox.Version,
	})
	if err != nil {
		t.Fatalf("finalize resume: %v", err)
	}
	sshWantAbsent(t, ready)
	if err := s.CompleteJob(t.Context(), resumeJob.ID, "ssh-resume", resumeJob.LeaseToken); err != nil {
		t.Fatalf("complete resume job: %v", err)
	}

	fresh := sshSet(t, s, own, ready, sshEndpointIPv6, sshHostKeyB)
	sshWant(t, fresh, sshEndpointIPv6, sshHostKeyB)

	// A late response from the previous boot cannot restore its pair even
	// though the row is ready again.
	_, err = s.SetSandboxSSHAccess(t.Context(), db.SetSSHAccessParams{
		Owner: own, SandboxID: sbx.ID, NodeID: sshNodeID(t, sbx),
		Generation: sbx.LifecycleGeneration, Endpoint: sshEndpointIPv4, HostKey: sshHostKeyA,
	})
	if !errors.Is(err, db.ErrSSHAccessStale) {
		t.Fatalf("late old-boot response: %v, want ErrSSHAccessStale", err)
	}
	after := sshCurrent(t, s, own, sbx.ID)
	sshWant(t, after, sshEndpointIPv6, sshHostKeyB)
	if after.Version != fresh.Version || after.LifecycleGeneration != fresh.LifecycleGeneration {
		t.Fatalf("late response moved version/generation: %+v", after)
	}
}

func TestSSHAccessResumeAcceptanceClears(t *testing.T) {
	t.Run("archived", func(t *testing.T) {
		s := dbtestOpen(t)
		own := owner(t, s)
		sshNode(t, s)
		sbx := phase1CreateReady(t, s, own, db.CreateSandboxParams{})
		archived, _ := phase1Stopped(t, s, own, sbx)
		sshRawSet(t, s, sbx.ID, sshEndpointIPv4, sshHostKeyA)
		resumed, err := s.AcceptSandboxResume(t.Context(), db.ResumeParams{
			Owner: own, SandboxID: archived.ID,
		})
		if err != nil {
			t.Fatalf("accept resume from archived: %v", err)
		}
		sshWantAbsent(t, resumed.Sandbox)
	})

	t.Run("archiving superseded", func(t *testing.T) {
		s := dbtestOpen(t)
		own := owner(t, s)
		sshNode(t, s)
		sbx := phase1CreateReady(t, s, own, db.CreateSandboxParams{})
		phase1BootDone(t, s, "ssh-boot")
		op, _, _, created, err := s.AcceptSandboxStop(t.Context(), own, sbx.ID, false)
		if err != nil || !created {
			t.Fatalf("accept stop: created=%v err=%v", created, err)
		}
		sshRawSet(t, s, sbx.ID, sshEndpointIPv4, sshHostKeyA)
		resumed, err := s.AcceptSandboxResume(t.Context(), db.ResumeParams{
			Owner: own, SandboxID: sbx.ID, SupersedeStopID: op.ID,
		})
		if err != nil {
			t.Fatalf("accept superseding resume: %v", err)
		}
		sshWantAbsent(t, resumed.Sandbox)
		if resumed.Superseded == nil || resumed.Superseded.ID != op.ID {
			t.Fatalf("resume did not supersede stop: %+v", resumed.Superseded)
		}
	})
}

func TestSSHAccessTransitionSandboxJobClearsAndPreserves(t *testing.T) {
	t.Run("ready-ish transition preserves pair", func(t *testing.T) {
		s := dbtestOpen(t)
		own := owner(t, s)
		sshNode(t, s)
		sbx := phase1CreateReady(t, s, own, db.CreateSandboxParams{})
		sshSet(t, s, own, sbx, sshEndpointIPv4, sshHostKeyA)
		job := phase1ClaimKind(t, s, "ssh-job", db.JobProvision)
		out, err := s.TransitionSandboxJob(t.Context(), job.ID, "ssh-job", job.LeaseToken, db.TransitionParams{
			SandboxID: sbx.ID, Owner: own,
			FromStates: []db.SandboxState{db.StateReady}, ExpectedVersion: sbx.Version,
			To: db.StateIdle,
		})
		if err != nil {
			t.Fatalf("transition ready to idle: %v", err)
		}
		sshWant(t, out, sshEndpointIPv4, sshHostKeyA)
	})

	t.Run("non-ready transition clears pair", func(t *testing.T) {
		s := dbtestOpen(t)
		own := owner(t, s)
		sshNode(t, s)
		sbx := phase1CreateReady(t, s, own, db.CreateSandboxParams{})
		sshSet(t, s, own, sbx, sshEndpointIPv4, sshHostKeyA)
		job := phase1ClaimKind(t, s, "ssh-job", db.JobProvision)
		out, err := s.TransitionSandboxJob(t.Context(), job.ID, "ssh-job", job.LeaseToken, db.TransitionParams{
			SandboxID: sbx.ID, Owner: own,
			FromStates: []db.SandboxState{db.StateReady}, ExpectedVersion: sbx.Version,
			To: db.StateError, ErrorReason: "vm_failed",
		})
		if err != nil {
			t.Fatalf("transition ready to error: %v", err)
		}
		sshWantAbsent(t, out)
	})
}

func TestSSHAccessFencedFinalizersClear(t *testing.T) {
	t.Run("normal stop finalize", func(t *testing.T) {
		s := dbtestOpen(t)
		own := owner(t, s)
		sshNode(t, s)
		sbx := phase1CreateReady(t, s, own, db.CreateSandboxParams{})
		sshSet(t, s, own, sbx, sshEndpointIPv4, sshHostKeyA)
		op, archiving, _, _, err := s.AcceptSandboxStop(t.Context(), own, sbx.ID, false)
		if err != nil {
			t.Fatal(err)
		}
		sshRawSet(t, s, sbx.ID, sshEndpointIPv6, sshHostKeyB)
		job := phase1ClaimKind(t, s, "ssh-stop", db.JobStop)
		stopped, _, err := s.FinalizeSandboxStop(t.Context(), job.ID, "ssh-stop", job.LeaseToken, db.FinalizeStopParams{
			Owner: own, SandboxID: sbx.ID, StopID: op.ID, ExpectedVersion: archiving.Version,
		})
		if err != nil {
			t.Fatalf("finalize stop: %v", err)
		}
		sshWantAbsent(t, stopped)
	})

	t.Run("missing disk stop finalize", func(t *testing.T) {
		s := dbtestOpen(t)
		own := owner(t, s)
		sshNode(t, s)
		sbx := phase1CreateReady(t, s, own, db.CreateSandboxParams{})
		sshSet(t, s, own, sbx, sshEndpointIPv4, sshHostKeyA)
		op, archiving, _, _, err := s.AcceptSandboxStop(t.Context(), own, sbx.ID, false)
		if err != nil {
			t.Fatal(err)
		}
		sshRawSet(t, s, sbx.ID, sshEndpointIPv6, sshHostKeyB)
		job := phase1ClaimKind(t, s, "ssh-stop", db.JobStop)
		failed, _, _, err := s.FinalizeSandboxStopMissingDisk(t.Context(), job.ID, "ssh-stop", job.LeaseToken, db.FinalizeStopParams{
			Owner: own, SandboxID: sbx.ID, StopID: op.ID, ExpectedVersion: archiving.Version,
		})
		if err != nil {
			t.Fatalf("finalize missing-disk stop: %v", err)
		}
		sshWantAbsent(t, failed)
	})

	t.Run("resume transition to ready", func(t *testing.T) {
		s := dbtestOpen(t)
		own := owner(t, s)
		sshNode(t, s)
		sbx := phase1CreateReady(t, s, own, db.CreateSandboxParams{})
		sshSet(t, s, own, sbx, sshEndpointIPv4, sshHostKeyA)
		archived, _ := phase1Stopped(t, s, own, sbx)
		resumed, err := s.AcceptSandboxResume(t.Context(), db.ResumeParams{Owner: own, SandboxID: archived.ID})
		if err != nil {
			t.Fatal(err)
		}
		sshRawSet(t, s, sbx.ID, sshEndpointIPv6, sshHostKeyB)
		job := phase1ClaimKind(t, s, "ssh-resume", db.JobResume)
		ready, err := s.FinalizeSandboxResume(t.Context(), job.ID, "ssh-resume", job.LeaseToken, db.FinalizeResumeParams{
			Owner: own, SandboxID: sbx.ID,
			Generation: resumed.Sandbox.LifecycleGeneration, Version: resumed.Sandbox.Version,
		})
		if err != nil {
			t.Fatalf("finalize resume: %v", err)
		}
		sshWantAbsent(t, ready)
	})

	t.Run("resume failure", func(t *testing.T) {
		s := dbtestOpen(t)
		own := owner(t, s)
		sshNode(t, s)
		sbx := phase1CreateReady(t, s, own, db.CreateSandboxParams{})
		sshSet(t, s, own, sbx, sshEndpointIPv4, sshHostKeyA)
		archived, _ := phase1Stopped(t, s, own, sbx)
		resumed, err := s.AcceptSandboxResume(t.Context(), db.ResumeParams{Owner: own, SandboxID: archived.ID})
		if err != nil {
			t.Fatal(err)
		}
		sshRawSet(t, s, sbx.ID, sshEndpointIPv6, sshHostKeyB)
		job := phase1ClaimKind(t, s, "ssh-resume", db.JobResume)
		failed, err := s.FinalizeSandboxResumeFailure(t.Context(), job.ID, "ssh-resume", job.LeaseToken, db.FinalizeResumeFailureParams{
			Owner: own, SandboxID: sbx.ID,
			Generation: resumed.Sandbox.LifecycleGeneration, Version: resumed.Sandbox.Version,
		}, "boot_failed")
		if err != nil {
			t.Fatalf("finalize resume failure: %v", err)
		}
		sshWantAbsent(t, failed)
	})

	t.Run("resume missing disk", func(t *testing.T) {
		s := dbtestOpen(t)
		own := owner(t, s)
		sshNode(t, s)
		sbx := phase1CreateReady(t, s, own, db.CreateSandboxParams{})
		sshSet(t, s, own, sbx, sshEndpointIPv4, sshHostKeyA)
		archived, _ := phase1Stopped(t, s, own, sbx)
		resumed, err := s.AcceptSandboxResume(t.Context(), db.ResumeParams{Owner: own, SandboxID: archived.ID})
		if err != nil {
			t.Fatal(err)
		}
		sshRawSet(t, s, sbx.ID, sshEndpointIPv6, sshHostKeyB)
		job := phase1ClaimKind(t, s, "ssh-resume", db.JobResume)
		failed, err := s.FinalizeSandboxResumeMissingDisk(t.Context(), job.ID, "ssh-resume", job.LeaseToken, db.FinalizeResumeFailureParams{
			Owner: own, SandboxID: sbx.ID,
			Generation: resumed.Sandbox.LifecycleGeneration, Version: resumed.Sandbox.Version,
		})
		if err != nil {
			t.Fatalf("finalize resume missing disk: %v", err)
		}
		sshWantAbsent(t, failed)
	})

	t.Run("generic transition to error", func(t *testing.T) {
		s := dbtestOpen(t)
		own := owner(t, s)
		sshNode(t, s)
		sbx := phase1CreateReady(t, s, own, db.CreateSandboxParams{})
		sshSet(t, s, own, sbx, sshEndpointIPv4, sshHostKeyA)
		failed, err := s.TransitionSandbox(t.Context(), db.TransitionParams{
			SandboxID: sbx.ID, Owner: own,
			FromStates: []db.SandboxState{db.StateReady}, ExpectedVersion: sbx.Version,
			To: db.StateError, ErrorReason: "vm_failed",
		})
		if err != nil {
			t.Fatalf("transition to error: %v", err)
		}
		sshWantAbsent(t, failed)
	})
}

func TestSSHAccessStaleCompletionKeepsFreshPair(t *testing.T) {
	s := dbtestOpen(t)
	own := owner(t, s)
	sshNode(t, s)
	sbx := phase1CreateReady(t, s, own, db.CreateSandboxParams{})

	// First resume: keep its job running so it can later act as an old claim.
	first, _ := phase1Stopped(t, s, own, sbx)
	resumed1, err := s.AcceptSandboxResume(t.Context(), db.ResumeParams{Owner: own, SandboxID: first.ID})
	if err != nil {
		t.Fatalf("first resume: %v", err)
	}
	oldResume := phase1ClaimKind(t, s, "old-resume", db.JobResume)
	if _, err := s.FinalizeSandboxResume(t.Context(), oldResume.ID, "old-resume", oldResume.LeaseToken, db.FinalizeResumeParams{
		Owner: own, SandboxID: sbx.ID,
		Generation: resumed1.Sandbox.LifecycleGeneration, Version: resumed1.Sandbox.Version,
	}); err != nil {
		t.Fatalf("first finalize: %v", err)
	}

	// Second resume: keep its stop job running as the old stop claim.
	op2, archiving2, _, _, err := s.AcceptSandboxStop(t.Context(), own, sbx.ID, false)
	if err != nil {
		t.Fatalf("second stop: %v", err)
	}
	oldStop := phase1ClaimKind(t, s, "old-stop", db.JobStop)
	if _, _, err := s.FinalizeSandboxStop(t.Context(), oldStop.ID, "old-stop", oldStop.LeaseToken, db.FinalizeStopParams{
		Owner: own, SandboxID: sbx.ID, StopID: op2.ID, ExpectedVersion: archiving2.Version,
	}); err != nil {
		t.Fatalf("second finalize stop: %v", err)
	}
	resumed2, err := s.AcceptSandboxResume(t.Context(), db.ResumeParams{Owner: own, SandboxID: sbx.ID})
	if err != nil {
		t.Fatalf("second resume: %v", err)
	}
	resume2 := phase1ClaimKind(t, s, "new-resume", db.JobResume)
	ready2, err := s.FinalizeSandboxResume(t.Context(), resume2.ID, "new-resume", resume2.LeaseToken, db.FinalizeResumeParams{
		Owner: own, SandboxID: sbx.ID,
		Generation: resumed2.Sandbox.LifecycleGeneration, Version: resumed2.Sandbox.Version,
	})
	if err != nil {
		t.Fatalf("second finalize resume: %v", err)
	}
	fresh := sshSet(t, s, own, ready2, sshEndpointIPv6, sshHostKeyB)

	// A replay of the CURRENT resume claim after the transition already
	// reached ready must conflict and leave the fresh pair untouched.
	_, err = s.FinalizeSandboxResume(t.Context(), resume2.ID, "new-resume", resume2.LeaseToken, db.FinalizeResumeParams{
		Owner: own, SandboxID: sbx.ID,
		Generation: resumed2.Sandbox.LifecycleGeneration, Version: resumed2.Sandbox.Version,
	})
	if !errors.Is(err, db.ErrConflict) {
		t.Fatalf("current resume replay: %v, want ErrConflict", err)
	}
	afterCurrent := sshCurrent(t, s, own, sbx.ID)
	sshWant(t, afterCurrent, sshEndpointIPv6, sshHostKeyB)
	if afterCurrent.Version != fresh.Version || afterCurrent.LifecycleGeneration != fresh.LifecycleGeneration {
		t.Fatalf("current replay moved version/generation: %+v", afterCurrent)
	}

	// Both old claims are still live, but their generation/state no longer
	// owns the row. Neither may wipe the fresh pair.
	_, err = s.FinalizeSandboxResume(t.Context(), oldResume.ID, "old-resume", oldResume.LeaseToken, db.FinalizeResumeParams{
		Owner: own, SandboxID: sbx.ID,
		Generation: resumed1.Sandbox.LifecycleGeneration, Version: resumed1.Sandbox.Version,
	})
	if !errors.Is(err, db.ErrConflict) {
		t.Fatalf("old resume completion: %v, want ErrConflict", err)
	}
	_, _, err = s.FinalizeSandboxStop(t.Context(), oldStop.ID, "old-stop", oldStop.LeaseToken, db.FinalizeStopParams{
		Owner: own, SandboxID: sbx.ID, StopID: op2.ID, ExpectedVersion: archiving2.Version,
	})
	if !errors.Is(err, db.ErrConflict) {
		t.Fatalf("old stop completion: %v, want ErrConflict", err)
	}

	after := sshCurrent(t, s, own, sbx.ID)
	sshWant(t, after, sshEndpointIPv6, sshHostKeyB)
	if after.Version != fresh.Version || after.LifecycleGeneration != fresh.LifecycleGeneration {
		t.Fatalf("stale completions moved version/generation: %+v", after)
	}
}

func TestSSHMigrationEmptyAndNullable(t *testing.T) {
	s := dbtestOpen(t)
	provider := phase1Provider(t, s)
	if _, err := provider.DownTo(t.Context(), 5); err != nil {
		t.Fatalf("empty rollback: %v", err)
	}
	var present *string
	if err := s.Pool().QueryRow(t.Context(),
		`SELECT to_regclass('sandboxes')::text`).Scan(&present); err != nil {
		t.Fatalf("sandboxes after rollback: %v", err)
	}
	if present == nil {
		t.Fatal("sandboxes table disappeared")
	}
	if n := count(t, s, `SELECT count(*) FROM information_schema.columns
		WHERE table_schema = current_schema() AND table_name = 'sandboxes'
		  AND column_name IN ('ssh_endpoint', 'ssh_host_key')`); n != 0 {
		t.Fatalf("ssh columns after rollback: %d", n)
	}
	if _, err := provider.Up(t.Context()); err != nil {
		t.Fatalf("re-apply: %v", err)
	}
	if got := phase1SchemaVersion(t, s); got != 6 {
		t.Fatalf("schema version after re-apply: %d, want 6", got)
	}
	if n := count(t, s, `SELECT count(*) FROM information_schema.columns
		WHERE table_schema = current_schema() AND table_name = 'sandboxes'
		  AND column_name IN ('ssh_endpoint', 'ssh_host_key') AND is_nullable = 'YES'`); n != 2 {
		t.Fatalf("nullable ssh columns after re-apply: %d, want 2", n)
	}
}

func TestSSHMigrationPopulatedPolicies(t *testing.T) {
	t.Run("ordinary populated rows roll back to two", func(t *testing.T) {
		s := dbtestOpen(t)
		own := owner(t, s)
		sshNode(t, s)
		sbx := phase1CreateReady(t, s, own, db.CreateSandboxParams{})
		provider := phase1Provider(t, s)
		if _, err := provider.DownTo(t.Context(), 2); err != nil {
			t.Fatalf("ordinary populated rollback: %v", err)
		}
		if _, err := provider.Up(t.Context()); err != nil {
			t.Fatalf("re-apply after ordinary rollback: %v", err)
		}
		read := sshCurrent(t, s, own, sbx.ID)
		sshWantAbsent(t, read)
	})

	t.Run("live metadata refuses and deleted metadata rolls back", func(t *testing.T) {
		s := dbtestOpen(t)
		own := owner(t, s)
		sshNode(t, s)
		sbx := phase1CreateReady(t, s, own, db.CreateSandboxParams{})
		stored := sshSet(t, s, own, sbx, sshEndpointIPv4, sshHostKeyA)
		provider := phase1Provider(t, s)
		_, err := provider.DownTo(t.Context(), 5)
		if err == nil {
			t.Fatal("rollback with live ssh metadata was allowed")
		}
		if !strings.Contains(err.Error(), "cannot roll back phase 1 ssh metadata: live sandbox ssh metadata exists") {
			t.Fatalf("rollback error = %v", err)
		}
		if got := phase1SchemaVersion(t, s); got != 6 {
			t.Fatalf("schema version after refused rollback: %d, want 6", got)
		}
		after := sshCurrent(t, s, own, sbx.ID)
		sshWant(t, after, sshEndpointIPv4, sshHostKeyA)
		if after.Version != stored.Version {
			t.Fatalf("refused rollback moved version: %d", after.Version)
		}

		// Metadata on a soft-deleted row is not owner-visible or resumable
		// and does not block dropping the columns.
		if _, err := s.Pool().Exec(t.Context(),
			`UPDATE sandboxes SET deleted_at = now() WHERE id = $1`, sbx.ID); err != nil {
			t.Fatalf("soft delete: %v", err)
		}
		if _, err := provider.DownTo(t.Context(), 5); err != nil {
			t.Fatalf("deleted-row metadata rollback: %v", err)
		}
		if n := count(t, s, `SELECT count(*) FROM information_schema.columns
			WHERE table_schema = current_schema() AND table_name = 'sandboxes'
			  AND column_name IN ('ssh_endpoint', 'ssh_host_key')`); n != 0 {
			t.Fatalf("ssh columns after deleted-row rollback: %d", n)
		}
		if _, err := provider.Up(t.Context()); err != nil {
			t.Fatalf("re-apply after deleted-row rollback: %v", err)
		}
	})
}

func TestSSHMigrationPairConstraint(t *testing.T) {
	s := dbtestOpen(t)
	own := owner(t, s)
	sshNode(t, s)
	sbx := phase1CreateReady(t, s, own, db.CreateSandboxParams{})

	if _, err := s.Pool().Exec(t.Context(), `
		UPDATE sandboxes SET ssh_host_key = $2 WHERE id = $1`,
		sbx.ID, sshHostKeyA); err == nil {
		t.Fatal("host-key-only metadata was accepted")
	}
	if _, err := s.Pool().Exec(t.Context(), `
		UPDATE sandboxes SET ssh_endpoint = $2 WHERE id = $1`,
		sbx.ID, sshEndpointIPv4); err == nil {
		t.Fatal("endpoint-only metadata was accepted")
	}
	if _, err := s.Pool().Exec(t.Context(), `
		UPDATE sandboxes SET ssh_endpoint = '', ssh_host_key = $2 WHERE id = $1`,
		sbx.ID, sshHostKeyA); err == nil {
		t.Fatal("blank endpoint was accepted")
	}
	if _, err := s.Pool().Exec(t.Context(), `
		UPDATE sandboxes SET ssh_endpoint = $2, ssh_host_key = '' WHERE id = $1`,
		sbx.ID, sshEndpointIPv4); err == nil {
		t.Fatal("blank host key was accepted")
	}
	if _, err := s.Pool().Exec(t.Context(), `
		UPDATE sandboxes SET ssh_endpoint = $2, ssh_host_key = $3 WHERE id = $1`,
		sbx.ID, "203.0.113.10 :22", sshHostKeyA); err == nil {
		t.Fatal("endpoint whitespace was accepted")
	}
	if _, err := s.Pool().Exec(t.Context(), `
		UPDATE sandboxes SET ssh_endpoint = $2, ssh_host_key = $3 WHERE id = $1`,
		sbx.ID, sshEndpointIPv4, "ssh-ed25519 AAAA\nssh-rsa BBBB"); err == nil {
		t.Fatal("multi-line host key was accepted")
	}
	sshWantAbsent(t, sshCurrent(t, s, own, sbx.ID))
}
