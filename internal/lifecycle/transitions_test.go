package lifecycle

import (
	"testing"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
)

func TestCanTransitionTable(t *testing.T) {
	tests := []struct {
		name string
		from db.SandboxState
		to   db.SandboxState
		want bool
	}{
		{"init to provisioning", db.StateInit, db.StateProvisioning, true},
		{"init to cancelled", db.StateInit, db.StateCancelled, true},
		{"provisioning to ready", db.StateProvisioning, db.StateReady, true},
		{"provisioning to error", db.StateProvisioning, db.StateError, true},
		{"provisioning to cancelled", db.StateProvisioning, db.StateCancelled, true},
		{"provisioning to archived", db.StateProvisioning, db.StateArchived, false},
		{"provisioned to cloning", db.StateProvisioned, db.StateCloning, true},
		{"cloning to ready", db.StateCloning, db.StateReady, true},
		{"ready to running", db.StateReady, db.StateRunning, true},
		{"ready to idle", db.StateReady, db.StateIdle, true},
		{"running to archiving", db.StateRunning, db.StateArchiving, true},
		{"archiving to archived", db.StateArchiving, db.StateArchived, true},
		{"archived to ready", db.StateArchived, db.StateReady, true},
		{"error to cancelled", db.StateError, db.StateCancelled, true},
		{"error to ready", db.StateError, db.StateReady, false},
		{"cancelled to anything", db.StateCancelled, db.StateReady, false},
		{"unknown source", db.SandboxState("bogus"), db.StateReady, false},
		{"ready to provisioning", db.StateReady, db.StateProvisioning, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := CanTransition(tc.from, tc.to); got != tc.want {
				t.Fatalf("CanTransition(%s, %s) = %v, want %v", tc.from, tc.to, got, tc.want)
			}
		})
	}
}
