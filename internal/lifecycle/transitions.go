package lifecycle

import "github.com/nathabonfim59/bamboo-raft/internal/db"

// CanTransition reports whether the Phase 0.5 state machine allows from → to.
// All lifecycle writes consult it; the database additionally enforces
// optimistic versions and expected from-states, so a stale worker cannot
// move a row twice.
//
// The API's full state enum is kept in the database, but this build only drives
// init/provisioning/provisioned/ready/error/cancelled. Later phases extend
// this table without changing wire states.
func CanTransition(from, to db.SandboxState) bool {
	switch from {
	case db.StateInit:
		return to == db.StateProvisioning || to == db.StateCancelled
	case db.StateProvisioning:
		return to == db.StateProvisioned || to == db.StateReady || to == db.StateError || to == db.StateCancelled
	case db.StateProvisioned:
		return to == db.StateCloning || to == db.StateReady || to == db.StateError || to == db.StateCancelled
	case db.StateCloning:
		return to == db.StateReady || to == db.StateError || to == db.StateCancelled
	case db.StateReady, db.StateIdle, db.StateRunning:
		return to == db.StateReady || to == db.StateIdle || to == db.StateRunning ||
			to == db.StateArchiving || to == db.StateError || to == db.StateCancelled
	case db.StateArchiving:
		return to == db.StateArchived || to == db.StateError || to == db.StateCancelled
	case db.StateArchived:
		return to == db.StateReady || to == db.StateError || to == db.StateCancelled
	case db.StateError:
		return to == db.StateCancelled
	case db.StateCancelled:
		return false
	default:
		return false
	}
}
