// Phase 2 reset and restore entry points. ResetVM is in phase2_reset.go,
// SnapshotVM in phase2_snapshot.go, the CreateVM from_manifest restore in
// phase2_restore.go.

package node

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
	"github.com/nathabonfim59/bamboo-raft/internal/snapformat"
)

// checkFromManifest validates a CreateVM from_manifest request in nodefake's
// order: the two sources are exclusive, the node needs an object store (and
// a QEMU that passed the Phase 2 capability gate), and the key must parse.
// It runs at the CreateVM entry, before loadRestore fetches the manifest and
// before any reservation, disk or record exists, so a refused request leaves
// no VM behind.
func (m *Manager) checkFromManifest(req *nodev1.CreateVMRequest) error {
	if req.GetFromSnapshot() != "" {
		return status.Error(codes.InvalidArgument, "from_snapshot and from_manifest are exclusive")
	}
	if m.cfg.Store == nil {
		return status.Error(codes.FailedPrecondition, "this node has no object store")
	}
	if err := m.phase2Unavailable(); err != nil {
		return err
	}
	if _, _, err := snapformat.ParseManifestKey(req.GetFromManifest()); err != nil {
		return status.Errorf(codes.InvalidArgument, "%v", err)
	}
	return nil
}
