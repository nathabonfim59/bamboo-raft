package api

// Create-from resolution policy for saved snapshots. The server's admission
// adapter calls it after the keyed replay check and forwards the returned
// Source/MachineType/Resources into the scheduler. It never invents a machine
// type or a disk floor.

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/apierr"
	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/machinetype"
)

// SnapshotRestoreResolver resolves an owner-scoped saved-snapshot name to one
// immutable generation. *db.Store implements it.
type SnapshotRestoreResolver interface {
	SnapshotRestoreSource(ctx context.Context, owner uuid.UUID, name string) (db.SnapshotSource, error)
}

// SnapshotShareResolver resolves a share code to the sharer's current
// generation. *db.Store implements it; a resolver that does not leaves share
// codes not found.
type SnapshotShareResolver interface {
	SnapshotShareSource(ctx context.Context, code string) (db.SnapshotSource, error)
}

// ShareCodePrefix is the public prefix of a saved-snapshot share code. A
// create `from` starting with it is a share, never a name.
const ShareCodePrefix = "bsh_"

// RestoreTarget is the accepted create-from policy: the immutable source plus
// the machine shape the request asked for.
type RestoreTarget struct {
	Source      db.SnapshotSource
	MachineType string
	Resources   db.Resources
}

// ResolveSnapshotRestore resolves a create-from name. typeExplicit is true iff
// the raw create carried a type member (including an explicit empty string,
// which is invalid). Omitted type inherits the snapshot's type. Explicit type
// contributes CPU/RAM and disk, but the disk is grow-only: a smaller disk than
// the snapshot's canonical floor is 400 invalid_json. Missing/foreign/not-ready
// names and unknown legacy disks are the same 404 snapshot_not_found.
func ResolveSnapshotRestore(ctx context.Context, resolver SnapshotRestoreResolver,
	owner uuid.UUID, name string, typeExplicit bool, requestedType string,
) (RestoreTarget, *apierr.Error) {
	if resolver == nil || owner == uuid.Nil() || name == "" {
		return RestoreTarget{}, snapshotNotFound()
	}
	share := strings.HasPrefix(name, ShareCodePrefix)
	var (
		source db.SnapshotSource
		err    error
	)
	if share {
		shares, ok := resolver.(SnapshotShareResolver)
		if !ok {
			return RestoreTarget{}, snapshotNotFound()
		}
		source, err = shares.SnapshotShareSource(ctx, name)
	} else {
		source, err = resolver.SnapshotRestoreSource(ctx, owner, name)
	}
	switch {
	case errors.Is(err, db.ErrSnapshotNotFound), errors.Is(err, db.ErrSnapshotDiskUnknown):
		return RestoreTarget{}, snapshotNotFound()
	case err != nil:
		return RestoreTarget{}, coreInternal()
	}
	if source.DiskGiB == 0 || source.OwnerID == uuid.Nil() || source.Name == "" ||
		!db.ValidSnapshotArtifactID(source.ArtifactID) {
		return RestoreTarget{}, snapshotNotFound()
	}
	if share {
		if source.ShareID == "" || !db.ValidSnapshotShareID(source.ShareID) {
			return RestoreTarget{}, snapshotNotFound()
		}
	} else if source.OwnerID != owner || source.Name != name {
		return RestoreTarget{}, snapshotNotFound()
	}
	if source.GenerationBacked() {
		if source.ManifestKey == "" || source.Image == "" || source.DiskBytes == 0 {
			return RestoreTarget{}, snapshotNotFound()
		}
	} else if source.NodeID == "" {
		return RestoreTarget{}, snapshotNotFound()
	}

	machineName := source.MachineType
	if typeExplicit {
		machineName = requestedType
		if machineName == "" {
			return RestoreTarget{}, coreInvalidJSON("type must be a supported machine type")
		}
	} else if machineName == "" {
		// A legacy generation without a canonical type cannot inherit one.
		// Never fabricate the default: that would change the disk shape.
		return RestoreTarget{}, snapshotNotFound()
	}
	machine, ok := machinetype.Lookup(machineName)
	if !ok {
		if typeExplicit {
			return RestoreTarget{}, coreInvalidJSON("type must be a supported machine type")
		}
		return RestoreTarget{}, snapshotNotFound()
	}
	resources := db.Resources{
		VCPUs:     machine.VCPUs,
		MemoryMiB: machine.MemoryMiB,
		DiskGiB:   machine.DiskGiB,
	}
	if resources.DiskGiB < source.DiskGiB {
		return RestoreTarget{}, coreInvalidJSON("type disk is smaller than the snapshot's disk")
	}
	return RestoreTarget{Source: source, MachineType: machine.Name, Resources: resources}, nil
}

// snapshotNotFound is the create-from safe missing answer.
func snapshotNotFound() *apierr.Error {
	return apierr.New(http.StatusNotFound, apierr.CodeSnapshotNotFound, "saved snapshot not found")
}
