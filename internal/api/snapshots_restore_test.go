package api

import (
	"context"
	"errors"
	"testing"

	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/apierr"
	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/machinetype"
)

type snapshotRestoreFake struct {
	source db.SnapshotSource
	err    error
}

func (f snapshotRestoreFake) SnapshotRestoreSource(context.Context, uuid.UUID, string) (db.SnapshotSource, error) {
	return f.source, f.err
}

func TestResolveSnapshotRestore(t *testing.T) {
	owner := uuid.NewV7()
	defaultSource := db.SnapshotSource{
		RowID: uuid.NewV7(), OwnerID: owner, Name: "base", SourceSandboxID: "bx_1",
		ArtifactID: "snap-" + "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		NodeID:     "node-a", MachineType: "default", DiskGiB: 51, Generation: 1,
	}
	largeSource := defaultSource
	largeSource.MachineType = "large"
	largeSource.DiskGiB = 111
	legacySource := defaultSource
	legacySource.MachineType = "retired-type"

	tests := []struct {
		name         string
		source       db.SnapshotSource
		err          error
		typeExplicit bool
		requested    string
		wantMachine  string
		wantDisk     uint64
		wantCode     string
		wantStatus   int
	}{
		{
			name: "omitted type inherits", source: largeSource,
			wantMachine: "large", wantDisk: 111,
		},
		{
			name: "explicit equal disk small", source: defaultSource,
			typeExplicit: true, requested: "small",
			wantMachine: "small", wantDisk: 51,
		},
		{
			name: "explicit grow disk large", source: defaultSource,
			typeExplicit: true, requested: "large",
			wantMachine: "large", wantDisk: 111,
		},
		{
			name: "explicit shrink rejected", source: largeSource,
			typeExplicit: true, requested: "small",
			wantStatus: 400, wantCode: apierr.CodeInvalidJSON,
		},
		{
			name: "missing", err: db.ErrSnapshotNotFound,
			wantStatus: 404, wantCode: apierr.CodeSnapshotNotFound,
		},
		{
			name: "omitted type unknown legacy", source: legacySource,
			wantStatus: 404, wantCode: apierr.CodeSnapshotNotFound,
		},
		{
			name: "explicit type keeps legacy disk floor", source: legacySource,
			typeExplicit: true, requested: "large",
			wantMachine: "large", wantDisk: 111,
		},
		{
			name: "unknown legacy disk", err: db.ErrSnapshotDiskUnknown,
			wantStatus: 404, wantCode: apierr.CodeSnapshotNotFound,
		},
		{
			name: "explicit empty type", source: defaultSource,
			typeExplicit: true, requested: "",
			wantStatus: 400, wantCode: apierr.CodeInvalidJSON,
		},
		{
			name: "explicit unknown type", source: defaultSource,
			typeExplicit: true, requested: "enormous",
			wantStatus: 400, wantCode: apierr.CodeInvalidJSON,
		},
		{
			name: "repository error", err: errors.New("driver"),
			wantStatus: 500, wantCode: apierr.CodeInternal,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			target, apiErr := ResolveSnapshotRestore(t.Context(), snapshotRestoreFake{source: tc.source, err: tc.err},
				owner, "base", tc.typeExplicit, tc.requested)
			if tc.wantCode != "" {
				if apiErr == nil || apiErr.Status != tc.wantStatus || apiErr.Code != tc.wantCode {
					t.Fatalf("error = status %d code %q, want %d %s", apiErrStatus(apiErr), apiErrCode(apiErr), tc.wantStatus, tc.wantCode)
				}
				return
			}
			if apiErr != nil {
				t.Fatalf("unexpected error: status %d code %q", apiErr.Status, apiErr.Code)
			}
			if target.MachineType != tc.wantMachine || target.Resources.DiskGiB != tc.wantDisk {
				t.Fatalf("target machine %q disk %d, want machine %s disk %d", target.MachineType, target.Resources.DiskGiB, tc.wantMachine, tc.wantDisk)
			}
			machine, ok := machinetype.Lookup(tc.wantMachine)
			if !ok || target.Resources.VCPUs != machine.VCPUs || target.Resources.MemoryMiB != machine.MemoryMiB {
				t.Fatalf("resources vcpus %d memory %d, machine found %v", target.Resources.VCPUs, target.Resources.MemoryMiB, ok)
			}
			if target.Source.RowID != tc.source.RowID {
				t.Fatalf("source row changed: %s", target.Source.RowID)
			}
		})
	}
}

func TestResolveSnapshotRestoreRequiresIdentity(t *testing.T) {
	if _, err := ResolveSnapshotRestore(t.Context(), nil, uuid.NewV7(), "base", false, ""); err == nil ||
		err.Code != apierr.CodeSnapshotNotFound {
		t.Fatalf("nil resolver: %v", err)
	}
}

// shareRestoreFake implements both name and code resolution.
type shareRestoreFake struct {
	name    db.SnapshotSource
	share   db.SnapshotSource
	err     error
	gotCode string
}

func (f *shareRestoreFake) SnapshotRestoreSource(context.Context, uuid.UUID, string) (db.SnapshotSource, error) {
	return f.name, f.err
}

func (f *shareRestoreFake) SnapshotShareSource(_ context.Context, code string) (db.SnapshotSource, error) {
	f.gotCode = code
	return f.share, f.err
}

func TestResolveSnapshotRestoreGenerationAndShare(t *testing.T) {
	owner := uuid.NewV7()
	sharer := uuid.NewV7()
	generationID := uuid.NewV7()
	generation := db.SnapshotSource{
		RowID: uuid.NewV7(), OwnerID: owner, Name: "base", SourceSandboxID: "bx_1",
		ArtifactID:   "snap-" + "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		NodeID:       "node-a",
		MachineType:  "default",
		DiskGiB:      51,
		Generation:   3,
		GenerationID: &generationID,
		ManifestKey:  "bbraft/chains/x/generations/0000000003.json",
		Image:        "guest.qcow2",
		DiskBytes:    51 << 30,
	}
	t.Run("generation backed", func(t *testing.T) {
		target, apiErr := ResolveSnapshotRestore(t.Context(), &shareRestoreFake{name: generation}, owner, "base", false, "")
		if apiErr != nil {
			t.Fatalf("unexpected error: %s", apiErr.Code)
		}
		if !target.Source.GenerationBacked() || target.Source.ManifestKey != generation.ManifestKey {
			t.Fatalf("source = %+v, want generation-backed", target.Source)
		}
		if target.MachineType != "default" || target.Resources.DiskGiB != 51 {
			t.Fatalf("target = %+v", target)
		}
	})
	t.Run("generation without manifest", func(t *testing.T) {
		broken := generation
		broken.ManifestKey = ""
		_, apiErr := ResolveSnapshotRestore(t.Context(), &shareRestoreFake{name: broken}, owner, "base", false, "")
		if apiErr == nil || apiErr.Code != apierr.CodeSnapshotNotFound {
			t.Fatalf("error = %v, want snapshot_not_found", apiErr)
		}
	})
	shareID := "shr_" + "0123456789abcdef0123456789abcdef"
	shared := generation
	shared.OwnerID = sharer
	shared.ShareID = shareID
	code := ShareCodePrefix + "abcdefghijklmnopqrstuvwxyz234567abcdefghijklmnopqrstuv"
	t.Run("share code", func(t *testing.T) {
		fake := &shareRestoreFake{share: shared}
		target, apiErr := ResolveSnapshotRestore(t.Context(), fake, owner, code, false, "")
		if apiErr != nil {
			t.Fatalf("unexpected error: %s", apiErr.Code)
		}
		if fake.gotCode != code {
			t.Fatalf("resolved code %q, want %q", fake.gotCode, code)
		}
		if target.Source.OwnerID != sharer || target.Source.ShareID != shareID {
			t.Fatalf("source = %+v, want the sharer's code", target.Source)
		}
	})
	t.Run("share without share id", func(t *testing.T) {
		_, apiErr := ResolveSnapshotRestore(t.Context(), &shareRestoreFake{share: generation}, owner, code, false, "")
		if apiErr == nil || apiErr.Code != apierr.CodeSnapshotNotFound {
			t.Fatalf("error = %v, want snapshot_not_found", apiErr)
		}
	})
	t.Run("share on a name-only resolver", func(t *testing.T) {
		_, apiErr := ResolveSnapshotRestore(t.Context(), snapshotRestoreFake{source: generation}, owner, code, false, "")
		if apiErr == nil || apiErr.Code != apierr.CodeSnapshotNotFound {
			t.Fatalf("error = %v, want snapshot_not_found", apiErr)
		}
	})
	t.Run("wrong share code", func(t *testing.T) {
		_, apiErr := ResolveSnapshotRestore(t.Context(), &shareRestoreFake{err: db.ErrSnapshotNotFound}, owner, code, false, "")
		if apiErr == nil || apiErr.Code != apierr.CodeSnapshotNotFound {
			t.Fatalf("error = %v, want snapshot_not_found", apiErr)
		}
	})
}

func apiErrStatus(e *apierr.Error) int {
	if e == nil {
		return 0
	}
	return e.Status
}

func apiErrCode(e *apierr.Error) string {
	if e == nil {
		return ""
	}
	return e.Code
}
