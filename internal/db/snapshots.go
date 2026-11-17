package db

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/jackc/pgx/v5"
	"uuid"
)

// Saved-snapshot errors. Owner-scoped lookups never distinguish missing rows
// from foreign ones.
var (
	// ErrInvalidSnapshotName rejects a name outside the pinned syntax.
	ErrInvalidSnapshotName = errors.New("db: invalid snapshot name")
	// ErrSnapshotNotFound means the name or no deployable ready generation
	// exists for this owner.
	ErrSnapshotNotFound = errors.New("db: snapshot not found")
	// ErrSnapshotSaveInProgress means the current generation of this name is
	// still saving.
	ErrSnapshotSaveInProgress = errors.New("db: snapshot save in progress")
	// ErrSnapshotSourceNotReady means the source sandbox exists and is live
	// but cannot be saved in its current state.
	ErrSnapshotSourceNotReady = errors.New("db: snapshot source not ready")
	// ErrSnapshotPinMismatch means a pin flag and its counter disagree. It is
	// a data-integrity error, never silently clamped.
	ErrSnapshotPinMismatch = errors.New("db: snapshot pin counter mismatch")
	// ErrInvalidSnapshotArtifact rejects an artifact id outside the pinned
	// snap-<32 lowercase hex> shape.
	ErrInvalidSnapshotArtifact = errors.New("db: invalid snapshot artifact id")
	// ErrSnapshotNotPurgeable means cleanup was asked to mark a row purged
	// while it still has pins or is not retired/deleted/failed.
	ErrSnapshotNotPurgeable = errors.New("db: snapshot not purgeable")
	// ErrSnapshotPinNotReleasable means the restore is neither ready nor
	// archived (StopVM confirmed the disk kept), nor provably cleaned up
	// (reservation released); the pin must stay.
	ErrSnapshotPinNotReleasable = errors.New("db: snapshot pin not releasable")
	// ErrSnapshotDiskTooSmall means the requested restore disk is smaller
	// than the snapshot's canonical disk. Growth is allowed.
	ErrSnapshotDiskTooSmall = errors.New("db: snapshot disk too small")
	// ErrSnapshotDiskUnknown means a legacy snapshot row has no recorded
	// disk and no known machine-type floor. Restore refuses instead of
	// guessing a smaller disk.
	ErrSnapshotDiskUnknown = errors.New("db: snapshot disk unknown")
	// ErrSnapshotJobActive means a save job is queued or running; the
	// reconciler must not decide its outcome.
	ErrSnapshotJobActive = errors.New("db: snapshot job still active")
)

var (
	snapshotNamePattern     = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,62}$`)
	snapshotArtifactPattern = regexp.MustCompile(`^snap-[a-f0-9]{32}$`)
)

// ValidSnapshotName reports the pinned saved-snapshot name syntax.
func ValidSnapshotName(name string) bool { return snapshotNamePattern.MatchString(name) }

// NewSnapshotArtifactID returns a fresh server-chosen artifact id: "snap-"
// plus 32 lowercase hex digits. It is never derived from a user name.
func NewSnapshotArtifactID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("db: snapshot artifact id: %w", err)
	}
	return "snap-" + hex.EncodeToString(b[:]), nil
}

// ValidSnapshotArtifactID reports the exact artifact id shape the database
// CHECK enforces.
func ValidSnapshotArtifactID(id string) bool { return snapshotArtifactPattern.MatchString(id) }

// AcceptSnapshotSaveParams is one accepted save. ArtifactID is server-chosen;
// an empty value is generated here.
type AcceptSnapshotSaveParams struct {
	Owner       uuid.UUID
	SandboxID   string
	Name        string
	ArtifactID  string
	MaxAttempts int
}

// AcceptSnapshotSave atomically starts a new saved-snapshot generation. It
// serializes same-name saves, rejects a save while the current generation is
// saving, refuses a source that is not an own live ready/idle/running sandbox,
// and enqueues the leased save job. The previous ready generation stays
// untouched (non-current but ready) until the new one completes.
func (s *Store) AcceptSnapshotSave(ctx context.Context, p AcceptSnapshotSaveParams) (SavedSnapshot, error) {
	if p.Owner == uuid.Nil() || p.SandboxID == "" {
		return SavedSnapshot{}, errors.New("db: accept snapshot save requires owner and sandbox id")
	}
	if !ValidSnapshotName(p.Name) {
		return SavedSnapshot{}, ErrInvalidSnapshotName
	}
	if p.ArtifactID == "" {
		id, err := NewSnapshotArtifactID()
		if err != nil {
			return SavedSnapshot{}, err
		}
		p.ArtifactID = id
	}
	if !ValidSnapshotArtifactID(p.ArtifactID) {
		return SavedSnapshot{}, ErrInvalidSnapshotArtifact
	}
	if p.MaxAttempts <= 0 {
		p.MaxAttempts = DefaultMaxAttempts
	}

	var out SavedSnapshot
	err := s.runTx(ctx, "accept snapshot save", func(ctx context.Context, tx pgx.Tx) error {
		if err := lockSnapshotNameTx(ctx, tx, p.Owner, p.Name); err != nil {
			return err
		}
		// Fixed lock order: sandbox row first, then saved_snapshot rows.
		var (
			state       SandboxState
			machineType string
			diskGiB     int64
			nodeID      *string
			deletedAt   *time.Time
		)
		err := tx.QueryRow(ctx, `
			SELECT state, machine_type, reserved_disk_gib, node_id, deleted_at
			FROM sandboxes WHERE id = $1 AND owner_id = $2 FOR SHARE`,
			p.SandboxID, p.Owner).Scan(&state, &machineType, &diskGiB, &nodeID, &deletedAt)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return fmtError("snapshot source read", err)
		}
		if deletedAt != nil {
			// A source whose deletion was accepted is indistinguishable
			// from one that never existed.
			return ErrNotFound
		}
		if !snapshotSourceReady(state) || nodeID == nil || *nodeID == "" {
			return ErrSnapshotSourceNotReady
		}

		rows, err := lockSnapshotRowsTx(ctx, tx, p.Owner, p.Name)
		if err != nil {
			return err
		}
		current := currentSnapshot(rows)
		if current != nil && current.Status == SnapshotSaving {
			return ErrSnapshotSaveInProgress
		}
		// Generations are monotonic for the lifetime of the name, even
		// after a delete and reuse, so a late job can never be mistaken for
		// a newer generation.
		generation := int64(1)
		for _, row := range rows {
			if row.Generation >= generation {
				generation = row.Generation + 1
			}
		}
		if current != nil {
			if _, err := tx.Exec(ctx, `
				UPDATE saved_snapshots SET is_current = false, updated_at = now()
				WHERE id = $1 AND owner_id = $2 AND is_current`, current.ID, p.Owner); err != nil {
				return fmtError("retire snapshot name pointer", err)
			}
		}

		row := tx.QueryRow(ctx, `
			INSERT INTO saved_snapshots
				(id, owner_id, name, sandbox_id, status, artifact_id, generation,
				 machine_type, disk_gib, node_id, is_current)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, true)
			RETURNING `+snapshotCols,
			uuid.NewV7(), p.Owner, p.Name, p.SandboxID, SnapshotSaving,
			p.ArtifactID, generation, machineType, diskGiB, nodeID)
		out, err = scanSnapshot(row)
		if err != nil {
			return fmtError("insert snapshot generation", err)
		}

		payload, err := marshalJobPayload(SaveSnapshotPayload{
			SnapshotID: out.ID,
			NodeID:     *nodeID,
			VMID:       p.SandboxID,
			ArtifactID: out.ArtifactID,
			Generation: out.Generation,
		})
		if err != nil {
			return err
		}
		if _, err := enqueueJobTx(ctx, tx, JobInput{
			Kind:        JobSaveSnapshot,
			OwnerID:     p.Owner,
			SandboxID:   &p.SandboxID,
			SnapshotID:  &out.ID,
			Payload:     payload,
			MaxAttempts: p.MaxAttempts,
		}); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return SavedSnapshot{}, err
	}
	return out, nil
}

// AcceptSnapshotDeleteParams names one deletion.
type AcceptSnapshotDeleteParams struct {
	Owner uuid.UUID
	Name  string
}

// AcceptSnapshotDelete removes every live generation of a name. The accepted
// deletion marks rows retired/deleted and enqueues idempotent node cleanup;
// artifacts with active restore pins are cleaned only after the pin releases.
// A save in progress refuses with [ErrSnapshotSaveInProgress].
func (s *Store) AcceptSnapshotDelete(ctx context.Context, p AcceptSnapshotDeleteParams) (SavedSnapshot, error) {
	if p.Owner == uuid.Nil() || p.Name == "" {
		return SavedSnapshot{}, ErrNotFound
	}
	var out SavedSnapshot
	err := s.runTx(ctx, "accept snapshot delete", func(ctx context.Context, tx pgx.Tx) error {
		if err := lockSnapshotNameTx(ctx, tx, p.Owner, p.Name); err != nil {
			return err
		}
		rows, err := lockSnapshotRowsTx(ctx, tx, p.Owner, p.Name)
		if err != nil {
			return err
		}
		current := currentSnapshot(rows)
		if current == nil {
			return ErrNotFound
		}
		if current.Status == SnapshotSaving {
			return ErrSnapshotSaveInProgress
		}
		live := false
		for _, row := range rows {
			if row.DeletedAt == nil {
				live = true
				break
			}
		}
		if !live {
			return ErrNotFound
		}
		updated, err := tx.Query(ctx, `
			UPDATE saved_snapshots SET
				deleted_at = now(),
				retired_at = CASE
					WHEN status = 'ready' AND retired_at IS NULL THEN now()
					ELSE retired_at
				END,
				is_current = false,
				updated_at = now()
			WHERE owner_id = $1 AND name = $2 AND deleted_at IS NULL
			RETURNING `+snapshotCols,
			p.Owner, p.Name)
		if err != nil {
			return fmtError("mark snapshot name deleted", err)
		}
		var deleted []SavedSnapshot
		for updated.Next() {
			row, err := scanSnapshot(updated)
			if err != nil {
				updated.Close()
				return fmtError("scan deleted snapshot", err)
			}
			deleted = append(deleted, row)
		}
		if err := updated.Err(); err != nil {
			updated.Close()
			return fmtError("mark snapshot name deleted", err)
		}
		updated.Close()
		out = *current
		for _, row := range deleted {
			if row.ID == current.ID {
				out = row
			}
			if _, err := enqueueSnapshotDeleteTx(ctx, tx, p.Owner, row); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return SavedSnapshot{}, err
	}
	return out, nil
}

// SnapshotRestoreSource resolves the newest deployable ready generation of a
// name. A replacement may be saving or failed; the previous ready generation
// stays deployable until the replacement becomes ready. Missing, foreign and
// names without a ready generation all answer [ErrSnapshotNotFound].
func (s *Store) SnapshotRestoreSource(ctx context.Context, owner uuid.UUID, name string) (SnapshotSource, error) {
	if owner == uuid.Nil() || name == "" {
		return SnapshotSource{}, ErrSnapshotNotFound
	}
	row := s.pool.QueryRow(ctx, `
		SELECT `+snapshotCols+` FROM saved_snapshots
		WHERE owner_id = $1 AND name = $2 AND deleted_at IS NULL
		  AND status = 'ready' AND retired_at IS NULL
		ORDER BY generation DESC, id DESC
		LIMIT 1`, owner, name)
	snap, err := scanSnapshot(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return SnapshotSource{}, ErrSnapshotNotFound
	}
	if err != nil {
		return SnapshotSource{}, fmtError("resolve snapshot source", err)
	}
	if snap.NodeID == nil || *snap.NodeID == "" || !ValidSnapshotArtifactID(snap.ArtifactID) {
		// A legacy row without a durable node cannot be restored without
		// guessing where its artifact lives.
		return SnapshotSource{}, ErrSnapshotNotFound
	}
	disk := snapshotCanonicalDisk(snap)
	if disk == 0 {
		// Never bypass the grow-only guard because a legacy row has no
		// recorded disk and no known machine type.
		return SnapshotSource{}, ErrSnapshotDiskUnknown
	}
	return SnapshotSource{
		RowID:           snap.ID,
		OwnerID:         snap.OwnerID,
		Name:            snap.Name,
		SourceSandboxID: snap.SandboxID,
		ArtifactID:      snap.ArtifactID,
		NodeID:          *snap.NodeID,
		MachineType:     snap.MachineType,
		DiskGiB:         disk,
		SizeBytes:       snap.SizeBytes,
		Generation:      snap.Generation,
	}, nil
}

// lockRestoreSourceTx locks and revalidates the exact generation a create was
// admitted from, increments its pin, and returns the row. Every mismatch is a
// safe [ErrSnapshotNotFound]: the client sees a missing snapshot, never
// another owner's row.
func lockRestoreSourceTx(ctx context.Context, tx pgx.Tx, owner uuid.UUID, nodeID string, reserve Resources, want SnapshotSource) (SavedSnapshot, error) {
	if want.RowID == uuid.Nil() || nodeID == "" || want.ArtifactID == "" {
		return SavedSnapshot{}, ErrSnapshotNotFound
	}
	row := tx.QueryRow(ctx, `
		SELECT `+snapshotCols+` FROM saved_snapshots
		WHERE id = $1 AND owner_id = $2 FOR UPDATE`, want.RowID, owner)
	snap, err := scanSnapshot(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return SavedSnapshot{}, ErrSnapshotNotFound
	}
	if err != nil {
		return SavedSnapshot{}, fmtError("lock snapshot source", err)
	}
	// Every identity field is immutable and part of the admission decision.
	// A tampered or stale source row is indistinguishable from a missing one.
	disk := snapshotCanonicalDisk(snap)
	if disk == 0 {
		return SavedSnapshot{}, ErrSnapshotDiskUnknown
	}
	if snap.Status != SnapshotReady || snap.RetiredAt != nil || snap.DeletedAt != nil ||
		snap.NodeID == nil || *snap.NodeID != nodeID || snap.ArtifactID != want.ArtifactID ||
		want.OwnerID != owner || want.Name != snap.Name ||
		want.SourceSandboxID != snap.SandboxID || want.Generation != snap.Generation ||
		want.MachineType != snap.MachineType || want.DiskGiB != disk ||
		want.NodeID != nodeID {
		return SavedSnapshot{}, ErrSnapshotNotFound
	}
	// CPU and RAM may grow or shrink with the requested type; only the disk
	// is grow-only because the snapshot's filesystem must fit. The floor is
	// the larger of the recorded disk and the known type floor.
	if reserve.DiskGiB < disk {
		return SavedSnapshot{}, ErrSnapshotDiskTooSmall
	}
	if _, err := tx.Exec(ctx, `
		UPDATE saved_snapshots SET pins = pins + 1, updated_at = now()
		WHERE id = $1 AND owner_id = $2`, snap.ID, owner); err != nil {
		return SavedSnapshot{}, fmtError("pin snapshot source", err)
	}
	snap.Pins++
	return snap, nil
}

// CompleteSnapshotSaveParams is the durable result of one successful node
// SaveSnapshot. JobID/Worker/Token fence the live worker claim;
// Generation/ArtifactID fence a stale generation.
type CompleteSnapshotSaveParams struct {
	Owner      uuid.UUID
	SnapshotID uuid.UUID
	ArtifactID string
	Generation int64
	NodeID     string
	SizeBytes  *int64

	JobID  uuid.UUID
	Worker string
	Token  uuid.UUID
}

// CompleteSnapshotSave marks one saving generation ready. If the row is still
// the current generation it becomes deployable and every older ready
// generation is retired; if a newer current generation exists the completion
// only retires its own row, so a stale worker can never switch or resurrect a
// name. A deleted name is never resurrected.
func (s *Store) CompleteSnapshotSave(ctx context.Context, p CompleteSnapshotSaveParams) (SavedSnapshot, bool, error) {
	var (
		out     SavedSnapshot
		changed bool
	)
	err := s.runTx(ctx, "complete snapshot save", func(ctx context.Context, tx pgx.Tx) error {
		if err := lockSnapshotSaveJobFenceTx(ctx, tx, p.JobID, p.Worker, p.Token, p.Owner, p.SnapshotID); err != nil {
			return err
		}
		name, err := snapshotNameTx(ctx, tx, p.Owner, p.SnapshotID)
		if err != nil {
			return err
		}
		rows, err := lockSnapshotRowsTx(ctx, tx, p.Owner, name)
		if err != nil {
			return err
		}
		row, ok := findSnapshot(rows, p.SnapshotID)
		if !ok {
			return ErrNotFound
		}
		if row.ArtifactID != p.ArtifactID || row.Generation != p.Generation {
			return ErrConflict
		}
		switch row.Status {
		case SnapshotReady:
			out, changed = row, false
			return nil
		case SnapshotFailed:
			// Another actor already recorded a terminal failure for this
			// generation; a delayed success must not resurrect it.
			out, changed = row, false
			return nil
		}
		nodeID, err := mergeSnapshotNodeID(row.NodeID, p.NodeID)
		if err != nil {
			return err
		}
		current := currentSnapshot(rows)
		switch {
		case current == nil || current.DeletedAt != nil:
			// The name was deleted while the save ran. Keep the fact that
			// the artifact succeeded, but never resurrect the name.
			out, err = updateSnapshotReadyTx(ctx, tx, row, nodeID, p.SizeBytes,
				snapshotReadyDeleted)
			if err != nil {
				return err
			}
			changed = true
			return enqueueSnapshotDeleteOrNoop(ctx, tx, p.Owner, out)
		case current.ID == row.ID:
			out, err = updateSnapshotReadyTx(ctx, tx, row, nodeID, p.SizeBytes,
				snapshotReadyCurrent)
			if err != nil {
				return err
			}
			changed = true
			retired, err := retireOlderReadyTx(ctx, tx, p.Owner, name, row.ID)
			if err != nil {
				return err
			}
			for _, old := range retired {
				if _, err := enqueueSnapshotDeleteTx(ctx, tx, p.Owner, old); err != nil {
					return err
				}
			}
			return nil
		default:
			// A newer generation is current: retire only this stale
			// completion; the current row is never mutated.
			out, err = updateSnapshotReadyTx(ctx, tx, row, nodeID, p.SizeBytes,
				snapshotReadyStale)
			if err != nil {
				return err
			}
			changed = true
			return enqueueSnapshotDeleteOrNoop(ctx, tx, p.Owner, out)
		}
	})
	if err != nil {
		return SavedSnapshot{}, false, err
	}
	return out, changed, nil
}

type snapshotReadyMode int

const (
	snapshotReadyCurrent snapshotReadyMode = iota
	snapshotReadyStale
	snapshotReadyDeleted
)

// updateSnapshotReadyTx writes one ready generation. It never touches another
// generation's current flag.
func updateSnapshotReadyTx(ctx context.Context, tx pgx.Tx, row SavedSnapshot, nodeID string,
	size *int64, mode snapshotReadyMode,
) (SavedSnapshot, error) {
	var (
		retired   any
		deleted   any
		isCurrent = row.IsCurrent
		sizeArg   any
	)
	if size != nil {
		sizeArg = *size
	}
	switch mode {
	case snapshotReadyCurrent:
		isCurrent = true
	case snapshotReadyStale:
		retired = time.Now()
		isCurrent = false
	case snapshotReadyDeleted:
		retired = time.Now()
		deleted = time.Now()
		isCurrent = false
	}
	updated := tx.QueryRow(ctx, `
		UPDATE saved_snapshots SET
			status = 'ready',
			size_bytes = COALESCE($3, size_bytes),
			node_id = $4,
			error_reason = '',
			ready_at = now(),
			retired_at = $5,
			deleted_at = COALESCE(deleted_at, $6),
			is_current = $7,
			updated_at = now()
		WHERE id = $1 AND owner_id = $2
		RETURNING `+snapshotCols,
		row.ID, row.OwnerID, sizeArg, nodeID, retired, deleted, isCurrent)
	out, err := scanSnapshot(updated)
	if err != nil {
		return SavedSnapshot{}, fmtError("mark snapshot ready", err)
	}
	return out, nil
}

// FailSnapshotSaveParams records a terminal save failure under a live worker
// claim. reason must be a fixed, non-secret classification.
type FailSnapshotSaveParams struct {
	Owner      uuid.UUID
	SnapshotID uuid.UUID
	ArtifactID string
	Generation int64
	Reason     string

	JobID  uuid.UUID
	Worker string
	Token  uuid.UUID
}

// FailSnapshotSave moves a saving generation to failed without touching earlier
// ready generations. When the failed row is current the name reports failed;
// restore-by-name still resolves the newest earlier ready generation.
func (s *Store) FailSnapshotSave(ctx context.Context, p FailSnapshotSaveParams) (SavedSnapshot, bool, error) {
	var (
		out     SavedSnapshot
		changed bool
	)
	err := s.runTx(ctx, "fail snapshot save", func(ctx context.Context, tx pgx.Tx) error {
		if err := lockSnapshotSaveJobFenceTx(ctx, tx, p.JobID, p.Worker, p.Token, p.Owner, p.SnapshotID); err != nil {
			return err
		}
		name, err := snapshotNameTx(ctx, tx, p.Owner, p.SnapshotID)
		if err != nil {
			return err
		}
		rows, err := lockSnapshotRowsTx(ctx, tx, p.Owner, name)
		if err != nil {
			return err
		}
		row, ok := findSnapshot(rows, p.SnapshotID)
		if !ok {
			return ErrNotFound
		}
		if row.ArtifactID != p.ArtifactID || row.Generation != p.Generation {
			return ErrConflict
		}
		if row.Status != SnapshotSaving {
			out, changed = row, false
			return nil
		}
		var retired any
		if !row.IsCurrent {
			retired = time.Now()
		}
		updated := tx.QueryRow(ctx, `
			UPDATE saved_snapshots SET
				status = 'failed',
				error_reason = $3,
				retired_at = COALESCE(retired_at, $4),
				updated_at = now()
			WHERE id = $1 AND owner_id = $2
			RETURNING `+snapshotCols,
			row.ID, p.Owner, p.Reason, retired)
		out, err = scanSnapshot(updated)
		if err != nil {
			return fmtError("mark snapshot failed", err)
		}
		changed = true
		return enqueueSnapshotDeleteOrNoop(ctx, tx, p.Owner, out)
	})
	if err != nil {
		return SavedSnapshot{}, false, err
	}
	return out, changed, nil
}

// ReconcileSnapshotSaveFailureParams records a terminal save failure from a
// reconciler that has no worker claim.
type ReconcileSnapshotSaveFailureParams struct {
	Owner      uuid.UUID
	SnapshotID uuid.UUID
	ArtifactID string
	Generation int64
	Reason     string
}

// ReconcileSnapshotSaveFailure fails a saving generation only after atomically
// proving every canonical save job for it is terminal or missing. It locks all
// matching job rows FOR UPDATE first; a queued/running claim therefore answers
// [ErrSnapshotJobActive] instead of racing a new worker into a terminal row.
func (s *Store) ReconcileSnapshotSaveFailure(ctx context.Context, p ReconcileSnapshotSaveFailureParams) (SavedSnapshot, bool, error) {
	var (
		out     SavedSnapshot
		changed bool
	)
	err := s.runTx(ctx, "reconcile snapshot save failure", func(ctx context.Context, tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			SELECT state FROM jobs
			WHERE owner_id = $1 AND snapshot_id = $2 AND kind = $3
			ORDER BY id FOR UPDATE`,
			p.Owner, p.SnapshotID, JobSaveSnapshot)
		if err != nil {
			return fmtError("lock snapshot save jobs", err)
		}
		active := false
		for rows.Next() {
			var state JobState
			if err := rows.Scan(&state); err != nil {
				rows.Close()
				return fmtError("scan snapshot save jobs", err)
			}
			if state == JobQueued || state == JobRunning {
				active = true
			}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return fmtError("lock snapshot save jobs", err)
		}
		rows.Close()
		if active {
			return ErrSnapshotJobActive
		}

		name, err := snapshotNameTx(ctx, tx, p.Owner, p.SnapshotID)
		if err != nil {
			return err
		}
		locks, err := lockSnapshotRowsTx(ctx, tx, p.Owner, name)
		if err != nil {
			return err
		}
		row, ok := findSnapshot(locks, p.SnapshotID)
		if !ok {
			return ErrNotFound
		}
		if row.ArtifactID != p.ArtifactID || row.Generation != p.Generation {
			return ErrConflict
		}
		if row.Status != SnapshotSaving {
			out, changed = row, false
			return nil
		}
		var retired any
		if !row.IsCurrent {
			retired = time.Now()
		}
		updated := tx.QueryRow(ctx, `
			UPDATE saved_snapshots SET
				status = 'failed',
				error_reason = $3,
				retired_at = COALESCE(retired_at, $4),
				updated_at = now()
			WHERE id = $1 AND owner_id = $2
			RETURNING `+snapshotCols,
			row.ID, p.Owner, p.Reason, retired)
		out, err = scanSnapshot(updated)
		if err != nil {
			return fmtError("reconcile snapshot failure write", err)
		}
		changed = true
		return enqueueSnapshotDeleteOrNoop(ctx, tx, p.Owner, out)
	})
	if err != nil {
		return SavedSnapshot{}, false, err
	}
	return out, changed, nil
}

// ReleaseSnapshotPin releases one restore's artifact pin exactly once. It
// locks the sandbox first and the artifact second (the fixed snapshot lock
// order). A flag without a counter is [ErrSnapshotPinMismatch], never clamped.
func (s *Store) ReleaseSnapshotPin(ctx context.Context, owner uuid.UUID, sandboxID string) error {
	return s.runTx(ctx, "release snapshot pin", func(ctx context.Context, tx pgx.Tx) error {
		var (
			snapshotID *uuid.UUID
			pinned     bool
			state      SandboxState
			vcpus      int64
			memoryMiB  int64
			diskGiB    int64
		)
		err := tx.QueryRow(ctx, `
			SELECT snapshot_id, snapshot_pin, state,
			       reserved_vcpus, reserved_memory_mib, reserved_disk_gib
			FROM sandboxes WHERE id = $1 AND owner_id = $2 FOR UPDATE`,
			sandboxID, owner).Scan(&snapshotID, &pinned, &state, &vcpus, &memoryMiB, &diskGiB)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return fmtError("release snapshot pin read", err)
		}
		if !pinned {
			return nil
		}
		// Releasing early would let cleanup delete an artifact a boot may
		// still need. Only a sandbox that reached ready, one that reached
		// archived after StopVM confirmed the disk kept, or one whose
		// reservation was released after confirmed DeleteVM is proof.
		if !snapshotPinReleasable(state, vcpus, memoryMiB, diskGiB) {
			return ErrSnapshotPinNotReleasable
		}
		if snapshotID == nil {
			return ErrSnapshotPinMismatch
		}
		tag, err := tx.Exec(ctx, `
			UPDATE saved_snapshots SET pins = pins - 1, updated_at = now()
			WHERE id = $1 AND owner_id = $2 AND pins > 0`, *snapshotID, owner)
		if err != nil {
			return fmtError("release snapshot pin decrement", err)
		}
		if tag.RowsAffected() == 0 {
			return ErrSnapshotPinMismatch
		}
		if _, err := tx.Exec(ctx, `
			UPDATE sandboxes SET snapshot_pin = false, updated_at = now()
			WHERE id = $1 AND owner_id = $2`, sandboxID, owner); err != nil {
			return fmtError("release snapshot pin clear", err)
		}
		row := tx.QueryRow(ctx,
			`SELECT `+snapshotCols+` FROM saved_snapshots WHERE id = $1 AND owner_id = $2`,
			*snapshotID, owner)
		snap, err := scanSnapshot(row)
		if err != nil {
			return fmtError("release snapshot pin snapshot read", err)
		}
		_, err = enqueueSnapshotDeleteTx(ctx, tx, owner, snap)
		return err
	})
}

// SnapshotCleanupCursor is the keyset position after one maintenance page:
// updated_at at PostgreSQL microsecond precision plus the row id.
type SnapshotCleanupCursor struct {
	UpdatedAt time.Time
	ID        uuid.UUID
}

// SnapshotPinCursor is the keyset position for eligible-pin scans on the
// sandboxes table.
type SnapshotPinCursor struct {
	UpdatedAt time.Time
	ID        string
}

// ReleaseEligibleSnapshotPins releases at most one page of pins whose restore
// outcome is already proven: a sandbox that reached ready/idle/running or
// archived, or whose reservation was released after a confirmed DeleteVM. It
// returns the next keyset cursor (nil at the end) so a batch containing an
// integrity mismatch cannot starve later rows on the next call.
func (s *Store) ReleaseEligibleSnapshotPins(ctx context.Context, after *SnapshotPinCursor, limit int) (int, *SnapshotPinCursor, error) {
	limit = clamp(limit, 1, 500, 100)
	args := []any{}
	where := `snapshot_pin
		  AND (state IN ('ready', 'idle', 'running', 'archived')
		       OR (reserved_vcpus = 0 AND reserved_memory_mib = 0 AND reserved_disk_gib = 0))`
	if after != nil {
		args = append(args, after.UpdatedAt, after.ID)
		where += fmt.Sprintf(" AND (updated_at, id) > ($%d, $%d)", len(args)-1, len(args))
	}
	args = append(args, limit+1)
	rows, err := s.pool.Query(ctx, `
		SELECT updated_at, id, owner_id FROM sandboxes
		WHERE `+where+`
		ORDER BY updated_at, id
		LIMIT $`+fmt.Sprintf("%d", len(args)), args...)
	if err != nil {
		return 0, nil, fmtError("list eligible snapshot pins", err)
	}
	type pin struct {
		updatedAt time.Time
		id        string
		owner     uuid.UUID
	}
	var pins []pin
	for rows.Next() {
		var p pin
		if err := rows.Scan(&p.updatedAt, &p.id, &p.owner); err != nil {
			rows.Close()
			return 0, nil, fmtError("scan eligible snapshot pin", err)
		}
		pins = append(pins, p)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, nil, fmtError("list eligible snapshot pins", err)
	}
	rows.Close()
	var next *SnapshotPinCursor
	if len(pins) > limit {
		pins = pins[:limit]
		last := pins[len(pins)-1]
		next = &SnapshotPinCursor{UpdatedAt: last.updatedAt, ID: last.id}
	}
	released := 0
	for _, p := range pins {
		if ctx.Err() != nil {
			return released, next, ctx.Err()
		}
		if err := s.ReleaseSnapshotPin(ctx, p.owner, p.id); err != nil {
			if errors.Is(err, ErrNotFound) || errors.Is(err, ErrSnapshotPinMismatch) ||
				errors.Is(err, ErrSnapshotPinNotReleasable) {
				continue
			}
			return released, next, err
		}
		released++
	}
	return released, next, nil
}

// PurgeableSnapshots returns one keyset page of retired/deleted/failed
// generations whose node artifact is safe to remove: no restore pins, a
// recorded node and not yet purged. Order is (updated_at, id). Callers walk
// pages instead of re-reading a sticky first batch, so rows blocked by an
// active cleanup job cannot starve later rows.
func (s *Store) PurgeableSnapshots(ctx context.Context, after *SnapshotCleanupCursor, limit int) ([]SavedSnapshot, *SnapshotCleanupCursor, error) {
	limit = clamp(limit, 1, 500, 100)
	args := []any{}
	where := `purged_at IS NULL AND pins = 0 AND node_id IS NOT NULL
		  AND (deleted_at IS NOT NULL OR retired_at IS NOT NULL OR status = 'failed')`
	if after != nil {
		args = append(args, after.UpdatedAt, after.ID)
		where += fmt.Sprintf(" AND (updated_at, id) > ($%d, $%d)", len(args)-1, len(args))
	}
	args = append(args, limit+1)
	sql := `SELECT ` + snapshotCols + ` FROM saved_snapshots WHERE ` + where +
		` ORDER BY updated_at, id LIMIT $` + fmt.Sprintf("%d", len(args))
	rows, err := s.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, nil, fmtError("list purgeable snapshots", err)
	}
	defer rows.Close()
	var out []SavedSnapshot
	for rows.Next() {
		snap, err := scanSnapshot(rows)
		if err != nil {
			return nil, nil, fmtError("scan purgeable snapshot", err)
		}
		out = append(out, snap)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmtError("list purgeable snapshots", err)
	}
	if len(out) > limit {
		out = out[:limit]
		last := out[len(out)-1]
		return out, &SnapshotCleanupCursor{UpdatedAt: last.UpdatedAt, ID: last.ID}, nil
	}
	return out, nil, nil
}

// CleanupArtifactsOnNode walks every immutable generation recorded on a node,
// including rows already marked purged. The node inventory reconciler compares
// these with Info.snapshots so an artifact that reappears after a late
// SaveSnapshot (DeleteSnapshot does not fence an earlier save) is cleaned
// again. It never invents an artifact: only rows with this node_id are
// returned.
func (s *Store) CleanupArtifactsOnNode(ctx context.Context, nodeID string, after *SnapshotCleanupCursor, limit int) ([]SavedSnapshot, *SnapshotCleanupCursor, error) {
	limit = clamp(limit, 1, 500, 100)
	args := []any{nodeID}
	where := `node_id = $1
		  AND (deleted_at IS NOT NULL OR retired_at IS NOT NULL OR status = 'failed')`
	if after != nil {
		args = append(args, after.UpdatedAt, after.ID)
		where += fmt.Sprintf(" AND (updated_at, id) > ($%d, $%d)", len(args)-1, len(args))
	}
	args = append(args, limit+1)
	sql := `SELECT ` + snapshotCols + ` FROM saved_snapshots WHERE ` + where +
		` ORDER BY updated_at, id LIMIT $` + fmt.Sprintf("%d", len(args))
	rows, err := s.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, nil, fmtError("list node cleanup artifacts", err)
	}
	defer rows.Close()
	var out []SavedSnapshot
	for rows.Next() {
		snap, err := scanSnapshot(rows)
		if err != nil {
			return nil, nil, fmtError("scan node cleanup artifact", err)
		}
		out = append(out, snap)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmtError("list node cleanup artifacts", err)
	}
	if len(out) > limit {
		out = out[:limit]
		last := out[len(out)-1]
		return out, &SnapshotCleanupCursor{UpdatedAt: last.UpdatedAt, ID: last.ID}, nil
	}
	return out, nil, nil
}

// SavedSnapshotByArtifact is the system reconciler's global artifact lookup.
// Artifact ids are server-generated and unique, so a node's Info list can be
// matched without guessing an owner. Unknown ids report false and are never
// deleted.
func (s *Store) SavedSnapshotByArtifact(ctx context.Context, artifactID string) (SavedSnapshot, bool, error) {
	row := s.pool.QueryRow(ctx,
		`SELECT `+snapshotCols+` FROM saved_snapshots WHERE artifact_id = $1`, artifactID)
	snap, err := scanSnapshot(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return SavedSnapshot{}, false, nil
	}
	if err != nil {
		return SavedSnapshot{}, false, fmtError("select snapshot by artifact", err)
	}
	return snap, true, nil
}

// EnqueueSnapshotDelete writes one snapshot.delete job unless the artifact
// already has a queued or running cleanup. The artifact row is locked so two
// reconcilers cannot enqueue duplicates; DeleteSnapshot is idempotent anyway.
func (s *Store) EnqueueSnapshotDelete(ctx context.Context, owner uuid.UUID, snapshotID uuid.UUID) (bool, error) {
	var added bool
	err := s.runTx(ctx, "enqueue snapshot delete", func(ctx context.Context, tx pgx.Tx) error {
		row := tx.QueryRow(ctx, `
			SELECT `+snapshotCols+` FROM saved_snapshots
			WHERE id = $1 AND owner_id = $2 FOR UPDATE`, snapshotID, owner)
		snap, err := scanSnapshot(row)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return fmtError("enqueue snapshot delete read", err)
		}
		added, err = enqueueSnapshotDeleteTx(ctx, tx, owner, snap)
		return err
	})
	if err != nil {
		return false, err
	}
	return added, nil
}

// MarkSnapshotPurged records that the node confirmed its snapshot artifact is
// gone. It is idempotent and refuses a row that still has a restore pin or is
// not retired/deleted/failed.
func (s *Store) MarkSnapshotPurged(ctx context.Context, owner uuid.UUID, snapshotID uuid.UUID, artifactID string) (SavedSnapshot, error) {
	var out SavedSnapshot
	err := s.runTx(ctx, "mark snapshot purged", func(ctx context.Context, tx pgx.Tx) error {
		row := tx.QueryRow(ctx, `
			SELECT `+snapshotCols+` FROM saved_snapshots
			WHERE id = $1 AND owner_id = $2 FOR UPDATE`, snapshotID, owner)
		snap, err := scanSnapshot(row)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return fmtError("mark snapshot purged read", err)
		}
		if snap.ArtifactID != artifactID {
			return ErrConflict
		}
		if snap.PurgedAt != nil {
			out = snap
			return nil
		}
		if !snapshotPurgeable(snap) {
			return ErrSnapshotNotPurgeable
		}
		updated := tx.QueryRow(ctx, `
			UPDATE saved_snapshots SET purged_at = now(), cleanup_error = '', updated_at = now()
			WHERE id = $1 AND owner_id = $2
			RETURNING `+snapshotCols, snapshotID, owner)
		out, err = scanSnapshot(updated)
		if err != nil {
			return fmtError("mark snapshot purged", err)
		}
		return nil
	})
	if err != nil {
		return SavedSnapshot{}, err
	}
	return out, nil
}

// ReopenSnapshotCleanup re-arms cleanup for an artifact that reappeared in a
// node's Info list after it had been marked purged. It never touches unknown
// artifacts: the row must be known, immutable (retired/deleted/failed), have
// no pins and a recorded node. It clears purged_at and enqueues one cleanup
// job transactionally.
func (s *Store) ReopenSnapshotCleanup(ctx context.Context, owner uuid.UUID, snapshotID uuid.UUID, artifactID string) (SavedSnapshot, bool, error) {
	var (
		out     SavedSnapshot
		changed bool
	)
	err := s.runTx(ctx, "reopen snapshot cleanup", func(ctx context.Context, tx pgx.Tx) error {
		row := tx.QueryRow(ctx, `
			SELECT `+snapshotCols+` FROM saved_snapshots
			WHERE id = $1 AND owner_id = $2 FOR UPDATE`, snapshotID, owner)
		snap, err := scanSnapshot(row)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return fmtError("reopen snapshot cleanup read", err)
		}
		if snap.ArtifactID != artifactID {
			return ErrConflict
		}
		if !snapshotImmutableArtifact(snap) {
			return ErrSnapshotNotPurgeable
		}
		if snap.PurgedAt == nil {
			out = snap
			return nil
		}
		updated := tx.QueryRow(ctx, `
			UPDATE saved_snapshots SET purged_at = NULL, cleanup_error = '', updated_at = now()
			WHERE id = $1 AND owner_id = $2
			RETURNING `+snapshotCols, snapshotID, owner)
		out, err = scanSnapshot(updated)
		if err != nil {
			return fmtError("reopen snapshot cleanup", err)
		}
		changed = true
		_, err = enqueueSnapshotDeleteTx(ctx, tx, owner, out)
		return err
	})
	if err != nil {
		return SavedSnapshot{}, false, err
	}
	return out, changed, nil
}

// snapshotNameTx reads the immutable name of one generation.
func snapshotNameTx(ctx context.Context, tx pgx.Tx, owner uuid.UUID, snapshotID uuid.UUID) (string, error) {
	var name string
	err := tx.QueryRow(ctx,
		`SELECT name FROM saved_snapshots WHERE id = $1 AND owner_id = $2`,
		snapshotID, owner).Scan(&name)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrNotFound
	}
	if err != nil {
		return "", fmtError("snapshot name read", err)
	}
	return name, nil
}

// lockSnapshotSaveJobFenceTx proves this caller still owns the live save claim
// and that the claim belongs to this owner and artifact generation. It locks
// the job row FOR SHARE, the same fixed order as the reconciler's FOR UPDATE,
// so a completion and a reconciliation decision cannot interleave.
func lockSnapshotSaveJobFenceTx(ctx context.Context, tx pgx.Tx, jobID uuid.UUID,
	worker string, token uuid.UUID, owner uuid.UUID, snapshotID uuid.UUID,
) error {
	if jobID == uuid.Nil() || worker == "" || token == uuid.Nil() {
		return ErrJobNotLeased
	}
	var one int
	err := tx.QueryRow(ctx, `
		SELECT 1 FROM jobs
		WHERE id = $1 AND `+jobFence+`
		  AND owner_id = $4 AND snapshot_id = $5 AND kind = $6
		FOR SHARE`, jobID, worker, token, owner, snapshotID, JobSaveSnapshot).Scan(&one)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrJobNotLeased
	}
	if err != nil {
		return fmtError("lock snapshot save job fence", err)
	}
	return nil
}

// lockSnapshotNameTx serializes same-name saves/deletes for one owner.
func lockSnapshotNameTx(ctx context.Context, tx pgx.Tx, owner uuid.UUID, name string) error {
	if _, err := tx.Exec(ctx,
		`SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`,
		"bbraft-snapshot:"+owner.String()+":"+name); err != nil {
		return fmtError("lock snapshot name", err)
	}
	return nil
}

// lockSnapshotRowsTx locks every generation row of one owner+name in id order.
// This is the fixed lock order every snapshot writer uses.
func lockSnapshotRowsTx(ctx context.Context, tx pgx.Tx, owner uuid.UUID, name string) ([]SavedSnapshot, error) {
	rows, err := tx.Query(ctx, `
		SELECT `+snapshotCols+` FROM saved_snapshots
		WHERE owner_id = $1 AND name = $2 ORDER BY id FOR UPDATE`, owner, name)
	if err != nil {
		return nil, fmtError("lock snapshot rows", err)
	}
	defer rows.Close()
	var out []SavedSnapshot
	for rows.Next() {
		snap, err := scanSnapshot(rows)
		if err != nil {
			return nil, fmtError("scan locked snapshot", err)
		}
		out = append(out, snap)
	}
	if err := rows.Err(); err != nil {
		return nil, fmtError("lock snapshot rows", err)
	}
	return out, nil
}

// findSnapshot returns the generation with id from an already-locked set.
func findSnapshot(rows []SavedSnapshot, id uuid.UUID) (SavedSnapshot, bool) {
	for _, row := range rows {
		if row.ID == id {
			return row, true
		}
	}
	return SavedSnapshot{}, false
}

// currentSnapshot returns the live current generation from an already-locked
// set, if any.
func currentSnapshot(rows []SavedSnapshot) *SavedSnapshot {
	for i := range rows {
		if rows[i].DeletedAt == nil && rows[i].IsCurrent {
			return &rows[i]
		}
	}
	return nil
}

// retireOlderReadyTx marks every other live ready generation of the name
// retired and returns the updated rows for cleanup.
func retireOlderReadyTx(ctx context.Context, tx pgx.Tx, owner uuid.UUID, name string, keep uuid.UUID) ([]SavedSnapshot, error) {
	rows, err := tx.Query(ctx, `
		UPDATE saved_snapshots SET retired_at = now(), is_current = false, updated_at = now()
		WHERE owner_id = $1 AND name = $2 AND id <> $3
		  AND deleted_at IS NULL AND status = 'ready' AND retired_at IS NULL
		RETURNING `+snapshotCols, owner, name, keep)
	if err != nil {
		return nil, fmtError("retire older snapshot generation", err)
	}
	defer rows.Close()
	var out []SavedSnapshot
	for rows.Next() {
		snap, err := scanSnapshot(rows)
		if err != nil {
			return nil, fmtError("scan retired snapshot", err)
		}
		out = append(out, snap)
	}
	if err := rows.Err(); err != nil {
		return nil, fmtError("retire older snapshot generation", err)
	}
	return out, nil
}

// mergeSnapshotNodeID prefers the row's recorded node and refuses a payload
// that tries to redirect the artifact to another node.
func mergeSnapshotNodeID(recorded *string, fromPayload string) (string, error) {
	if recorded != nil && *recorded != "" {
		if fromPayload != "" && fromPayload != *recorded {
			return "", ErrConflict
		}
		return *recorded, nil
	}
	if fromPayload == "" {
		return "", errors.New("db: snapshot has no artifact node")
	}
	return fromPayload, nil
}

// enqueueSnapshotDeleteOrNoop is the transaction-local backstop after a row
// becomes purgeable.
func enqueueSnapshotDeleteOrNoop(ctx context.Context, tx pgx.Tx, owner uuid.UUID, snap SavedSnapshot) error {
	_, err := enqueueSnapshotDeleteTx(ctx, tx, owner, snap)
	return err
}

// enqueueSnapshotDeleteTx inserts at most one active cleanup job per artifact.
func enqueueSnapshotDeleteTx(ctx context.Context, tx pgx.Tx, owner uuid.UUID, snap SavedSnapshot) (bool, error) {
	if snap.PurgedAt != nil || snap.NodeID == nil || *snap.NodeID == "" || !snapshotPurgeable(snap) {
		return false, nil
	}
	if _, err := tx.Exec(ctx,
		`SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`,
		"bbraft-snapshot-delete:"+snap.ID.String()); err != nil {
		return false, fmtError("lock snapshot cleanup", err)
	}
	var active bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM jobs
			WHERE owner_id = $1 AND snapshot_id = $2 AND kind = $3
			  AND state IN ('queued', 'running'))`,
		owner, snap.ID, JobDeleteSnapshot).Scan(&active); err != nil {
		return false, fmtError("check active snapshot cleanup", err)
	}
	if active {
		return false, nil
	}
	payload, err := marshalJobPayload(DeleteSnapshotPayload{
		SnapshotID: snap.ID,
		NodeID:     *snap.NodeID,
		ArtifactID: snap.ArtifactID,
	})
	if err != nil {
		return false, err
	}
	sandboxID := snap.SandboxID
	if _, err := enqueueJobTx(ctx, tx, JobInput{
		Kind:       JobDeleteSnapshot,
		OwnerID:    owner,
		SandboxID:  &sandboxID,
		SnapshotID: &snap.ID,
		Payload:    payload,
	}); err != nil {
		return false, err
	}
	return true, nil
}

// snapshotPurgeable is the single cleanup predicate: an immutable retired,
// deleted or failed artifact that has not been purged yet.
func snapshotPurgeable(snap SavedSnapshot) bool {
	return snap.PurgedAt == nil && snapshotImmutableArtifact(snap)
}

// snapshotImmutableArtifact reports the terminal artifact states and safety
// conditions under which cleanup may run. A purged row is included so node
// inventory reconciliation can detect an artifact that reappeared after a late
// SaveSnapshot; DeleteSnapshot is idempotent but does not fence an earlier
// save, so repeated reconciliation is required.
func snapshotImmutableArtifact(snap SavedSnapshot) bool {
	return snap.Pins == 0 && snap.NodeID != nil && *snap.NodeID != "" &&
		(snap.DeletedAt != nil || snap.RetiredAt != nil || snap.Status == SnapshotFailed)
}

// snapshotCanonicalDisk returns the grow-only floor for a saved snapshot: the
// larger of its recorded disk and the known machine-type disk. Zero means
// neither is known and restore must refuse rather than guess.
func snapshotCanonicalDisk(snap SavedSnapshot) uint64 {
	floor := uint64(0)
	switch snap.MachineType {
	case "small", "default":
		floor = 51
	case "large":
		floor = 111
	case "xlarge":
		floor = 251
	}
	return max(snap.DiskGiB, floor)
}

// snapshotSourceReady reports the states a save may start from.
func snapshotSourceReady(state SandboxState) bool {
	switch state {
	case StateReady, StateIdle, StateRunning:
		return true
	default:
		return false
	}
}

// snapshotPinReleasable reports the confirmed outcomes that may release a
// restore pin: the sandbox reached a ready-ish state, StopVM confirmed the VM
// off with its disk kept (canonical archived), or its reservation was already
// released after a confirmed DeleteVM. Archiving is deliberately excluded
// (unless nothing is reserved): the first disk may still be in flight and the
// artifact must stay until the stop outcome is confirmed. This is not part of
// the save/create source rules; snapshotSourceReady stays unchanged.
func snapshotPinReleasable(state SandboxState, vcpus, memoryMiB, diskGiB int64) bool {
	if snapshotSourceReady(state) || state == StateArchived {
		return true
	}
	return vcpus == 0 && memoryMiB == 0 && diskGiB == 0
}
