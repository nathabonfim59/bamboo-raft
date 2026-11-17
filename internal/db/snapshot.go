package db

import (
	"context"
	"fmt"
	"strings"
	"time"

	"uuid"
)

// SnapshotStatus is the save lifecycle of one saved-snapshot artifact
// generation.
type SnapshotStatus string

const (
	SnapshotSaving SnapshotStatus = "saving"
	SnapshotReady  SnapshotStatus = "ready"
	SnapshotFailed SnapshotStatus = "failed"
)

// SavedSnapshot is one immutable artifact generation of an owner-scoped name.
// Exactly one row per owner+name is current: it is the name's visible state.
// Older ready rows stay deployable until a replacement generation becomes
// ready, then they are retired and cleaned up. Rows are never hard-deleted;
// purged_at records that the node artifact is gone.
type SavedSnapshot struct {
	ID           uuid.UUID
	OwnerID      uuid.UUID
	Name         string
	SandboxID    string
	Status       SnapshotStatus
	ArtifactID   string // snap-<32 hex>, server-chosen, immutable per row
	Generation   int64
	MachineType  string
	DiskGiB      uint64
	SizeBytes    *int64
	ErrorReason  string
	NodeID       *string
	IsCurrent    bool
	Pins         int
	CleanupError string
	CreatedAt    time.Time
	UpdatedAt    time.Time
	ReadyAt      *time.Time
	RetiredAt    *time.Time
	PurgedAt     *time.Time
	DeletedAt    *time.Time
}

// SnapshotSource is the immutable restore input resolved from a saved-snapshot
// name. The create transaction revalidates every field against the locked row;
// it is not trusted on its own.
type SnapshotSource struct {
	RowID           uuid.UUID
	OwnerID         uuid.UUID
	Name            string
	SourceSandboxID string
	ArtifactID      string
	NodeID          string
	MachineType     string
	DiskGiB         uint64
	SizeBytes       *int64
	Generation      int64
}

const snapshotCols = `id, owner_id, name, sandbox_id, status, artifact_id, generation,
	machine_type, disk_gib, size_bytes, error_reason, node_id, is_current, pins, cleanup_error,
	created_at, updated_at, ready_at, retired_at, purged_at, deleted_at`

func scanSnapshot(row scanner) (SavedSnapshot, error) {
	var s SavedSnapshot
	err := row.Scan(&s.ID, &s.OwnerID, &s.Name, &s.SandboxID, &s.Status, &s.ArtifactID,
		&s.Generation, &s.MachineType, &s.DiskGiB, &s.SizeBytes, &s.ErrorReason, &s.NodeID,
		&s.IsCurrent, &s.Pins, &s.CleanupError,
		&s.CreatedAt, &s.UpdatedAt, &s.ReadyAt, &s.RetiredAt, &s.PurgedAt, &s.DeletedAt)
	return s, err
}

// SavedSnapshot returns one artifact generation by row id, owner-scoped. It
// includes historical, retired and soft-deleted rows so cleanup and restore
// pins can resolve an immutable reference; callers that need the name's
// visible state use [Store.SavedSnapshotByName].
func (s *Store) SavedSnapshot(ctx context.Context, owner uuid.UUID, id uuid.UUID) (SavedSnapshot, error) {
	row := s.pool.QueryRow(ctx,
		`SELECT `+snapshotCols+` FROM saved_snapshots WHERE id = $1 AND owner_id = $2`,
		id, owner)
	snap, err := scanSnapshot(row)
	if err != nil {
		return SavedSnapshot{}, errNoRows("select snapshot", err)
	}
	return snap, nil
}

// SavedSnapshotByName returns the owner's current generation for a name. A
// saving row means a save is in flight; a failed row still leaves earlier
// ready generations deployable through [Store.SnapshotRestoreSource].
func (s *Store) SavedSnapshotByName(ctx context.Context, owner uuid.UUID, name string) (SavedSnapshot, error) {
	row := s.pool.QueryRow(ctx,
		`SELECT `+snapshotCols+` FROM saved_snapshots
		 WHERE owner_id = $1 AND name = $2 AND deleted_at IS NULL AND is_current`,
		owner, name)
	snap, err := scanSnapshot(row)
	if err != nil {
		return SavedSnapshot{}, errNoRows("select snapshot by name", err)
	}
	return snap, nil
}

// SnapshotListQuery pages an owner's saved snapshots. The default is the
// current generation of each live name; IncludeAllGenerations exposes
// historical rows for tests and maintenance. NamePrefix is a literal prefix.
type SnapshotListQuery struct {
	Owner                 uuid.UUID
	Status                *SnapshotStatus
	Name                  string
	NamePrefix            string
	Limit                 int
	Ascending             bool
	After                 *PageCursor
	IncludeAllGenerations bool
}

// ListSavedSnapshots returns one page and the next keyset cursor.
func (s *Store) ListSavedSnapshots(ctx context.Context, q SnapshotListQuery) ([]SavedSnapshot, *PageCursor, error) {
	q.Limit = clamp(q.Limit, 1, 200, 100)
	where := []string{"owner_id = $1", "deleted_at IS NULL"}
	args := []any{q.Owner}
	add := func(fragment string, arg any) {
		args = append(args, arg)
		where = append(where, strings.Replace(fragment, "$n", "$"+fmt.Sprintf("%d", len(args)), 1))
	}
	if !q.IncludeAllGenerations {
		where = append(where, "is_current")
	}
	if q.Status != nil {
		add(`status = $n`, string(*q.Status))
	}
	if q.Name != "" {
		add(`name = $n`, q.Name)
	}
	if q.NamePrefix != "" {
		add(`name LIKE $n`, escapeLike(q.NamePrefix)+"%")
	}
	if q.After != nil {
		c := q.After.truncate()
		op := ">"
		if !q.Ascending {
			op = "<"
		}
		args = append(args, c.CreatedAt, c.ID)
		where = append(where, fmt.Sprintf("(created_at, id) %s ($%d, $%d)", op, len(args)-1, len(args)))
	}
	order := `created_at DESC, id DESC`
	if q.Ascending {
		order = `created_at ASC, id ASC`
	}
	sql := `SELECT ` + snapshotCols + ` FROM saved_snapshots WHERE ` +
		strings.Join(where, " AND ") + ` ORDER BY ` + order +
		` LIMIT ` + fmt.Sprintf("%d", q.Limit+1)
	rows, err := s.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, nil, fmtError("list snapshots", err)
	}
	defer rows.Close()
	var page []SavedSnapshot
	for rows.Next() {
		snap, err := scanSnapshot(rows)
		if err != nil {
			return nil, nil, fmtError("list snapshots scan", err)
		}
		page = append(page, snap)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmtError("list snapshots", err)
	}
	if len(page) > q.Limit {
		last := page[q.Limit-1]
		page = page[:q.Limit]
		next := PageCursor{CreatedAt: last.CreatedAt, ID: last.ID.String()}
		return page, &next, nil
	}
	return page, nil, nil
}

// AllSavedSnapshots walks every internal page and returns all matching rows.
// The pinned saved-snapshot list has no pagination, so the API must never
// truncate at one database page.
func (s *Store) AllSavedSnapshots(ctx context.Context, q SnapshotListQuery) ([]SavedSnapshot, error) {
	q.Limit = 200
	var out []SavedSnapshot
	var after *PageCursor
	for {
		page, next, err := s.ListSavedSnapshots(ctx, SnapshotListQuery{
			Owner:                 q.Owner,
			Status:                q.Status,
			Name:                  q.Name,
			NamePrefix:            q.NamePrefix,
			Limit:                 q.Limit,
			Ascending:             q.Ascending,
			After:                 after,
			IncludeAllGenerations: q.IncludeAllGenerations,
		})
		if err != nil {
			return nil, err
		}
		out = append(out, page...)
		if next == nil {
			return out, nil
		}
		after = next
	}
}
