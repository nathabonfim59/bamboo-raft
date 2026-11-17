package db

import (
	"context"
	"fmt"
)

// SavingSnapshots returns one keyset page of generations still in the saving
// state. The snapshot reconciler uses it to discover an artifact whose save
// job row is missing; the paged cursor means a long-lived saving row or a
// concurrently updated row cannot starve later pages. Order is (updated_at,
// id), matching [Store.PurgeableSnapshots].
func (s *Store) SavingSnapshots(ctx context.Context, after *SnapshotCleanupCursor, limit int) ([]SavedSnapshot, *SnapshotCleanupCursor, error) {
	limit = clamp(limit, 1, 500, 100)
	args := []any{}
	where := `status = 'saving'`
	if after != nil {
		args = append(args, after.UpdatedAt, after.ID)
		where += fmt.Sprintf(" AND (updated_at, id) > ($%d, $%d)", len(args)-1, len(args))
	}
	args = append(args, limit+1)
	sql := `SELECT ` + snapshotCols + ` FROM saved_snapshots WHERE ` + where +
		` ORDER BY updated_at, id LIMIT $` + fmt.Sprintf("%d", len(args))
	rows, err := s.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, nil, fmtError("list saving snapshots", err)
	}
	defer rows.Close()
	var out []SavedSnapshot
	for rows.Next() {
		snap, err := scanSnapshot(rows)
		if err != nil {
			return nil, nil, fmtError("scan saving snapshot", err)
		}
		out = append(out, snap)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmtError("list saving snapshots", err)
	}
	if len(out) > limit {
		out = out[:limit]
		last := out[len(out)-1]
		return out, &SnapshotCleanupCursor{UpdatedAt: last.UpdatedAt, ID: last.ID}, nil
	}
	return out, nil, nil
}
