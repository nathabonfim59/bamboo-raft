-- +goose Up
-- Saved snapshots are now one immutable row per artifact generation. The
-- current row of an owner+name is the name's visible state; older ready rows
-- stay deployable until a replacement generation is ready, and every row stays
-- until its node artifact is durably cleaned up.

ALTER TABLE saved_snapshots
    ADD COLUMN node_id       text REFERENCES nodes (id),
    ADD COLUMN disk_gib      bigint NOT NULL DEFAULT 0,
    ADD COLUMN is_current    boolean NOT NULL DEFAULT true,
    ADD COLUMN ready_at      timestamptz,
    ADD COLUMN retired_at    timestamptz,
    ADD COLUMN purged_at     timestamptz,
    ADD COLUMN pins          integer NOT NULL DEFAULT 0 CHECK (pins >= 0),
    ADD COLUMN cleanup_error text NOT NULL DEFAULT '';

-- Existing rows are one live row per name (the old unique index guarantees
-- it). Backfill the durable artifact node and disk size. The disk prefers a
-- live reservation but falls back to the known machine-type floor when the
-- source sandbox's reservation was already released (for example after its
-- deletion); an unknown type keeps 0 and restore rejects it instead of
-- guessing.
UPDATE saved_snapshots AS s
SET node_id = COALESCE(s.node_id, b.node_id),
    disk_gib = CASE
        WHEN s.disk_gib > 0 THEN s.disk_gib
        WHEN b.reserved_disk_gib > 0 THEN b.reserved_disk_gib
        WHEN s.machine_type IN ('small', 'default') THEN 51
        WHEN s.machine_type = 'large' THEN 111
        WHEN s.machine_type = 'xlarge' THEN 251
        ELSE 0
    END
FROM sandboxes AS b
WHERE b.id = s.sandbox_id AND b.owner_id = s.owner_id;

-- Rows whose source row is unavailable still get a known type floor.
UPDATE saved_snapshots
SET disk_gib = CASE
    WHEN machine_type IN ('small', 'default') THEN 51
    WHEN machine_type = 'large' THEN 111
    WHEN machine_type = 'xlarge' THEN 251
    ELSE disk_gib
END
WHERE disk_gib = 0 AND machine_type IN ('small', 'default', 'large', 'xlarge');

UPDATE saved_snapshots SET is_current = (deleted_at IS NULL);

DROP INDEX saved_snapshots_owner_name_active_uk;
DROP INDEX saved_snapshots_owner_list_idx;
DROP INDEX saved_snapshots_artifact_idx;

CREATE UNIQUE INDEX saved_snapshots_owner_name_current_uk
    ON saved_snapshots (owner_id, name)
    WHERE deleted_at IS NULL AND is_current;
CREATE UNIQUE INDEX saved_snapshots_artifact_id_uk
    ON saved_snapshots (artifact_id);
CREATE INDEX saved_snapshots_owner_current_list_idx
    ON saved_snapshots (owner_id, created_at DESC, id DESC)
    WHERE deleted_at IS NULL AND is_current;
CREATE INDEX saved_snapshots_purge_idx
    ON saved_snapshots (updated_at, id)
    WHERE purged_at IS NULL;

-- A restored sandbox pins the exact immutable artifact row it was accepted
-- from. snapshot_pin is the exactly-once release flag; pins on the artifact
-- row is the durable count that cleanup must observe.
ALTER TABLE sandboxes
    ADD COLUMN snapshot_id      uuid REFERENCES saved_snapshots (id),
    ADD COLUMN snapshot_pin     boolean NOT NULL DEFAULT false,
    ADD COLUMN from_snapshot_id text NOT NULL DEFAULT ''
        CHECK (from_snapshot_id = '' OR from_snapshot_id ~ '^snap-[a-f0-9]{32}$');

-- Jobs that belong to one artifact generation. This index is deliberately
-- non-unique: done/failed jobs stay as history, so the partial index only
-- accelerates the active-job lookup. At most one queued/running cleanup job
-- per artifact is enforced transactionally by the advisory lock plus EXISTS
-- check in enqueueSnapshotDeleteTx (internal/db/snapshots.go).
ALTER TABLE jobs
    ADD COLUMN snapshot_id uuid REFERENCES saved_snapshots (id);
CREATE INDEX jobs_snapshot_active_idx
    ON jobs (snapshot_id, kind)
    WHERE state IN ('queued', 'running');

-- +goose Down
-- Rollback is refused when the old single-row-per-name schema cannot represent
-- the current state without losing immutable history or an in-flight restore.
-- The preflight runs before any mutation and never hard-deletes artifacts:
--   * more than one live (deleted_at IS NULL) generation for one owner+name;
--   * a live retired/purged record whose cleanup state the old schema cannot
--     represent;
--   * a restore pin flag/counter (rolling back would silently forget it);
--   * a queued/running snapshot save or cleanup job (its handler contract is
--     removed by this rollback).
-- A representable schema (at most one live generation per name, no pins, no
-- active snapshot jobs) rolls back in place; historical soft-deleted rows are
-- preserved.
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM saved_snapshots
        WHERE deleted_at IS NULL
        GROUP BY owner_id, name
        HAVING count(*) > 1
    ) THEN
        RAISE EXCEPTION 'cannot roll back saved-snapshot generations: multiple live generations per name would lose history';
    END IF;
    IF EXISTS (
        SELECT 1 FROM saved_snapshots
        WHERE deleted_at IS NULL AND (retired_at IS NOT NULL OR purged_at IS NOT NULL)
    ) THEN
        RAISE EXCEPTION 'cannot roll back saved-snapshot generations: a live retired or purged record would lose cleanup state';
    END IF;
    IF EXISTS (SELECT 1 FROM sandboxes WHERE snapshot_pin)
       OR EXISTS (SELECT 1 FROM saved_snapshots WHERE pins > 0) THEN
        RAISE EXCEPTION 'cannot roll back saved-snapshot generations: a restore pin is still active';
    END IF;
    IF EXISTS (
        SELECT 1 FROM jobs
        WHERE kind IN ('snapshot.save', 'snapshot.delete')
          AND state IN ('queued', 'running')
    ) THEN
        RAISE EXCEPTION 'cannot roll back saved-snapshot generations: a snapshot job is still active';
    END IF;
END $$;
-- +goose StatementEnd

DROP INDEX jobs_snapshot_active_idx;
ALTER TABLE jobs DROP COLUMN snapshot_id;

ALTER TABLE sandboxes
    DROP COLUMN from_snapshot_id,
    DROP COLUMN snapshot_pin,
    DROP COLUMN snapshot_id;

DROP INDEX saved_snapshots_purge_idx;
DROP INDEX saved_snapshots_owner_current_list_idx;
DROP INDEX saved_snapshots_artifact_id_uk;
DROP INDEX saved_snapshots_owner_name_current_uk;

CREATE INDEX saved_snapshots_artifact_idx ON saved_snapshots (artifact_id);
CREATE INDEX saved_snapshots_owner_list_idx
    ON saved_snapshots (owner_id, created_at DESC, id DESC)
    WHERE deleted_at IS NULL;
CREATE UNIQUE INDEX saved_snapshots_owner_name_active_uk
    ON saved_snapshots (owner_id, name)
    WHERE deleted_at IS NULL;

ALTER TABLE saved_snapshots
    DROP COLUMN cleanup_error,
    DROP COLUMN pins,
    DROP COLUMN purged_at,
    DROP COLUMN retired_at,
    DROP COLUMN ready_at,
    DROP COLUMN is_current,
    DROP COLUMN disk_gib,
    DROP COLUMN node_id;
