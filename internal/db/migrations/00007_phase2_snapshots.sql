-- +goose Up
-- Object-store snapshot generations. Every sandbox owns a
-- chain of generations (0 = base, then incrementals); each attempt is one row
-- in snapshots, whose numbers are never reused. The API only ever shows the
-- completed, unpurged rows.

-- chain_id names the sandbox's current chain. The default backfills existing
-- rows (one fresh chain each) and keeps raw inserts valid; CreateSandbox
-- always writes an explicit uuid v7.
-- disk_node_id is the node that holds the sandbox's disk locally; NULL means
-- the disk exists only in the object store.
ALTER TABLE sandboxes
    ADD COLUMN chain_id          uuid NOT NULL DEFAULT gen_random_uuid(),
    ADD COLUMN snapshots_enabled boolean NOT NULL DEFAULT true,
    ADD COLUMN next_snapshot_at  timestamptz,
    ADD COLUMN disk_node_id      text,
    ADD COLUMN hydrated_at       timestamptz,
    ADD COLUMN data_expired_at   timestamptz,
    ADD COLUMN last_recover_at   timestamptz;

-- Phase 1 sandboxes keep their disk on their node while they are live or
-- archived; failed and cancelled rows never had a usable disk worth tracking.
UPDATE sandboxes
SET disk_node_id = node_id
WHERE deleted_at IS NULL
  AND node_id IS NOT NULL
  AND state NOT IN ('error', 'cancelled');

-- The snapshot worker scans ready sandboxes by their due time; NULL (never
-- snapshotted) sorts first.
CREATE INDEX sandboxes_snapshot_due_idx
    ON sandboxes (next_snapshot_at NULLS FIRST, id)
    WHERE deleted_at IS NULL AND snapshots_enabled
      AND state IN ('ready', 'idle', 'running');

-- One row per snapshot attempt of a chain. The size columns are NULL until
-- the node reports the finished generation. manifest_key is the object-store
-- key of the generation's manifest (snapformat.ManifestKey).
CREATE TABLE snapshots (
    id              uuid PRIMARY KEY,
    owner_id        uuid NOT NULL REFERENCES accounts (id),
    sandbox_id      text NOT NULL REFERENCES sandboxes (id),
    chain_id        uuid NOT NULL,
    generation      bigint NOT NULL CHECK (generation BETWEEN 0 AND 4294967295),
    kind            text NOT NULL CHECK (kind IN ('base', 'incremental')),
    status          text NOT NULL CHECK (status IN ('pending', 'completed', 'failed')),
    manifest_key    text NOT NULL,
    node_id         text NOT NULL,
    image           text,
    disk_bytes      bigint CHECK (disk_bytes >= 0),
    chunks          bigint CHECK (chunks >= 0),
    chunks_uploaded bigint CHECK (chunks_uploaded >= 0),
    bytes_uploaded  bigint CHECK (bytes_uploaded >= 0),
    content_bytes   bigint CHECK (content_bytes >= 0),
    frozen          boolean,
    error           text NOT NULL DEFAULT '',
    -- cut_at is the node's creation time of the generation, the order the API
    -- lists by.
    cut_at          timestamptz,
    started_at      timestamptz NOT NULL DEFAULT now(),
    completed_at    timestamptz,
    deadline_at     timestamptz NOT NULL,
    purged_at       timestamptz,
    CONSTRAINT snapshots_chain_generation_uk UNIQUE (chain_id, generation),
    CHECK ((kind = 'base') = (generation = 0)),
    CHECK ((status = 'completed') = (completed_at IS NOT NULL)),
    CHECK (status <> 'completed' OR (
        image IS NOT NULL AND disk_bytes IS NOT NULL AND chunks IS NOT NULL
        AND chunks_uploaded IS NOT NULL AND bytes_uploaded IS NOT NULL
        AND content_bytes IS NOT NULL AND frozen IS NOT NULL AND cut_at IS NOT NULL))
);

-- At most one attempt in flight per sandbox.
CREATE UNIQUE INDEX snapshots_pending_uk
    ON snapshots (sandbox_id)
    WHERE status = 'pending';

-- API listings: the owner's and one sandbox's completed generations, newest
-- first (the same index serves the ascending walk).
CREATE INDEX snapshots_owner_list_idx
    ON snapshots (owner_id, cut_at DESC, id DESC)
    WHERE status = 'completed' AND purged_at IS NULL;
CREATE INDEX snapshots_sandbox_list_idx
    ON snapshots (sandbox_id, cut_at DESC, id DESC)
    WHERE status = 'completed' AND purged_at IS NULL;

-- GC fence and reconciliation walk pending rows by deadline.
CREATE INDEX snapshots_pending_deadline_idx
    ON snapshots (deadline_at, id)
    WHERE status = 'pending';

-- The chunk garbage collector's singleton fence: a snapshot attempt starts
-- only while sweeping is false, and reads the row FOR SHARE so a sweep that
-- flips it waits for attempts that are beginning.
CREATE TABLE gc_state (
    id               boolean PRIMARY KEY DEFAULT true CHECK (id),
    sweeping         boolean NOT NULL DEFAULT false,
    sweep_started_at timestamptz,
    last_mark_at     timestamptz
);
INSERT INTO gc_state (id) VALUES (true);

-- A restore, fork or resume job records the generation it reads from, so the
-- collector keeps that generation's chunks while the job is active.
ALTER TABLE jobs
    ADD COLUMN source_generation_id uuid REFERENCES snapshots (id);
CREATE INDEX jobs_source_generation_idx
    ON jobs (source_generation_id)
    WHERE source_generation_id IS NOT NULL;

-- +goose Down
-- Generations live in the object store and cannot be rebuilt from the Phase 1
-- schema: refuse while any exist rather than orphaning their chunks.
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM snapshots) THEN
        RAISE EXCEPTION 'cannot roll back phase 2 snapshots: generations exist';
    END IF;
    IF EXISTS (SELECT 1 FROM jobs WHERE kind = 'sandbox.snapshot') THEN
        RAISE EXCEPTION 'cannot roll back phase 2 snapshots: snapshot jobs exist';
    END IF;
END $$;
-- +goose StatementEnd

DROP INDEX jobs_source_generation_idx;
ALTER TABLE jobs DROP COLUMN source_generation_id;

DROP TABLE gc_state;
DROP TABLE snapshots;

DROP INDEX sandboxes_snapshot_due_idx;
ALTER TABLE sandboxes
    DROP COLUMN last_recover_at,
    DROP COLUMN data_expired_at,
    DROP COLUMN hydrated_at,
    DROP COLUMN disk_node_id,
    DROP COLUMN next_snapshot_at,
    DROP COLUMN snapshots_enabled,
    DROP COLUMN chain_id;
