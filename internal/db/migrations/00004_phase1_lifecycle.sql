-- +goose Up
-- Phase 1 lifecycle (B1.1): stopped sandboxes keep their disk on the same
-- node while freeing vCPU/memory, resume needs the machine shape independent
-- of the live reservation, and stop/retention decisions need durable,
-- owner-scoped records.

-- desired_* is the machine shape the sandbox was created, resumed or resized
-- to. reserved_* stays the live node reservation: on stop the vCPU/memory
-- part is released while the disk part is retained, so deletion later frees
-- exactly what remains and never twice.
ALTER TABLE sandboxes
    ADD COLUMN desired_vcpus      integer NOT NULL DEFAULT 0,
    ADD COLUMN desired_memory_mib bigint NOT NULL DEFAULT 0,
    ADD COLUMN desired_disk_gib   bigint NOT NULL DEFAULT 0,
    -- ttl_seconds is the configured auto-stop duration, kept so a resume can
    -- reset the deadline without the caller resending it. NULL means no
    -- configured duration (unlimited or a legacy absolute expiry).
    ADD COLUMN ttl_seconds        integer,
    ADD COLUMN subdomain          text,
    ADD COLUMN stopped_at         timestamptz,
    ADD COLUMN stop_operation_id  text,
    -- lifecycle_generation distinguishes boot attempts from row versions:
    -- every accepted resume bumps it, and resume/provision workers may only
    -- act on the attempt whose generation they were enqueued for. Version
    -- keeps changing on every PATCH and transition.
    ADD COLUMN lifecycle_generation bigint NOT NULL DEFAULT 0
        CHECK (lifecycle_generation >= 0);

ALTER TABLE sandboxes
    ADD CONSTRAINT sandboxes_desired_resources_check
        CHECK (desired_vcpus >= 0 AND desired_memory_mib >= 0 AND desired_disk_gib >= 0);

ALTER TABLE sandboxes
    ADD CONSTRAINT sandboxes_ttl_seconds_check
        CHECK (ttl_seconds IS NULL OR (ttl_seconds BETWEEN 1 AND 2592000)),
    ADD CONSTRAINT sandboxes_subdomain_check
        CHECK (subdomain IS NULL OR (
            length(subdomain) BETWEEN 3 AND 40
            AND subdomain ~ '^[a-z0-9]([a-z0-9-]*[a-z0-9])?$'
            AND subdomain !~ '--'
            AND subdomain !~ '-desktop$'
            AND subdomain !~ '-[0-9]+$'));

-- Existing rows had no separate desired shape; their reservation is it.
UPDATE sandboxes
SET desired_vcpus      = reserved_vcpus,
    desired_memory_mib = reserved_memory_mib,
    desired_disk_gib   = reserved_disk_gib;

-- Existing finite TTLs keep auto-stop across a resume: recover the configured
-- duration from the legacy absolute deadline when it is an exact, valid
-- integral number of seconds in range. Fractional, zero, negative or
-- out-of-range deadlines stay NULL (no configured duration) rather than
-- guessing or silently disabling auto-stop for representable values.
UPDATE sandboxes
SET ttl_seconds = EXTRACT(EPOCH FROM (expires_at - created_at))::bigint
WHERE expires_at IS NOT NULL
  AND EXTRACT(EPOCH FROM (expires_at - created_at)) = trunc(EXTRACT(EPOCH FROM (expires_at - created_at)))
  AND EXTRACT(EPOCH FROM (expires_at - created_at)) BETWEEN 1 AND 2592000;

-- A subdomain is globally unique among live sandboxes: two owners can never
-- claim the same public label, and deleting a sandbox releases it.
CREATE UNIQUE INDEX sandboxes_subdomain_live_uk
    ON sandboxes (subdomain)
    WHERE subdomain IS NOT NULL AND deleted_at IS NULL;

-- The stopped-retention reaper walks archived rows by their stopped time.
CREATE INDEX sandboxes_retention_idx
    ON sandboxes (stopped_at, id)
    WHERE deleted_at IS NULL AND state = 'archived' AND stopped_at IS NOT NULL;

-- One durable stop request. status follows the pinned StopOperation enum:
-- pending (accepted, archiving), failing (last attempt failed, retried
-- automatically), completed (archived), superseded (a resume ended it).
-- requested_at is kept even after the stop ends; the API hides it on the
-- sandbox once ended. At most one pending/failing operation exists per
-- sandbox, which makes acceptance idempotent.
CREATE TABLE stop_operations (
    id              text PRIMARY KEY CHECK (id ~ '^stop_[a-f0-9]{16}$'),
    owner_id        uuid NOT NULL REFERENCES accounts (id),
    sandbox_id      text NOT NULL REFERENCES sandboxes (id),
    status          text NOT NULL CHECK (status IN (
                        'pending', 'failing', 'completed', 'superseded')),
    requested_at    timestamptz NOT NULL DEFAULT now(),
    last_attempt_at timestamptz,
    error           text NOT NULL DEFAULT '',
    ended_at        timestamptz,
    -- forced is the historical fact that force ended THIS stop; force_requested
    -- is the live escalation flag. A repeated force stop sets both the
    -- canonical row and its queued job, and workers reread the row so a stale
    -- payload can never hide the escalation.
    forced          boolean NOT NULL DEFAULT false,
    force_requested boolean NOT NULL DEFAULT false,
    node_id         text NOT NULL DEFAULT '',
    updated_at      timestamptz NOT NULL DEFAULT now(),
    CHECK ((status IN ('completed', 'superseded')) = (ended_at IS NOT NULL)),
    CHECK (NOT forced OR status = 'completed')
);
CREATE UNIQUE INDEX stop_operations_active_uk
    ON stop_operations (sandbox_id)
    WHERE status IN ('pending', 'failing');
CREATE INDEX stop_operations_owner_idx
    ON stop_operations (owner_id, requested_at DESC, id);

-- The sandbox remembers its latest stop even after it ended, so reads can
-- expose the pinned sandbox.stop metadata without scanning the table.
ALTER TABLE sandboxes
    ADD CONSTRAINT sandboxes_stop_operation_id_fk
        FOREIGN KEY (stop_operation_id) REFERENCES stop_operations (id);

-- +goose Down
-- Rolling back keeps every Phase 0.5-representable row intact: a legacy or
-- backfilled ttl_seconds only loses resume configuration that Phase 0.5 does
-- not have, and deleted/error rows keep no live behaviour from desired_*.
-- Refuse only actual Phase 1 lifecycle state that the old schema cannot
-- represent, rather than blocking the existing populated snapshot rollbacks.
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM stop_operations) THEN
        RAISE EXCEPTION 'cannot roll back phase 1 lifecycle: stop operations exist';
    END IF;
    IF EXISTS (SELECT 1 FROM sandboxes WHERE lifecycle_generation > 0) THEN
        RAISE EXCEPTION 'cannot roll back phase 1 lifecycle: a resume generation exists';
    END IF;
    IF EXISTS (
        SELECT 1 FROM sandboxes
        WHERE deleted_at IS NULL
          AND (subdomain IS NOT NULL OR state IN ('archiving', 'archived'))
    ) THEN
        RAISE EXCEPTION 'cannot roll back phase 1 lifecycle: a live stopped or named sandbox exists';
    END IF;
    IF EXISTS (
        SELECT 1 FROM sandboxes
        WHERE deleted_at IS NULL
          AND stopped_at IS NOT NULL
          AND state NOT IN ('error', 'cancelled')
    ) THEN
        RAISE EXCEPTION 'cannot roll back phase 1 lifecycle: a live sandbox carries stop metadata';
    END IF;
END $$;
-- +goose StatementEnd

ALTER TABLE sandboxes
    DROP CONSTRAINT sandboxes_desired_resources_check,
    DROP CONSTRAINT sandboxes_stop_operation_id_fk;
DROP TABLE stop_operations;

DROP INDEX sandboxes_retention_idx;
DROP INDEX sandboxes_subdomain_live_uk;
ALTER TABLE sandboxes
    DROP CONSTRAINT sandboxes_subdomain_check,
    DROP CONSTRAINT sandboxes_ttl_seconds_check;

ALTER TABLE sandboxes
    DROP COLUMN lifecycle_generation,
    DROP COLUMN stop_operation_id,
    DROP COLUMN stopped_at,
    DROP COLUMN subdomain,
    DROP COLUMN ttl_seconds,
    DROP COLUMN desired_disk_gib,
    DROP COLUMN desired_memory_mib,
    DROP COLUMN desired_vcpus;
