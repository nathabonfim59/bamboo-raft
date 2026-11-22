-- +goose Up
-- Multi-node placement and sandboxes created from an object-store generation.

-- from_manifest_key is the generation manifest (snapformat.ManifestKey) the
-- sandbox's first boot restores its disk from; empty for a fresh disk or a
-- node-local saved-snapshot restore.
ALTER TABLE sandboxes
    ADD COLUMN from_manifest_key text NOT NULL DEFAULT '';

-- A node that answered a manifest restore or a snapshot with
-- FAILED_PRECONDITION while it had the manifest's image has no usable object
-- store: placement for manifest restores skips it until a later SnapshotVM or
-- manifest restore on it succeeds. objstore_checked_at is the last change.
ALTER TABLE nodes
    ADD COLUMN objstore_ok         boolean NOT NULL DEFAULT true,
    ADD COLUMN objstore_checked_at timestamptz;

-- A batch create (count) stores every sandbox it created, in creation order,
-- on the idempotency key of its first request, so a replay returns the whole
-- batch. NULL for a single create.
ALTER TABLE idempotency_keys
    ADD COLUMN batch_sandbox_ids text[];

-- +goose Down
-- A sandbox restored from a manifest cannot be described by the older
-- schema: refuse rather than silently turning it into a fresh-disk sandbox.
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM sandboxes WHERE from_manifest_key <> '') THEN
        RAISE EXCEPTION 'cannot roll back phase 2 placement: sandboxes restored from a manifest exist';
    END IF;
END $$;
-- +goose StatementEnd

ALTER TABLE idempotency_keys DROP COLUMN batch_sandbox_ids;
ALTER TABLE nodes
    DROP COLUMN objstore_checked_at,
    DROP COLUMN objstore_ok;
ALTER TABLE sandboxes DROP COLUMN from_manifest_key;
