-- +goose Up
-- Saved snapshots backed by an object-store generation, and share codes.
--
-- A saved_snapshots row is now one of two kinds:
--   * legacy node-local (generation_id NULL, node_id set, artifact_id names
--     the node artifact): restored only on its recorded node until deleted;
--   * generation-backed (generation_id set, node_id NULL): the row points at
--     one completed snapshot generation and restores on any node that has the
--     object store. No node artifact is created for it.
-- artifact_id stays non-null in both kinds: it is the immutable public
-- snapshotId and the unique row identity even when nothing lives on a node.

ALTER TABLE saved_snapshots
    ADD COLUMN generation_id uuid REFERENCES snapshots (id);

-- Chunk GC reads this index to keep the generations saved snapshots pin.
CREATE INDEX saved_snapshots_generation_idx
    ON saved_snapshots (generation_id)
    WHERE generation_id IS NOT NULL;

-- Share codes let any account create a sandbox from one saved-snapshot name.
-- Only the SHA-256 of the code is stored; the code itself is shown once in the
-- create response and never again. A share deploys the name's current
-- generation, so re-saving the name changes what the code deploys, and
-- deleting the name revokes its codes.
CREATE TABLE snapshot_shares (
    id           text PRIMARY KEY
        CHECK (id ~ '^shr_[a-f0-9]{32}$'),
    owner_id     uuid NOT NULL REFERENCES accounts (id),
    name         text NOT NULL,
    code_sha256  bytea NOT NULL UNIQUE CHECK (length(code_sha256) = 32),
    code_prefix  text NOT NULL,
    status       text NOT NULL DEFAULT 'active'
        CHECK (status IN ('active', 'revoked')),
    use_count    bigint NOT NULL DEFAULT 0 CHECK (use_count >= 0),
    last_used_at timestamptz,
    revoked_at   timestamptz,
    created_at   timestamptz NOT NULL DEFAULT now(),
    CHECK ((status = 'revoked') = (revoked_at IS NOT NULL))
);

-- Listing one name's codes newest first.
CREATE INDEX snapshot_shares_owner_name_idx
    ON snapshot_shares (owner_id, name, created_at DESC, id DESC);

-- +goose Down
-- Generation-backed saves and share codes cannot be represented by the older
-- schema without silently turning them into node-local snapshots or orphaning
-- codes. Refuse while any exist.
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM saved_snapshots WHERE generation_id IS NOT NULL) THEN
        RAISE EXCEPTION 'cannot roll back phase 2 saved snapshots: generation-backed rows exist';
    END IF;
    IF EXISTS (SELECT 1 FROM snapshot_shares) THEN
        RAISE EXCEPTION 'cannot roll back phase 2 saved snapshots: share codes exist';
    END IF;
END $$;
-- +goose StatementEnd

DROP INDEX snapshot_shares_owner_name_idx;
DROP TABLE snapshot_shares;

DROP INDEX saved_snapshots_generation_idx;
ALTER TABLE saved_snapshots DROP COLUMN generation_id;
