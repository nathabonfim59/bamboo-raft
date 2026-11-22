-- +goose Up
-- Chunk garbage collection.
--
-- restore_source_generation_id is the generation a sandbox's disk was last
-- restored from the object store (create from a generation, fork, resume on
-- another node, node loss). It is written in the same transaction as
-- hydrated_at when the restore reaches READY. The collector keeps that
-- generation's chunks until the sandbox has a completed generation of its
-- own that was cut after the restore: until then the source is the only copy
-- of the restored data in the store.
ALTER TABLE sandboxes
    ADD COLUMN restore_source_generation_id uuid REFERENCES snapshots (id);

CREATE INDEX sandboxes_restore_source_idx
    ON sandboxes (restore_source_generation_id)
    WHERE restore_source_generation_id IS NOT NULL;

-- The liveness queries find a chain's latest completed generation and the
-- rows newer than it.
CREATE INDEX snapshots_chain_status_idx
    ON snapshots (chain_id, status, generation);

-- +goose Down
DROP INDEX snapshots_chain_status_idx;
DROP INDEX sandboxes_restore_source_idx;
ALTER TABLE sandboxes DROP COLUMN restore_source_generation_id;
