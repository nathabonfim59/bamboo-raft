-- +goose Up
-- The owner of a pending generation attempt. A row records the lifecycle job
-- and the lease token that are cutting it, so a later attempt can tell a live
-- snapshot from one whose worker crashed and take the row over instead of
-- waiting for the dead attempt's deadline to pass. Both are NULL on rows
-- written before this migration and on completed or failed rows.

ALTER TABLE snapshots
    ADD COLUMN job_id      uuid REFERENCES jobs (id),
    ADD COLUMN lease_token uuid;

-- +goose Down
-- Both columns are nullable and carry no data of their own: dropping them
-- loses only the takeover evidence, which the deadline-based reaper still
-- covers.
ALTER TABLE snapshots
    DROP COLUMN lease_token,
    DROP COLUMN job_id;
