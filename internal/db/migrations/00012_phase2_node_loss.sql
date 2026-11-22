-- +goose Up
-- Node loss. last_seen_at cannot measure how long a node has been away: the
-- inventory writer sets it to the epoch on the first disconnected
-- observation, so placement stops using the node at once. last_up_at is the
-- time of the node's last connected inventory observation and is never
-- touched by that stale marker; the lifecycle role declares a node lost only
-- once last_up_at is older than -lifecycle.node-lost-seconds, so a link blip
-- shorter than that moves nothing. A node already away when this migration
-- runs counts from the migration time.
ALTER TABLE nodes
    ADD COLUMN last_up_at timestamptz NOT NULL DEFAULT now();

UPDATE nodes SET last_up_at = last_seen_at
WHERE last_seen_at > 'epoch'::timestamptz;

-- +goose Down
ALTER TABLE nodes DROP COLUMN last_up_at;
