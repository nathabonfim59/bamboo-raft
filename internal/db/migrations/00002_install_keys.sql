-- Install secrets: one data key wrapped by the master key. The single-row
-- shape (id = 1) makes concurrent bootstraps resolve to one durable winner;
-- a wrong master key is detected by hint and never replaces the stored key.

-- +goose Up
CREATE TABLE install_keys (
    id          integer PRIMARY KEY CHECK (id = 1),
    wrapped_key bytea NOT NULL CHECK (length(wrapped_key) = 60),
    key_hint    text NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now()
);

-- +goose Down
DROP TABLE install_keys;
