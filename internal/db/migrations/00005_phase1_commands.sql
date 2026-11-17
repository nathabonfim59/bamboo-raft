-- +goose Up
-- Phase 1 detached commands (B2.1): durable owner-scoped command records and
-- a per-sandbox process id allocator. Process ids are positive uint32 values,
-- unique per sandbox and never reused, including for failed or ambiguous
-- starts. Output bytes are not stored here; only the node's metadata and the
-- last observed byte counts.

-- One allocator row per sandbox. last_process_id is the id handed out last;
-- raising it to 4294967295 is a successful allocation, and the next attempt
-- refuses instead of wrapping to 1.
CREATE TABLE command_process_counters (
    sandbox_id      text PRIMARY KEY REFERENCES sandboxes (id) ON DELETE CASCADE,
    last_process_id bigint NOT NULL DEFAULT 0
        CHECK (last_process_id BETWEEN 0 AND 4294967295)
);

-- One row per detached command start attempt. The row is committed before
-- StartProcess runs, so an ambiguous start keeps its process id and retries
-- with it. command holds the first 65536 bytes of the request cut on a whole
-- UTF-8 boundary; the full text exists only in the caller that started the
-- process. cwd is the request's optional value; the node owns path validity.
-- starting and running are open states; the rest are final.
CREATE TABLE command_records (
    id               text PRIMARY KEY
        CHECK (id ~ '^cmd_[23456789abcdefghjkmnpqrstuvwxyz]{12}$'),
    owner_id         uuid NOT NULL REFERENCES accounts (id),
    sandbox_id       text NOT NULL REFERENCES sandboxes (id),
    process_id       bigint NOT NULL
        CHECK (process_id BETWEEN 1 AND 4294967295),
    mode             text NOT NULL CHECK (mode = 'detached'),
    command          text NOT NULL CHECK (octet_length(command) <= 65536),
    cwd              text,
    status           text NOT NULL CHECK (status IN (
                         'starting', 'running', 'exited', 'lost', 'killed',
                         'failed', 'timed_out')),
    pid              integer NOT NULL DEFAULT 0 CHECK (pid >= 0),
    exit_code        integer,
    signal           text,
    started_at       timestamptz NOT NULL DEFAULT now(),
    finished_at      timestamptz,
    stdout_bytes     bigint NOT NULL DEFAULT 0 CHECK (stdout_bytes >= 0),
    stderr_bytes     bigint NOT NULL DEFAULT 0 CHECK (stderr_bytes >= 0),
    output_truncated boolean NOT NULL DEFAULT false,
    log_path         text NOT NULL DEFAULT '',
    err_log_path     text NOT NULL DEFAULT '',
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    UNIQUE (sandbox_id, process_id),
    -- starting and running are open and have no finish time. LOST may have an
    -- unknown finish (NULL) because Process.finished_at_unix_ms is 0 for LOST,
    -- and the pinned CommandRecord.finishedAt allows null; the other final
    -- statuses always carry a finish time.
    CHECK (
        (status IN ('starting', 'running') AND finished_at IS NULL)
        OR status = 'lost'
        OR (status IN ('exited', 'killed', 'timed_out', 'failed') AND finished_at IS NOT NULL)
    ),
    CHECK (exit_code IS NULL OR status IN ('exited', 'killed', 'timed_out')),
    CHECK (signal IS NULL OR status IN ('exited', 'killed', 'timed_out'))
);

-- +goose Down
-- A command record or a handed-out process id cannot be represented by the
-- old schema, and reusing an id after a rollback would violate the contract.
-- Refuse instead of silently losing the allocation history.
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM command_records) THEN
        RAISE EXCEPTION 'cannot roll back phase 1 commands: command records exist';
    END IF;
    IF EXISTS (SELECT 1 FROM command_process_counters WHERE last_process_id > 0) THEN
        RAISE EXCEPTION 'cannot roll back phase 1 commands: process ids were allocated';
    END IF;
END $$;
-- +goose StatementEnd

DROP TABLE command_records;
DROP TABLE command_process_counters;
