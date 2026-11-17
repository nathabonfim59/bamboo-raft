-- Schema v0 for Phase 0.5: accounts, api_keys, nodes,
-- sandboxes, sandbox_events, saved_snapshots, jobs, create idempotency and
-- deletion operations. Every owner-bearing row carries owner_id explicitly.

-- +goose Up
CREATE TABLE accounts (
    id         uuid PRIMARY KEY,
    name       text NOT NULL UNIQUE,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- API keys are stored hashed: key_sha256 is the 32-byte SHA-256 of the raw
-- key and the only unique credential identity. prefix is a display
-- identifier several keys may share. Raw keys never reach this table.
CREATE TABLE api_keys (
    id           uuid PRIMARY KEY,
    account_id   uuid NOT NULL REFERENCES accounts (id) ON DELETE CASCADE,
    name         text NOT NULL DEFAULT '',
    prefix       text NOT NULL DEFAULT '',
    key_sha256   bytea NOT NULL UNIQUE CHECK (length(key_sha256) = 32),
    created_at   timestamptz NOT NULL DEFAULT now(),
    last_used_at timestamptz,
    revoked_at   timestamptz
);
CREATE INDEX api_keys_account_idx ON api_keys (account_id);

-- Node inventory written by the nodehub role. Reserved capacity is the sum
-- of live sandbox reservations; totals come from node Info.
CREATE TABLE nodes (
    id                  text PRIMARY KEY,
    cpu_model           text NOT NULL DEFAULT '',
    qemu_version        text NOT NULL DEFAULT '',
    default_image       text NOT NULL DEFAULT '',
    total_vcpus         integer NOT NULL DEFAULT 0,
    total_memory_mib    bigint NOT NULL DEFAULT 0,
    total_disk_gib      bigint NOT NULL DEFAULT 0,
    reserved_vcpus      integer NOT NULL DEFAULT 0,
    reserved_memory_mib bigint NOT NULL DEFAULT 0,
    reserved_disk_gib   bigint NOT NULL DEFAULT 0,
    last_seen_at        timestamptz NOT NULL DEFAULT now(),
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now(),
    CHECK (reserved_vcpus >= 0 AND reserved_memory_mib >= 0 AND reserved_disk_gib >= 0)
);

-- state values are the API Sandbox.state enum; Phase 0.5 uses the subset
-- provisioning/ready/error/cancelled. deleted_at marks an accepted deletion:
-- such rows are gone from every owner-visible list and lookup. env_sealed is
-- AES-256-GCM ciphertext (nonce || ct, AAD binds owner+id+field); plaintext
-- env never reaches this table and normal reads never decrypt it.
CREATE TABLE sandboxes (
    id                  text PRIMARY KEY,
    owner_id            uuid NOT NULL REFERENCES accounts (id),
    state               text NOT NULL CHECK (state IN (
                            'init', 'provisioning', 'provisioned', 'cloning',
                            'ready', 'idle', 'running', 'archiving', 'archived',
                            'error', 'cancelled')),
    machine_type        text NOT NULL DEFAULT 'default',
    name                text NOT NULL DEFAULT '',
    labels              jsonb NOT NULL DEFAULT '{}'::jsonb,
    env_sealed          bytea,
    node_id             text REFERENCES nodes (id),
    reserved_vcpus      integer NOT NULL DEFAULT 0,
    reserved_memory_mib bigint NOT NULL DEFAULT 0,
    reserved_disk_gib   bigint NOT NULL DEFAULT 0,
    version             integer NOT NULL DEFAULT 1 CHECK (version >= 1),
    expires_at          timestamptz,
    error_reason        text NOT NULL DEFAULT '',
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now(),
    deleted_at          timestamptz
);
CREATE INDEX sandboxes_owner_list_idx
    ON sandboxes (owner_id, created_at DESC, id DESC)
    WHERE deleted_at IS NULL;
CREATE INDEX sandboxes_owner_name_idx
    ON sandboxes (owner_id, name)
    WHERE deleted_at IS NULL;
CREATE INDEX sandboxes_expiry_idx
    ON sandboxes (expires_at)
    WHERE deleted_at IS NULL AND expires_at IS NOT NULL;
CREATE INDEX sandboxes_node_idx ON sandboxes (node_id) WHERE deleted_at IS NULL;

CREATE TABLE sandbox_events (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    sandbox_id  text NOT NULL REFERENCES sandboxes (id) ON DELETE CASCADE,
    owner_id    uuid NOT NULL REFERENCES accounts (id),
    type        text NOT NULL,
    data        jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX sandbox_events_sandbox_idx ON sandbox_events (sandbox_id, id);

-- Named saved snapshots. A row is one artifact generation of an owner+name:
-- re-saving bumps generation, and a late job can only complete the
-- generation it was enqueued for. deleted_at removes the name; artifacts of
-- deleted names stay until durable cleanup (B2). artifact_id is
-- server-generated: "snap-" plus 32 lowercase hex digits (37 characters),
-- which the object-store safe filename rule accepts as a subset.
CREATE TABLE saved_snapshots (
    id           uuid PRIMARY KEY,
    owner_id     uuid NOT NULL REFERENCES accounts (id),
    name         text NOT NULL,
    sandbox_id   text NOT NULL REFERENCES sandboxes (id),
    status       text NOT NULL CHECK (status IN ('saving', 'ready', 'failed')),
    artifact_id  text NOT NULL CHECK (artifact_id ~ '^snap-[a-f0-9]{32}$'),
    generation   integer NOT NULL DEFAULT 1 CHECK (generation >= 1),
    machine_type text NOT NULL DEFAULT '',
    size_bytes   bigint,
    error_reason text NOT NULL DEFAULT '',
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now(),
    deleted_at   timestamptz
);
CREATE UNIQUE INDEX saved_snapshots_owner_name_active_uk
    ON saved_snapshots (owner_id, name)
    WHERE deleted_at IS NULL;
CREATE INDEX saved_snapshots_owner_list_idx
    ON saved_snapshots (owner_id, created_at DESC, id DESC)
    WHERE deleted_at IS NULL;
CREATE INDEX saved_snapshots_artifact_idx ON saved_snapshots (artifact_id);

-- Durable work queue. A job is claimed with FOR UPDATE SKIP LOCKED, a
-- lease and a fresh lease_token; every mutation of a running job must
-- present its token before the lease expires, so a stale attempt (crash,
-- expiry, reclaimed or reused worker name) can never complete or requeue
-- work. An expired lease is reclaimable until attempts reaches
-- max_attempts, after which FailStaleJobs marks it failed.
CREATE TABLE jobs (
    id           uuid PRIMARY KEY,
    kind         text NOT NULL,
    owner_id     uuid NOT NULL REFERENCES accounts (id),
    sandbox_id   text REFERENCES sandboxes (id),
    payload      jsonb NOT NULL DEFAULT '{}'::jsonb,
    state        text NOT NULL DEFAULT 'queued'
                 CHECK (state IN ('queued', 'running', 'done', 'failed', 'cancelled')),
    attempts     integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    max_attempts integer NOT NULL DEFAULT 5 CHECK (max_attempts >= 1),
    run_after    timestamptz NOT NULL DEFAULT now(),
    leased_by    text NOT NULL DEFAULT '',
    leased_until timestamptz,
    lease_token  uuid,
    last_error   text NOT NULL DEFAULT '',
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX jobs_ready_idx ON jobs (run_after, id) WHERE state = 'queued';
CREATE INDEX jobs_lease_idx ON jobs (leased_until) WHERE state = 'running';
CREATE INDEX jobs_sandbox_idx ON jobs (sandbox_id);

-- Exactly-once sandbox creation: the row is written in the same transaction
-- as the sandbox, so a lost response replays instead of minting a second
-- billable sandbox. Keys are kept 24 hours.
CREATE TABLE idempotency_keys (
    owner_id       uuid NOT NULL REFERENCES accounts (id),
    key            text NOT NULL,
    request_sha256 bytea NOT NULL,
    sandbox_id     text NOT NULL REFERENCES sandboxes (id),
    response       jsonb,
    created_at     timestamptz NOT NULL DEFAULT now(),
    expires_at     timestamptz NOT NULL,
    PRIMARY KEY (owner_id, key)
);
CREATE INDEX idempotency_expiry_idx ON idempotency_keys (expires_at);

-- Accepted sandbox/snapshot deletions (API DeletionOperation). One active
-- operation per target: the unique partial index makes acceptance idempotent
-- while completed operations stay as owner-visible terminal records.
CREATE TABLE deletion_operations (
    id            text PRIMARY KEY CHECK (id ~ '^bdop_[a-f0-9]{32}$'),
    owner_id      uuid NOT NULL REFERENCES accounts (id),
    kind          text NOT NULL CHECK (kind IN ('sandbox', 'snapshot')),
    target_id     text NOT NULL,
    reason        text NOT NULL CHECK (reason IN ('explicit', 'zdr', 'account')),
    status        text NOT NULL CHECK (status IN ('pending', 'processing', 'blocked', 'completed')),
    stage         text CHECK (stage IN ('removing', 'waiting_for_uploads', 'retrying', 'completed')),
    expected_by   timestamptz,
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    requested_at  timestamptz NOT NULL DEFAULT now(),
    completed_at  timestamptz,
    updated_at    timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX deletion_operations_active_uk
    ON deletion_operations (target_id)
    WHERE status <> 'completed';
CREATE INDEX deletion_operations_owner_idx
    ON deletion_operations (owner_id, requested_at DESC);

-- +goose Down
DROP TABLE deletion_operations;
DROP TABLE idempotency_keys;
DROP TABLE jobs;
DROP TABLE saved_snapshots;
DROP TABLE sandbox_events;
DROP TABLE sandboxes;
DROP TABLE nodes;
DROP TABLE api_keys;
DROP TABLE accounts;
