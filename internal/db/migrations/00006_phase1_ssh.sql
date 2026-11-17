-- +goose Up
-- Phase 1 SSH access metadata (B4.1): one optional endpoint+host-key pair per
-- sandbox boot. The API stores it only after AddSSHKey succeeded for the same
-- owner, sandbox, node and lifecycle generation. A stop, resume or terminal
-- error clears it in the same fenced state change. Host keys are public; user
-- public keys, private keys, environment values and command output are never
-- stored here.

ALTER TABLE sandboxes
    ADD COLUMN ssh_endpoint text,
    ADD COLUMN ssh_host_key text;

ALTER TABLE sandboxes
    ADD CONSTRAINT sandboxes_ssh_metadata_check CHECK (
        (ssh_endpoint IS NULL AND ssh_host_key IS NULL)
        OR (
            ssh_endpoint IS NOT NULL AND ssh_host_key IS NOT NULL
            AND ssh_endpoint <> ''
            AND ssh_endpoint !~ '[[:space:][:cntrl:]]'
            AND ssh_host_key <> ''
            AND ssh_host_key !~ '[[:cntrl:]]'
        )
    );

-- +goose Down
-- A live sandbox carrying SSH endpoint/host-key metadata would lose
-- connection information clients may have pinned in known_hosts. Refuse that
-- rollback; ordinary populated rows and metadata on soft-deleted rows (which
-- are no longer owner-visible or resumable) do not block it.
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM sandboxes
        WHERE deleted_at IS NULL
          AND (ssh_endpoint IS NOT NULL OR ssh_host_key IS NOT NULL)
    ) THEN
        RAISE EXCEPTION 'cannot roll back phase 1 ssh metadata: live sandbox ssh metadata exists';
    END IF;
END $$;
-- +goose StatementEnd

ALTER TABLE sandboxes
    DROP CONSTRAINT sandboxes_ssh_metadata_check,
    DROP COLUMN ssh_host_key,
    DROP COLUMN ssh_endpoint;
