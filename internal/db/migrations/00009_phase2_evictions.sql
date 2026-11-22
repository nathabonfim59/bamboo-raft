-- +goose Up
-- VM evictions: a node's local copy of a sandbox that the control plane no
-- longer uses (the sandbox resumed on another node, its node was lost, its
-- data expired or the node ran low on disk). The copy is deleted with DeleteVM
-- on that node as soon as it is reachable; VM reconciliation deletes the VMs
-- of open rows on a returning node before it looks at anything else in that
-- node's inventory, so an evicted copy is never reported as unknown, started
-- or snapshotted. done_at is set once DeleteVM confirmed. The sandbox's
-- reservation on the node was released when the row was written, so capacity
-- accounting ignores the evicted copy meanwhile.
--
-- reason is a short fixed code written by the server (resumed_elsewhere,
-- node_lost, ...), never node text.
CREATE TABLE vm_evictions (
    id         uuid PRIMARY KEY,
    node_id    text NOT NULL,
    sandbox_id text NOT NULL REFERENCES sandboxes (id),
    reason     text NOT NULL CHECK (reason <> ''),
    created_at timestamptz NOT NULL DEFAULT now(),
    done_at    timestamptz
);

-- At most one open eviction per copy; placement and resume admission refuse
-- a node with an open eviction for the same sandbox, so the sandbox never
-- returns to a node before its stale copy is gone.
CREATE UNIQUE INDEX vm_evictions_open_uk
    ON vm_evictions (node_id, sandbox_id)
    WHERE done_at IS NULL;

-- Reconciliation walks one node's open rows.
CREATE INDEX vm_evictions_node_open_idx
    ON vm_evictions (node_id, created_at, id)
    WHERE done_at IS NULL;

-- +goose Down
-- An open eviction is a VM copy that still has to be deleted: dropping the
-- table would turn it into an unknown VM that reconciliation preserves.
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM vm_evictions WHERE done_at IS NULL) THEN
        RAISE EXCEPTION 'cannot roll back phase 2 evictions: open evictions exist';
    END IF;
END $$;
-- +goose StatementEnd

DROP TABLE vm_evictions;
