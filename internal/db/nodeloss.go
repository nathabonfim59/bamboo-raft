package db

// Node loss. A node whose last connected inventory observation (last_up_at)
// is older than the grace, while at least one other node was seen within it,
// is lost: the lifecycle role moves what it held to where the object store
// lets it continue. Each sandbox is decided in its own transaction that
// locks the sandbox row, rechecks that it still sits on the lost node in the
// state the caller decided on, then locks the lost node's row and rechecks
// that the node is still lost. Two lifecycle workers deciding the same
// sandbox therefore serialize on its row, and the second finds it moved.
//
// Every outcome releases the sandbox's reservation on the lost node and
// records the node's copy in vm_evictions, so a returning node has the copy
// deleted by VM reconciliation before anything else looks at it, and
// capacity accounting never counts it meanwhile.

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"uuid"
)

// EvictionReasonNodeLost is the reason of an eviction written when the
// sandbox's node was lost.
const EvictionReasonNodeLost = "node_lost"

// NodeLostReason is the sandbox error reason and event reason of node loss.
const NodeLostReason = "node_lost"

// Events of node loss. No event carries a node id.
const (
	// EventSandboxMoved is recorded when a running sandbox is re-placed on
	// another node from its last generation; data {"reason": "node_lost"}.
	EventSandboxMoved = "sandbox.moved"
	// EventSandboxDiskEvicted is recorded when a stopped sandbox's local
	// disk is given up and only the object store holds its data; data
	// {"reason": ...}.
	EventSandboxDiskEvicted = "sandbox.disk_evicted"
)

// ErrNodeNotLost means the node came back (or never was lost) by the time
// the node-loss transaction looked: nothing changed.
var ErrNodeNotLost = errors.New("db: node is not lost")

// LostNodes lists the nodes whose last connected observation is before
// cutoff, by id, while at least one other node was observed connected at or
// after cutoff. Without such a node the observer itself (the inventory
// poller) may be what is down, so nothing is reported lost.
func (s *Store) LostNodes(ctx context.Context, cutoff time.Time) ([]string, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT n.id FROM nodes n
		WHERE n.last_up_at < $1
		  AND EXISTS (SELECT 1 FROM nodes o WHERE o.id <> n.id AND o.last_up_at >= $1)
		ORDER BY n.id`, cutoff)
	if err != nil {
		return nil, fmtError("list lost nodes", err)
	}
	out, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, fmtError("list lost nodes", err)
	}
	return out, nil
}

// SandboxesOnLostNode lists the live sandboxes node loss may act on for
// nodeID, by id: ready-ish or archiving with node_id = nodeID, and archived
// ones whose disk_node_id is nodeID. Every one is rechecked under its row
// lock before anything changes.
func (s *Store) SandboxesOnLostNode(ctx context.Context, nodeID string) ([]Sandbox, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT `+sandboxCols+` FROM sandboxes
		WHERE deleted_at IS NULL
		  AND ((node_id = $1 AND state IN ('ready', 'idle', 'running', 'archiving'))
		    OR (disk_node_id = $1 AND state = 'archived'))
		ORDER BY id`, nodeID)
	if err != nil {
		return nil, fmtError("list sandboxes on lost node", err)
	}
	defer rows.Close()
	var out []Sandbox
	for rows.Next() {
		sbx, err := scanSandbox(rows)
		if err != nil {
			return nil, fmtError("scan sandbox on lost node", err)
		}
		out = append(out, sbx)
	}
	if err := rows.Err(); err != nil {
		return nil, fmtError("list sandboxes on lost node", err)
	}
	return out, nil
}

// NodeLossParams names one sandbox of a lost node and the facts the caller
// decided on. LostBefore is the loss cutoff: the node is still lost while its
// last_up_at is before it and another node's is not. Version is the row
// version the caller read (0 skips the check).
type NodeLossParams struct {
	Owner      uuid.UUID
	SandboxID  string
	LostNode   string
	LostBefore time.Time
	Version    int64
}

// NodeLossMove is where a moved sandbox goes: the placed node and the
// completed generation it restores from.
type NodeLossMove struct {
	ToNode       string
	GenerationID uuid.UUID
	ManifestKey  string
}

// validate rejects a call without owner, sandbox, node or cutoff.
func (p NodeLossParams) validate() error {
	if p.Owner == uuid.Nil() {
		return ErrNoOwner
	}
	if p.SandboxID == "" || p.LostNode == "" || p.LostBefore.IsZero() {
		return errors.New("db: node loss needs a sandbox, a node and a cutoff")
	}
	return nil
}

// lockLostSandboxTx locks the sandbox, checks the expected version, then
// locks the lost node's row and checks the node is still lost. The sandbox
// row is locked first, like every other sandbox-then-node transaction.
func lockLostSandboxTx(ctx context.Context, tx pgx.Tx, p NodeLossParams) (Sandbox, error) {
	sbx, err := sandboxRowForUpdateTx(ctx, tx, p.Owner, p.SandboxID)
	if err != nil {
		return Sandbox{}, err
	}
	if p.Version != 0 && sbx.Version != p.Version {
		return Sandbox{}, ErrConflict
	}
	var upAt time.Time
	err = tx.QueryRow(ctx, `SELECT last_up_at FROM nodes WHERE id = $1 FOR UPDATE`,
		p.LostNode).Scan(&upAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Sandbox{}, ErrNodeNotLost
	}
	if err != nil {
		return Sandbox{}, fmtError("lock lost node", err)
	}
	if !upAt.Before(p.LostBefore) {
		return Sandbox{}, ErrNodeNotLost
	}
	var observed bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM nodes WHERE id <> $1 AND last_up_at >= $2)`,
		p.LostNode, p.LostBefore).Scan(&observed); err != nil {
		return Sandbox{}, fmtError("node loss observer check", err)
	}
	if !observed {
		return Sandbox{}, ErrNodeNotLost
	}
	return sbx, nil
}

// hasCompletedGenerationTx reports whether the sandbox's chain has a
// completed, unpurged generation.
func hasCompletedGenerationTx(ctx context.Context, tx pgx.Tx, sbx Sandbox) (bool, error) {
	var ok bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM snapshots
			WHERE chain_id = $1 AND sandbox_id = $2 AND status = 'completed' AND purged_at IS NULL)`,
		sbx.ChainID, sbx.ID).Scan(&ok); err != nil {
		return false, fmtError("completed generation check", err)
	}
	return ok, nil
}

// MoveLostSandbox re-places a ready-ish sandbox of a lost node on
// m.ToNode from its completed generation m.GenerationID. In one transaction
// it rechecks the sandbox and the node (see [NodeLossParams]), locks the
// generation FOR SHARE (it must be a completed, unpurged generation of the
// sandbox's chain with that manifest), grows the disk to the generation's
// disk, releases the reservation on the lost node, reserves the shape on
// ToNode, moves the sandbox to provisioning on ToNode with a fresh lifecycle
// generation and no local disk, records the lost node's copy as an eviction,
// enqueues a [JobResume] that restores the manifest there (source generation
// recorded) and records the state event and [EventSandboxMoved].
//
// A deleted or foreign sandbox answers [ErrNotFound]; a newer version, a
// sandbox no longer on the lost node or not ready-ish, or a ToNode that is
// the lost node or holds an open eviction of the sandbox [ErrConflict]; a
// node that is back [ErrNodeNotLost]; snapshots off [ErrSnapshotsDisabled];
// expired data [ErrSnapshotDataExpired]; an unusable generation
// [ErrSnapshotNotFound]; a full ToNode [ErrNoCapacity]; an unknown one
// [ErrNodeUnavailable].
func (s *Store) MoveLostSandbox(ctx context.Context, p NodeLossParams, m NodeLossMove) (Sandbox, Job, error) {
	if err := p.validate(); err != nil {
		return Sandbox{}, Job{}, err
	}
	if m.ToNode == "" || m.ManifestKey == "" || m.GenerationID == uuid.Nil() {
		return Sandbox{}, Job{}, errors.New("db: node loss move needs a node and a generation")
	}
	var (
		out Sandbox
		job Job
	)
	err := s.runTx(ctx, "move lost sandbox", func(ctx context.Context, tx pgx.Tx) error {
		sbx, err := lockLostSandboxTx(ctx, tx, p)
		if err != nil {
			return err
		}
		switch {
		case derefString(sbx.NodeID) != p.LostNode || !readyishState(sbx.State):
			return ErrConflict
		case !sbx.SnapshotsEnabled:
			return ErrSnapshotsDisabled
		case sbx.DataExpiredAt != nil:
			return ErrSnapshotDataExpired
		case m.ToNode == p.LostNode:
			return ErrConflict
		}
		image, diskBytes, err := lockResumeGenerationTx(ctx, tx, ResumeElsewhereParams{
			ResumeParams: ResumeParams{Owner: p.Owner, SandboxID: p.SandboxID},
			GenerationID: m.GenerationID,
			ManifestKey:  m.ManifestKey,
		}, sbx.ChainID)
		if err != nil {
			return err
		}
		if open, err := openEvictionTx(ctx, tx, m.ToNode, sbx.ID); err != nil {
			return err
		} else if open {
			return ErrConflict
		}
		desired := sbx.Desired
		desired.DiskGiB = max(desired.DiskGiB, DiskGiBFor(diskBytes))

		if err := releaseNodeTx(ctx, tx, sbx.NodeID, sbx.Reserved); err != nil {
			return err
		}
		if err := reserveResumeCapacityTx(ctx, tx, m.ToNode, desired); err != nil {
			return err
		}
		out, err = scanSandbox(tx.QueryRow(ctx, `
			UPDATE sandboxes SET
				state = 'provisioning', version = version + 1,
				lifecycle_generation = lifecycle_generation + 1,
				desired_disk_gib = $3,
				reserved_vcpus = $4, reserved_memory_mib = $5, reserved_disk_gib = $3,
				node_id = $6, disk_node_id = NULL,
				error_reason = '', ssh_endpoint = NULL, ssh_host_key = NULL,
				updated_at = now()
			WHERE id = $1 AND version = $2
			RETURNING `+sandboxCols,
			sbx.ID, sbx.Version, desired.DiskGiB, desired.VCPUs, desired.MemoryMiB, m.ToNode))
		if err != nil {
			return fmtError("move lost sandbox transition", err)
		}
		if err := insertEvictionTx(ctx, tx, p.LostNode, sbx.ID, EvictionReasonNodeLost); err != nil {
			return err
		}
		payload, err := marshalJobPayload(ResumePayload{
			NodeID:       m.ToNode,
			VMID:         out.ID,
			MachineType:  out.MachineType,
			Resources:    out.Desired,
			Generation:   out.LifecycleGeneration,
			FromManifest: m.ManifestKey,
			Image:        image,
			EvictNode:    p.LostNode,
		})
		if err != nil {
			return err
		}
		job, err = enqueueJobTx(ctx, tx, JobInput{
			Kind:               JobResume,
			OwnerID:            p.Owner,
			SandboxID:          &p.SandboxID,
			Payload:            payload,
			SourceGenerationID: new(m.GenerationID),
		})
		if err != nil {
			return err
		}
		if err := insertEventTx(ctx, tx, out, EventInput{Type: EventStatePrefix + string(StateProvisioning)}); err != nil {
			return err
		}
		return insertEventTx(ctx, tx, out, EventInput{
			Type: EventSandboxMoved,
			Data: mustJSON(map[string]any{"reason": NodeLostReason}),
		})
	})
	if err != nil {
		return Sandbox{}, Job{}, err
	}
	return out, job, nil
}

// FailLostSandbox ends a ready-ish sandbox of a lost node that has nothing
// to restore from (snapshots off, or no completed generation yet): in one
// transaction it rechecks the sandbox and the node, releases its whole
// reservation, moves it to error with reason [NodeLostReason], clears
// node_id and disk_node_id (the copy on the lost node is no longer the
// sandbox's VM) and records that copy as an eviction.
//
// Errors are those of [Store.MoveLostSandbox]; a sandbox with snapshots on
// that has a completed generation answers [ErrConflict]: it is moved, not
// failed.
func (s *Store) FailLostSandbox(ctx context.Context, p NodeLossParams) (Sandbox, error) {
	if err := p.validate(); err != nil {
		return Sandbox{}, err
	}
	var out Sandbox
	err := s.runTx(ctx, "fail lost sandbox", func(ctx context.Context, tx pgx.Tx) error {
		sbx, err := lockLostSandboxTx(ctx, tx, p)
		if err != nil {
			return err
		}
		if derefString(sbx.NodeID) != p.LostNode || !readyishState(sbx.State) {
			return ErrConflict
		}
		if sbx.SnapshotsEnabled && sbx.DataExpiredAt == nil {
			if ok, err := hasCompletedGenerationTx(ctx, tx, sbx); err != nil {
				return err
			} else if ok {
				return ErrConflict
			}
		}
		out, err = detachLostTx(ctx, tx, sbx, StateError, NodeLostReason, p.LostNode)
		if err != nil {
			return err
		}
		return insertEventTx(ctx, tx, out, EventInput{
			Type: EventStatePrefix + string(StateError),
			Data: mustJSON(map[string]any{"reason": NodeLostReason}),
		})
	})
	if err != nil {
		return Sandbox{}, err
	}
	return out, nil
}

// EvictLostDisk gives up the local disk of an archived sandbox kept on a
// lost node when the object store holds a completed generation of it: in one
// transaction it rechecks the sandbox (archived, disk_node_id = the lost
// node) and the node, releases its reservation, clears node_id and
// disk_node_id, records the copy as an eviction and records
// [EventSandboxDiskEvicted]. A later resume restores from the store on
// another node.
//
// Errors are those of [Store.MoveLostSandbox]; without a completed
// generation it answers [ErrNoRestorableData] (the disk is the only copy and
// stays where it is).
func (s *Store) EvictLostDisk(ctx context.Context, p NodeLossParams) (Sandbox, error) {
	if err := p.validate(); err != nil {
		return Sandbox{}, err
	}
	var out Sandbox
	err := s.runTx(ctx, "evict lost disk", func(ctx context.Context, tx pgx.Tx) error {
		sbx, err := lockLostSandboxTx(ctx, tx, p)
		if err != nil {
			return err
		}
		if sbx.State != StateArchived || derefString(sbx.DiskNodeID) != p.LostNode {
			return ErrConflict
		}
		if ok, err := hasCompletedGenerationTx(ctx, tx, sbx); err != nil {
			return err
		} else if !ok || sbx.DataExpiredAt != nil {
			return ErrNoRestorableData
		}
		out, err = detachLostTx(ctx, tx, sbx, StateArchived, sbx.ErrorReason, p.LostNode)
		if err != nil {
			return err
		}
		return insertEventTx(ctx, tx, out, EventInput{
			Type: EventSandboxDiskEvicted,
			Data: mustJSON(map[string]any{"reason": NodeLostReason}),
		})
	})
	if err != nil {
		return Sandbox{}, err
	}
	return out, nil
}

// FinishLostStop finalizes a stop in flight on a lost node when the object
// store holds a completed generation: in one transaction it rechecks the
// sandbox (archiving on the lost node, its current stop operation pending or
// failing) and the node, releases the whole reservation, moves the sandbox to
// archived with no node and no local disk, completes the operation, records
// the copy as an eviction and records stop.completed with reason
// [NodeLostReason]. Writes made after that generation are lost. The queued
// stop job later finds the operation completed and ends.
//
// Errors are those of [Store.MoveLostSandbox]; a superseded or completed
// operation [ErrConflict]; without a completed generation
// [ErrNoRestorableData] (the stop keeps failing until the node returns).
func (s *Store) FinishLostStop(ctx context.Context, p NodeLossParams) (Sandbox, StopOperation, error) {
	if err := p.validate(); err != nil {
		return Sandbox{}, StopOperation{}, err
	}
	var (
		out Sandbox
		op  StopOperation
	)
	err := s.runTx(ctx, "finish lost stop", func(ctx context.Context, tx pgx.Tx) error {
		sbx, err := lockLostSandboxTx(ctx, tx, p)
		if err != nil {
			return err
		}
		if sbx.State != StateArchiving || derefString(sbx.NodeID) != p.LostNode || sbx.StopOperationID == nil {
			return ErrConflict
		}
		op, err = stopForUpdateTx(ctx, tx, p.Owner, p.SandboxID, *sbx.StopOperationID)
		if err != nil {
			return err
		}
		if op.Status != StopPending && op.Status != StopFailing {
			return ErrConflict
		}
		if ok, err := hasCompletedGenerationTx(ctx, tx, sbx); err != nil {
			return err
		} else if !ok || !sbx.SnapshotsEnabled || sbx.DataExpiredAt != nil {
			return ErrNoRestorableData
		}
		out, err = detachLostTx(ctx, tx, sbx, StateArchived, "", p.LostNode)
		if err != nil {
			return err
		}
		op, err = completeStopTx(ctx, tx, p.Owner, p.SandboxID, op.ID)
		if err != nil {
			return err
		}
		data := stopCompletedData(op, false)
		data["reason"] = NodeLostReason
		return insertEventTx(ctx, tx, out, EventInput{Type: "stop.completed", Data: mustJSON(data)})
	})
	if err != nil {
		return Sandbox{}, StopOperation{}, err
	}
	return out, op, nil
}

// detachLostTx is the common write of the outcomes that leave nothing on any
// node: release the locked sandbox's reservation, set state and reason, clear
// node_id, disk_node_id and the reservation counters, and record the lost
// node's copy as an eviction. A sandbox that was archiving gets its stopped
// time now, as a completed stop does.
func detachLostTx(ctx context.Context, tx pgx.Tx, sbx Sandbox, to SandboxState, reason, lostNode string) (Sandbox, error) {
	if err := releaseNodeTx(ctx, tx, sbx.NodeID, sbx.Reserved); err != nil {
		return Sandbox{}, err
	}
	out, err := scanSandbox(tx.QueryRow(ctx, `
		UPDATE sandboxes SET
			state = $3, version = version + 1, error_reason = $4,
			node_id = NULL, disk_node_id = NULL,
			reserved_vcpus = 0, reserved_memory_mib = 0, reserved_disk_gib = 0,
			stopped_at = CASE WHEN $5 THEN now() ELSE stopped_at END,
			ssh_endpoint = NULL, ssh_host_key = NULL, updated_at = now()
		WHERE id = $1 AND version = $2
		RETURNING `+sandboxCols, sbx.ID, sbx.Version, string(to), reason, sbx.State == StateArchiving))
	if err != nil {
		return Sandbox{}, fmtError("detach lost sandbox", err)
	}
	if err := insertEvictionTx(ctx, tx, lostNode, sbx.ID, EvictionReasonNodeLost); err != nil {
		return Sandbox{}, err
	}
	return out, nil
}

// readyishState lists the states in which a sandbox should have a running VM.
func readyishState(state SandboxState) bool {
	switch state {
	case StateReady, StateIdle, StateRunning:
		return true
	default:
		return false
	}
}
