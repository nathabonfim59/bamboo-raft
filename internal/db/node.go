package db

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"uuid"
)

// Resources is one machine shape: the same numbers machinetype resolves for
// a sandbox size, kept here so the store stays independent of that package.
type Resources struct {
	VCPUs     uint32
	MemoryMiB uint64
	DiskGiB   uint64
}

// Node is the stored inventory of one registered node.
type Node struct {
	ID           string
	CPUModel     string
	QEMUVersion  string
	DefaultImage string
	Total        Resources
	Reserved     Resources
	LastSeenAt   time.Time
	CreatedAt    time.Time
	UpdatedAt    time.Time
	// ObjectStoreOK is false while the node is known to lack a usable object
	// store (it refused a manifest restore with FAILED_PRECONDITION although
	// it had the image); placement for manifest restores skips it.
	// ObjectStoreCheckedAt is when the flag last changed, nil if never.
	ObjectStoreOK        bool
	ObjectStoreCheckedAt *time.Time
}

// Free is the capacity not reserved yet, never negative.
func (n Node) Free() Resources {
	return Resources{
		VCPUs:     n.Total.VCPUs - min(n.Reserved.VCPUs, n.Total.VCPUs),
		MemoryMiB: n.Total.MemoryMiB - min(n.Reserved.MemoryMiB, n.Total.MemoryMiB),
		DiskGiB:   n.Total.DiskGiB - min(n.Reserved.DiskGiB, n.Total.DiskGiB),
	}
}

// NodeInfo is the inventory payload the nodehub role writes from a node's
// Info RPC plus connection freshness.
//
// Capacity.VCPUs and Capacity.MemoryMiB are the node's TOTAL offer after its
// host reserve. Capacity.DiskGiB is not used: disk is reported as
// DiskFreeGiB, the disk still available to NEW sandboxes at report time,
// which the node already nets against its own outstanding disk reservations.
type NodeInfo struct {
	ID           string
	CPUModel     string
	QEMUVersion  string
	DefaultImage string
	Capacity     Resources
	DiskFreeGiB  uint64
	SeenAt       time.Time
}

const nodeCols = `id, cpu_model, qemu_version, default_image,
	total_vcpus, total_memory_mib, total_disk_gib,
	reserved_vcpus, reserved_memory_mib, reserved_disk_gib,
	last_seen_at, created_at, updated_at, objstore_ok, objstore_checked_at`

func scanNode(row scanner) (Node, error) {
	var n Node
	err := row.Scan(&n.ID, &n.CPUModel, &n.QEMUVersion, &n.DefaultImage,
		&n.Total.VCPUs, &n.Total.MemoryMiB, &n.Total.DiskGiB,
		&n.Reserved.VCPUs, &n.Reserved.MemoryMiB, &n.Reserved.DiskGiB,
		&n.LastSeenAt, &n.CreatedAt, &n.UpdatedAt, &n.ObjectStoreOK, &n.ObjectStoreCheckedAt)
	return n, err
}

// UpsertNodeInfo inserts or refreshes a node's inventory. Reservation
// counters are never touched: a node that reports smaller totals keeps its
// live reservations (they belong to running sandboxes and are released only
// by their owners), so accounting never silently frees another VM's room.
//
// vCPUs and memory are stored as the reported totals. Disk is different: the
// node reports free disk already net of its own reservations, so storing it
// as the total would make placement subtract the reservations a second time.
// total_disk_gib is instead set to the reported free plus the reservations
// the row holds at that instant, in this one statement under the row lock, so
// total - reserved equals the reported free at the report baseline. Later
// ReserveCapacity and ReleaseSandboxReservation calls move that difference
// once each. A reservation the node has not seen yet, or a deletion it has
// not finished, can skew the baseline until the next poll; node admission
// remains the authority.
//
// The observation time is also the node's last_up_at, the clock node-loss
// detection reads ([Store.LostNodes]); [Store.TouchNodeSeen] never moves it.
func (s *Store) UpsertNodeInfo(ctx context.Context, info NodeInfo) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO nodes (id, cpu_model, qemu_version, default_image,
			total_vcpus, total_memory_mib, total_disk_gib, last_seen_at, last_up_at, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8, now(), now())
		ON CONFLICT (id) DO UPDATE SET
			cpu_model = EXCLUDED.cpu_model,
			qemu_version = EXCLUDED.qemu_version,
			default_image = EXCLUDED.default_image,
			total_vcpus = EXCLUDED.total_vcpus,
			total_memory_mib = EXCLUDED.total_memory_mib,
			total_disk_gib = EXCLUDED.total_disk_gib + nodes.reserved_disk_gib,
			last_seen_at = EXCLUDED.last_seen_at,
			last_up_at = EXCLUDED.last_seen_at,
			updated_at = now()`,
		info.ID, info.CPUModel, info.QEMUVersion, info.DefaultImage,
		info.Capacity.VCPUs, info.Capacity.MemoryMiB, info.DiskFreeGiB, info.SeenAt)
	if err != nil {
		return fmtError("upsert node info", err)
	}
	return nil
}

// TouchNodeSeen refreshes connection freshness without changing inventory.
// It leaves last_up_at alone: the inventory writer calls it with the epoch to
// mark a disconnected node stale, which must not make a short blip look like
// a node gone for good.
func (s *Store) TouchNodeSeen(ctx context.Context, id string, at time.Time) error {
	tag, err := s.pool.Exec(ctx,
		`UPDATE nodes SET last_seen_at = $2, updated_at = now() WHERE id = $1`, id, at)
	if err != nil {
		return fmtError("touch node seen", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// Node returns one node's inventory.
func (s *Store) Node(ctx context.Context, id string) (Node, error) {
	row := s.pool.QueryRow(ctx, `SELECT `+nodeCols+` FROM nodes WHERE id = $1`, id)
	n, err := scanNode(row)
	if err != nil {
		return Node{}, errNoRows("select node", err)
	}
	return n, nil
}

// NodesWithCapacity lists fresh nodes with at least need free, most free
// vCPUs first. Freshness is last_seen_at within freshWithin; the caller
// combines it with live Hub connectivity.
func (s *Store) NodesWithCapacity(ctx context.Context, need Resources, freshWithin time.Duration) ([]Node, error) {
	cutoff := time.Now().Add(-freshWithin)
	rows, err := s.pool.Query(ctx, `SELECT `+nodeCols+` FROM nodes
		WHERE last_seen_at >= $4
		  AND total_vcpus - reserved_vcpus >= $1
		  AND total_memory_mib - reserved_memory_mib >= $2
		  AND total_disk_gib - reserved_disk_gib >= $3
		ORDER BY total_vcpus - reserved_vcpus DESC, id`,
		need.VCPUs, need.MemoryMiB, need.DiskGiB, cutoff)
	if err != nil {
		return nil, fmtError("list nodes with capacity", err)
	}
	defer rows.Close()
	var nodes []Node
	for rows.Next() {
		n, err := scanNode(rows)
		if err != nil {
			return nil, fmtError("scan node", err)
		}
		nodes = append(nodes, n)
	}
	if err := rows.Err(); err != nil {
		return nil, fmtError("list nodes with capacity", err)
	}
	return nodes, nil
}

// ReserveCapacity atomically adds add to nodeID's reservation counters,
// refusing with [ErrNoCapacity] when the totals would be exceeded. It is
// safe under concurrency: the row update serializes competing reservations.
func (s *Store) ReserveCapacity(ctx context.Context, nodeID string, add Resources) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE nodes SET
			reserved_vcpus = reserved_vcpus + $2,
			reserved_memory_mib = reserved_memory_mib + $3,
			reserved_disk_gib = reserved_disk_gib + $4,
			updated_at = now()
		WHERE id = $1
		  AND reserved_vcpus + $2 <= total_vcpus
		  AND reserved_memory_mib + $3 <= total_memory_mib
		  AND reserved_disk_gib + $4 <= total_disk_gib`,
		nodeID, add.VCPUs, add.MemoryMiB, add.DiskGiB)
	if err != nil {
		return fmtError("reserve capacity", err)
	}
	if tag.RowsAffected() == 0 {
		// Either the node is unknown or it is full; callers treat both
		// as placement failure.
		return ErrNoCapacity
	}
	return nil
}

// ReleaseCapacity subtracts sub from nodeID's reservation counters,
// clamping at zero so a release is never lost to a missing or rebuilt node.
// Prefer [Store.ReleaseSandboxReservation] for sandbox-owned reservations:
// it releases exactly once and cannot double-free.
func (s *Store) ReleaseCapacity(ctx context.Context, nodeID string, sub Resources) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE nodes SET
			reserved_vcpus = GREATEST(reserved_vcpus - $2, 0),
			reserved_memory_mib = GREATEST(reserved_memory_mib - $3, 0),
			reserved_disk_gib = GREATEST(reserved_disk_gib - $4, 0),
			updated_at = now()
		WHERE id = $1`,
		nodeID, sub.VCPUs, sub.MemoryMiB, sub.DiskGiB)
	if err != nil {
		return fmtError("release capacity", err)
	}
	return nil
}

// ReleaseSandboxReservation returns one sandbox's reservation to its node
// exactly once: in a single transaction it locks the sandbox row, subtracts
// the stored reservation counters from the node and zeroes them on the
// sandbox row, so failure-cleanup and repeated job delivery can never
// double-release. The sandbox must exist and belong to owner; a second call
// finds zeroed counters and does nothing. It applies to live and deleted
// sandboxes alike.
func (s *Store) ReleaseSandboxReservation(ctx context.Context, owner uuid.UUID, sandboxID string) error {
	return s.runTx(ctx, "release sandbox reservation", func(ctx context.Context, tx pgx.Tx) error {
		var (
			nodeID     *string
			vcpus, mem int64
			disk       int64
		)
		err := tx.QueryRow(ctx, `
			SELECT node_id, reserved_vcpus, reserved_memory_mib, reserved_disk_gib
			FROM sandboxes WHERE id = $1 AND owner_id = $2 FOR UPDATE`,
			sandboxID, owner).Scan(&nodeID, &vcpus, &mem, &disk)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return fmtError("release reservation select", err)
		}
		if vcpus == 0 && mem == 0 && disk == 0 {
			return nil // already released
		}
		if nodeID != nil {
			if _, err := tx.Exec(ctx, `
				UPDATE nodes SET
					reserved_vcpus = GREATEST(reserved_vcpus - $2, 0),
					reserved_memory_mib = GREATEST(reserved_memory_mib - $3, 0),
					reserved_disk_gib = GREATEST(reserved_disk_gib - $4, 0),
					updated_at = now()
				WHERE id = $1`, *nodeID, vcpus, mem, disk); err != nil {
				return fmtError("release reservation node", err)
			}
		}
		if _, err := tx.Exec(ctx, `
			UPDATE sandboxes SET reserved_vcpus = 0, reserved_memory_mib = 0,
				reserved_disk_gib = 0, updated_at = now()
			WHERE id = $1`, sandboxID); err != nil {
			return fmtError("release reservation clear", err)
		}
		return nil
	})
}

// MarkNodeObjectStore records whether nodeID has a usable object store. A
// manifest restore refused with FAILED_PRECONDITION on a node that has the
// manifest's image marks it false; a later successful SnapshotVM or manifest
// restore marks it true again. objstore_checked_at moves only when the flag
// changes, so the success path can call this on every snapshot cheaply. An
// unknown node answers [ErrNotFound].
func (s *Store) MarkNodeObjectStore(ctx context.Context, nodeID string, ok bool) error {
	var exists bool
	err := s.pool.QueryRow(ctx, `
		WITH changed AS (
			UPDATE nodes SET objstore_ok = $2, objstore_checked_at = now(), updated_at = now()
			WHERE id = $1 AND objstore_ok IS DISTINCT FROM $2
			RETURNING id)
		SELECT EXISTS (SELECT 1 FROM changed) OR EXISTS (SELECT 1 FROM nodes WHERE id = $1)`,
		nodeID, ok).Scan(&exists)
	if err != nil {
		return fmtError("mark node object store", err)
	}
	if !exists {
		return ErrNotFound
	}
	return nil
}
