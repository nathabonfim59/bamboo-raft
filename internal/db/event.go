package db

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"uuid"
)

// EventStatePrefix builds the event type recorded for a state move:
// "state.ready" for a move to ready.
const EventStatePrefix = "state."

// EventSandboxHydrated is recorded when a sandbox restored from an
// object-store manifest has its whole disk local.
const EventSandboxHydrated = "sandbox.hydrated"

// EventInput is one lifecycle event to record with a write.
type EventInput struct {
	Type string
	Data string // JSON object; "" means {}
}

// SandboxEvent is one recorded lifecycle event.
type SandboxEvent struct {
	ID        int64
	SandboxID string
	OwnerID   uuid.UUID
	Type      string
	Data      string // JSON object
	CreatedAt time.Time
}

// AppendEvent records one event for a live owner-scoped sandbox. The owner
// predicate lives inside the write itself: one INSERT ... SELECT from the
// owner-visible sandbox row, so nothing appends to a foreign or deleted
// sandbox. Writes that already run inside a Store operation get their event
// for free; this method covers agent or operator events outside those paths.
func (s *Store) AppendEvent(ctx context.Context, owner uuid.UUID, sandboxID string, e EventInput) error {
	if e.Data == "" {
		e.Data = "{}"
	}
	tag, err := s.pool.Exec(ctx, `
		INSERT INTO sandbox_events (sandbox_id, owner_id, type, data)
		SELECT s.id, s.owner_id, $3, $4::jsonb
		FROM sandboxes s
		WHERE s.id = $1 AND s.owner_id = $2 AND s.deleted_at IS NULL`,
		sandboxID, owner, e.Type, e.Data)
	if err != nil {
		return fmtError("append event", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// insertEventTx writes one event row inside an open transaction.
func insertEventTx(ctx context.Context, tx pgx.Tx, sbx Sandbox, e EventInput) error {
	if e.Data == "" {
		e.Data = "{}"
	}
	_, err := tx.Exec(ctx,
		`INSERT INTO sandbox_events (sandbox_id, owner_id, type, data) VALUES ($1, $2, $3, $4::jsonb)`,
		sbx.ID, sbx.OwnerID, e.Type, e.Data)
	if err != nil {
		return fmtError("insert event", err)
	}
	return nil
}

// SandboxEvents lists a live owner-scoped sandbox's events oldest first, up
// to limit, joining the owner predicate into the query itself.
func (s *Store) SandboxEvents(ctx context.Context, owner uuid.UUID, sandboxID string, limit int) ([]SandboxEvent, error) {
	limit = clamp(limit, 1, 1000, 100)
	rows, err := s.pool.Query(ctx, `
		SELECT e.id, e.sandbox_id, e.owner_id, e.type, e.data, e.created_at
		FROM sandbox_events e
		JOIN sandboxes s ON s.id = e.sandbox_id
		WHERE e.sandbox_id = $1 AND s.owner_id = $2 AND s.deleted_at IS NULL
		ORDER BY e.id LIMIT $3`,
		sandboxID, owner, limit)
	if err != nil {
		return nil, fmtError("list events", err)
	}
	defer rows.Close()
	var out []SandboxEvent
	var n int
	for rows.Next() {
		var (
			e    SandboxEvent
			data []byte
		)
		if err := rows.Scan(&e.ID, &e.SandboxID, &e.OwnerID, &e.Type, &data, &e.CreatedAt); err != nil {
			return nil, fmtError("list events scan", err)
		}
		e.Data = string(data)
		out = append(out, e)
		n++
	}
	if err := rows.Err(); err != nil {
		return nil, fmtError("list events", err)
	}
	if n == 0 {
		// Distinguish "no such live sandbox" from "no events yet".
		if _, err := s.Sandbox(ctx, owner, sandboxID); err != nil {
			return nil, err
		}
	}
	return out, nil
}
