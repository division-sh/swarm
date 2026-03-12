package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"empireai/internal/events"
	"github.com/google/uuid"
)

func (s *PostgresStore) AppendEvent(ctx context.Context, evt events.Event) error {
	return s.AppendEventTx(ctx, nil, evt)
}

func (s *PostgresStore) BeginEventTx(ctx context.Context) (*sql.Tx, error) {
	return s.DB.BeginTx(ctx, nil)
}

func (s *PostgresStore) AppendEventTx(ctx context.Context, tx *sql.Tx, evt events.Event) error {
	id := evt.ID
	if id == "" {
		id = uuid.NewString()
	}
	taskID := sanitizeOptionalUUID(evt.TaskID)
	verticalID := sanitizeOptionalUUID(evt.EntityID())
	createdAt := evt.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now()
	}

	const q = `
		INSERT INTO events (id, type, source_agent, task_id, vertical_id, payload, created_at)
		VALUES ($1::uuid, $2, $3, NULLIF($4,'')::uuid, NULLIF($5,'')::uuid, $6, $7)
		ON CONFLICT (id) DO NOTHING
	`
	execFn := s.DB.ExecContext
	if tx != nil {
		execFn = tx.ExecContext
	}
	_, err := execFn(ctx, q, id, string(evt.Type), evt.SourceAgent, taskID, verticalID, evt.Payload, createdAt)
	if err != nil {
		return fmt.Errorf("append event: %w", err)
	}
	return nil
}

func (s *PostgresStore) PersistEventWithDeliveries(ctx context.Context, evt events.Event, agentIDs []string) error {
	id := evt.ID
	if id == "" {
		id = uuid.NewString()
	}
	taskID := sanitizeOptionalUUID(evt.TaskID)
	verticalID := sanitizeOptionalUUID(evt.EntityID())
	createdAt := evt.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now()
	}

	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin event tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	const insertEvent = `
		INSERT INTO events (id, type, source_agent, task_id, vertical_id, payload, created_at)
		VALUES ($1::uuid, $2, $3, NULLIF($4,'')::uuid, NULLIF($5,'')::uuid, $6, $7)
		ON CONFLICT (id) DO NOTHING
	`
	if _, err := tx.ExecContext(ctx, insertEvent, id, string(evt.Type), evt.SourceAgent, taskID, verticalID, evt.Payload, createdAt); err != nil {
		return fmt.Errorf("append event: %w", err)
	}

	const insertDelivery = `
		INSERT INTO event_deliveries (event_id, agent_id, created_at)
		VALUES ($1::uuid, $2, now())
		ON CONFLICT (event_id, agent_id) DO NOTHING
	`
	for _, agentID := range agentIDs {
		agentID = strings.TrimSpace(agentID)
		if agentID == "" {
			continue
		}
		if _, err := tx.ExecContext(ctx, insertDelivery, id, agentID); err != nil {
			return fmt.Errorf("insert event delivery (agent=%s): %w", agentID, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit event tx: %w", err)
	}
	return nil
}

func sanitizeOptionalUUID(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if _, err := uuid.Parse(raw); err != nil {
		return ""
	}
	return raw
}

func (s *PostgresStore) InsertEventDeliveries(ctx context.Context, eventID string, agentIDs []string) error {
	return s.InsertEventDeliveriesTx(ctx, nil, eventID, agentIDs)
}

func (s *PostgresStore) InsertEventDeliveriesTx(ctx context.Context, tx *sql.Tx, eventID string, agentIDs []string) error {
	if len(agentIDs) == 0 {
		return nil
	}
	const q = `
		INSERT INTO event_deliveries (event_id, agent_id, created_at)
		VALUES ($1::uuid, $2, now())
		ON CONFLICT (event_id, agent_id) DO NOTHING
	`
	execFn := s.DB.ExecContext
	if tx != nil {
		execFn = tx.ExecContext
	}
	for _, agentID := range agentIDs {
		if _, err := execFn(ctx, q, eventID, agentID); err != nil {
			return fmt.Errorf("insert event delivery (agent=%s): %w", agentID, err)
		}
	}
	return nil
}

func (s *PostgresStore) UpsertPipelineReceipt(ctx context.Context, eventID, status, errText string) error {
	return s.UpsertPipelineReceiptTx(ctx, nil, eventID, status, errText)
}

func (s *PostgresStore) UpsertPipelineReceiptTx(ctx context.Context, tx *sql.Tx, eventID, status, errText string) error {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return nil
	}
	status = strings.TrimSpace(strings.ToLower(status))
	if status == "" {
		status = "processed"
	}
	if strings.TrimSpace(errText) != "" && status == "processed" {
		status = "error"
	}
	const q = `
		INSERT INTO pipeline_receipts (event_id, status, error, processed_at)
		VALUES ($1::uuid, $2, NULLIF($3,''), now())
		ON CONFLICT (event_id) DO UPDATE SET
			status = EXCLUDED.status,
			error = EXCLUDED.error,
			processed_at = now()
	`
	execFn := s.DB.ExecContext
	if tx != nil {
		execFn = tx.ExecContext
	}
	if _, err := execFn(ctx, q, eventID, status, strings.TrimSpace(errText)); err != nil {
		if isMissingPipelineReceiptsTable(err) {
			return nil
		}
		// Backward-compatible fallback for legacy schema versions that still
		// use pipeline_receipts.result instead of status/error.
		if isLegacyPipelineReceiptsColumns(err) {
			const legacyQ = `
				INSERT INTO pipeline_receipts (event_id, result, processed_at)
				VALUES ($1::uuid, $2, now())
				ON CONFLICT (event_id) DO UPDATE SET
					result = EXCLUDED.result,
					processed_at = now()
			`
			if _, legacyErr := execFn(ctx, legacyQ, eventID, status); legacyErr == nil {
				return nil
			}
		}
		return fmt.Errorf("upsert pipeline receipt: %w", err)
	}
	return nil
}

func (s *PostgresStore) ListEventsMissingPipelineReceipt(ctx context.Context, since time.Time, limit int) ([]events.Event, error) {
	if limit <= 0 {
		limit = 200
	}
	if since.IsZero() {
		since = time.Now().Add(-24 * time.Hour)
	}
	rows, err := s.DB.QueryContext(ctx, `
		SELECT
			e.id::text, e.type, e.source_agent,
			COALESCE(e.task_id::text, ''),
			COALESCE(e.vertical_id::text, ''),
			e.payload, e.created_at
		FROM events e
		LEFT JOIN pipeline_receipts pr ON pr.event_id = e.id
		WHERE pr.event_id IS NULL
		  AND e.created_at >= $1
		ORDER BY e.created_at ASC
		LIMIT $2
	`, since, limit)
	if err != nil {
		if isMissingPipelineReceiptsTable(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("list events missing pipeline receipt: %w", err)
	}
	defer rows.Close()

	out := make([]events.Event, 0, limit)
	for rows.Next() {
		var evt events.Event
		var legacyVerticalID string
		if err := rows.Scan(
			&evt.ID,
			&evt.Type,
			&evt.SourceAgent,
			&evt.TaskID,
			&legacyVerticalID,
			&evt.Payload,
			&evt.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan missing pipeline receipt event: %w", err)
		}
		evt = evt.WithEntityID(legacyVerticalID)
		out = append(out, evt)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read missing pipeline receipt events: %w", err)
	}
	return out, nil
}

func isMissingPipelineReceiptsTable(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "does not exist") && strings.Contains(msg, "pipeline_receipts")
}

func isLegacyPipelineReceiptsColumns(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	if !strings.Contains(msg, "pipeline_receipts") {
		return false
	}
	return strings.Contains(msg, "column") && (strings.Contains(msg, "status") || strings.Contains(msg, "error"))
}

func (s *PostgresStore) EventExists(ctx context.Context, eventID string) (bool, error) {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return false, nil
	}
	var exists bool
	if err := s.DB.QueryRowContext(ctx, `
		SELECT EXISTS(SELECT 1 FROM events WHERE id = $1::uuid)
	`, eventID).Scan(&exists); err != nil {
		return false, fmt.Errorf("event exists lookup: %w", err)
	}
	return exists, nil
}
