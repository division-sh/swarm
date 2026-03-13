package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"empireai/internal/events"
	runtimemanager "empireai/internal/runtime/manager"
)

func (s *PostgresStore) UpsertEventReceipt(ctx context.Context, eventID, agentID, status, errText string) error {
	if eventID == "" || agentID == "" {
		return nil
	}
	if status == "" {
		status = "processed"
	}

	const q = `
		INSERT INTO event_receipts (event_id, agent_id, processed_at, status, retry_count, error)
		VALUES ($1::uuid, $2, now(), $3, CASE WHEN $3 = 'error' THEN 1 ELSE 0 END, NULLIF($4,''))
		ON CONFLICT (event_id, agent_id) DO UPDATE SET
			processed_at = now(),
			status = CASE
				-- v2.0: allow 3 retries (1m, 5m, 30m) after the initial attempt.
				-- We store retry_count as the number of failures seen so far; dead-letter after the 4th failure.
				WHEN EXCLUDED.status = 'error' AND event_receipts.retry_count + 1 >= 4 THEN 'dead_letter'
				ELSE EXCLUDED.status
			END,
			error = EXCLUDED.error,
			retry_count = CASE
				WHEN EXCLUDED.status = 'error' THEN event_receipts.retry_count + 1
				ELSE event_receipts.retry_count
			END
	`
	if _, err := s.DB.ExecContext(ctx, q, eventID, agentID, status, errText); err != nil {
		return fmt.Errorf("upsert event receipt: %w", err)
	}
	return nil
}

func (s *PostgresStore) ListPendingEventsForAgent(ctx context.Context, agentID string, since time.Time, limit int) ([]events.Event, error) {
	if agentID == "" {
		return nil, nil
	}
	if limit <= 0 {
		limit = 200
	}
	if since.IsZero() {
		since = time.Now().Add(-30 * 24 * time.Hour)
	}

	const q = `
		SELECT
			e.id::text, e.type, e.source_agent,
			COALESCE(e.task_id::text, ''),
			COALESCE(e.entity_id::text, ''),
			e.payload, e.created_at
		FROM event_deliveries d
		INNER JOIN events e ON e.id = d.event_id
		LEFT JOIN event_receipts r
			ON r.event_id = d.event_id
			AND r.agent_id = d.agent_id
		WHERE d.agent_id = $1
		  AND e.created_at >= $2
		  AND (
				r.event_id IS NULL
				OR (
					r.status = 'error'
					AND r.retry_count <= 3
					AND (
						(r.retry_count = 1 AND r.processed_at <= now() - interval '1 minute')
						OR
						(r.retry_count = 2 AND r.processed_at <= now() - interval '5 minute')
						OR
						(r.retry_count = 3 AND r.processed_at <= now() - interval '30 minute')
					)
				)
			)
		ORDER BY e.created_at ASC
		LIMIT $3
	`
	rows, err := s.DB.QueryContext(ctx, q, agentID, since, limit)
	if err != nil {
		return nil, fmt.Errorf("query pending events for %s: %w", agentID, err)
	}
	defer rows.Close()

	out := make([]events.Event, 0)
	for rows.Next() {
		var evt events.Event
		var legacyEntityID string
		if err := rows.Scan(
			&evt.ID,
			&evt.Type,
			&evt.SourceAgent,
			&evt.TaskID,
			&legacyEntityID,
			&evt.Payload,
			&evt.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan pending event: %w", err)
		}
		evt = evt.WithEntityID(legacyEntityID)
		out = append(out, evt)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read pending events rows: %w", err)
	}
	return out, nil
}

func (s *PostgresStore) ListPendingSubscribedEvents(
	ctx context.Context,
	agentID string,
	subscriptions []events.EventType,
	since time.Time,
	limit int,
) ([]events.Event, error) {
	if agentID == "" || len(subscriptions) == 0 {
		return nil, nil
	}
	if limit <= 0 {
		limit = 500
	}
	if since.IsZero() {
		since = time.Now().Add(-30 * 24 * time.Hour)
	}

	const q = `
		SELECT
			e.id::text, e.type, e.source_agent,
			COALESCE(e.task_id::text, ''),
			COALESCE(e.entity_id::text, ''),
			e.payload, e.created_at
		FROM events e
		LEFT JOIN event_receipts r
			ON r.event_id = e.id
			AND r.agent_id = $1
		WHERE e.created_at >= $2
		  AND (
				NOT EXISTS (
					SELECT 1
					FROM event_deliveries d_any
					WHERE d_any.event_id = e.id
				)
				OR EXISTS (
					SELECT 1
					FROM event_deliveries d_me
					WHERE d_me.event_id = e.id
					  AND d_me.agent_id = $1
				)
			)
		  AND (
				r.event_id IS NULL
				OR (
					r.status = 'error'
					AND r.retry_count <= 3
					AND (
						(r.retry_count = 1 AND r.processed_at <= now() - interval '1 minute')
						OR
						(r.retry_count = 2 AND r.processed_at <= now() - interval '5 minute')
						OR
						(r.retry_count = 3 AND r.processed_at <= now() - interval '30 minute')
					)
				)
			)
		ORDER BY e.created_at ASC
		LIMIT $3
	`
	rows, err := s.DB.QueryContext(ctx, q, agentID, since, limit)
	if err != nil {
		return nil, fmt.Errorf("query pending subscribed events for %s: %w", agentID, err)
	}
	defer rows.Close()

	out := make([]events.Event, 0)
	for rows.Next() {
		var evt events.Event
		var legacyEntityID string
		if err := rows.Scan(
			&evt.ID,
			&evt.Type,
			&evt.SourceAgent,
			&evt.TaskID,
			&legacyEntityID,
			&evt.Payload,
			&evt.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan pending subscribed event: %w", err)
		}
		evt = evt.WithEntityID(legacyEntityID)
		if matchesAnySubscription(string(evt.Type), subscriptions) {
			out = append(out, evt)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read pending subscribed events rows: %w", err)
	}
	return out, nil
}

func (s *PostgresStore) GetEventReceipt(ctx context.Context, eventID, agentID string) (runtimemanager.EventReceipt, bool, error) {
	eventID = strings.TrimSpace(eventID)
	agentID = strings.TrimSpace(agentID)
	if eventID == "" || agentID == "" {
		return runtimemanager.EventReceipt{}, false, fmt.Errorf("event_id and agent_id are required")
	}
	var r runtimemanager.EventReceipt
	r.EventID = eventID
	r.AgentID = agentID
	if err := s.DB.QueryRowContext(ctx, `
		SELECT COALESCE(status, 'processed'), COALESCE(retry_count, 0), COALESCE(error, '')
		FROM event_receipts
		WHERE event_id = $1::uuid AND agent_id = $2
	`, eventID, agentID).Scan(&r.Status, &r.RetryCount, &r.Error); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return runtimemanager.EventReceipt{}, false, nil
		}
		return runtimemanager.EventReceipt{}, false, fmt.Errorf("get event receipt: %w", err)
	}
	return r, true, nil
}
