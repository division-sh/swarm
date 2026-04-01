package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/lib/pq"
)

func (s *PostgresStore) CancelActiveTraceWorkByProducer(ctx context.Context, producerID string) ([]string, error) {
	producerID = strings.TrimSpace(producerID)
	if producerID == "" {
		return nil, nil
	}

	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin cancel traces tx: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	traceRows, err := tx.QueryContext(ctx, `
		SELECT DISTINCT e.trace_id
		FROM events e
		WHERE COALESCE(e.trace_id, '') <> ''
		  AND COALESCE(e.produced_by, '') = $1
		  AND e.event_name NOT LIKE 'platform.%'
		  AND EXISTS (
			SELECT 1
			FROM events te
			JOIN event_deliveries d ON d.event_id = te.event_id
			WHERE te.trace_id = e.trace_id
			  AND d.status IN ('pending', 'in_progress')
		  )
		ORDER BY e.trace_id ASC
	`, producerID)
	if err != nil {
		return nil, fmt.Errorf("query active traces for producer %s: %w", producerID, err)
	}
	defer traceRows.Close()

	traceIDs := make([]string, 0, 8)
	for traceRows.Next() {
		var traceID string
		if err := traceRows.Scan(&traceID); err != nil {
			return nil, fmt.Errorf("scan active trace id: %w", err)
		}
		traceID = strings.TrimSpace(traceID)
		if traceID != "" {
			traceIDs = append(traceIDs, traceID)
		}
	}
	if err := traceRows.Err(); err != nil {
		return nil, fmt.Errorf("read active trace ids: %w", err)
	}
	if len(traceIDs) == 0 {
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("commit empty cancel traces tx: %w", err)
		}
		committed = true
		return nil, nil
	}

	agentRows, err := tx.QueryContext(ctx, `
		SELECT DISTINCT d.subscriber_id
		FROM event_deliveries d
		JOIN events e ON e.event_id = d.event_id
		WHERE e.trace_id = ANY($1::text[])
		  AND d.subscriber_type = 'agent'
		  AND d.status IN ('pending', 'in_progress')
		ORDER BY d.subscriber_id ASC
	`, pq.Array(traceIDs))
	if err != nil {
		return nil, fmt.Errorf("query affected agents for traces: %w", err)
	}
	defer agentRows.Close()

	affectedAgents := make([]string, 0, 16)
	for agentRows.Next() {
		var agentID string
		if err := agentRows.Scan(&agentID); err != nil {
			return nil, fmt.Errorf("scan affected agent: %w", err)
		}
		agentID = strings.TrimSpace(agentID)
		if agentID != "" {
			affectedAgents = append(affectedAgents, agentID)
		}
	}
	if err := agentRows.Err(); err != nil {
		return nil, fmt.Errorf("read affected agents: %w", err)
	}

	sideEffects, err := json.Marshal(map[string]any{
		"manager_status": "dead_letter",
		"reason_code":    "cancelled_by_kill_previous",
		"retry_count":    0,
		"error":          "cancelled by --kill-previous",
	})
	if err != nil {
		return nil, fmt.Errorf("marshal kill_previous receipt side effects: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO event_receipts (
			event_id, subscriber_type, subscriber_id, entity_id, flow_instance,
			outcome, reason_code, side_effects, processed_at
		)
		SELECT
			e.event_id,
			d.subscriber_type,
			d.subscriber_id,
			e.entity_id,
			e.flow_instance,
			'dead_letter',
			'cancelled_by_kill_previous',
			$2::jsonb,
			now()
		FROM event_deliveries d
		JOIN events e ON e.event_id = d.event_id
		WHERE e.trace_id = ANY($1::text[])
		  AND d.status IN ('pending', 'in_progress')
		ON CONFLICT (event_id, subscriber_id) DO UPDATE SET
			entity_id = EXCLUDED.entity_id,
			flow_instance = EXCLUDED.flow_instance,
			outcome = EXCLUDED.outcome,
			reason_code = EXCLUDED.reason_code,
			side_effects = EXCLUDED.side_effects,
			processed_at = now()
	`, pq.Array(traceIDs), string(sideEffects)); err != nil {
		return nil, fmt.Errorf("upsert kill_previous receipts: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `
		UPDATE event_deliveries d
		SET
			status = 'dead_letter',
			reason_code = 'cancelled_by_kill_previous',
			last_error = 'cancelled by --kill-previous',
			active_session_id = NULL,
			delivered_at = now()
		FROM events e
		WHERE e.event_id = d.event_id
		  AND e.trace_id = ANY($1::text[])
		  AND d.status IN ('pending', 'in_progress')
	`, pq.Array(traceIDs)); err != nil {
		return nil, fmt.Errorf("mark kill_previous deliveries dead_letter: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit cancel traces tx: %w", err)
	}
	committed = true
	return affectedAgents, nil
}
