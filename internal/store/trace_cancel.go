package store

import (
	"context"
	"fmt"
	"strings"

	"github.com/lib/pq"
)

func (s *PostgresStore) CancelActiveRunWorkByProducer(ctx context.Context, producerID string) ([]string, error) {
	producerID = strings.TrimSpace(producerID)
	if producerID == "" {
		return nil, nil
	}

	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin cancel runs tx: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	runRows, err := tx.QueryContext(ctx, `
		SELECT DISTINCT e.run_id::text
		FROM events e
		WHERE e.run_id IS NOT NULL
		  AND COALESCE(e.produced_by, '') = $1
		  AND e.event_name NOT LIKE 'platform.%'
		  AND EXISTS (
			SELECT 1
			FROM events re
			JOIN event_deliveries d ON d.event_id = re.event_id
			WHERE re.run_id = e.run_id
			  AND d.status IN ('pending', 'in_progress')
		  )
		ORDER BY e.run_id::text ASC
	`, producerID)
	if err != nil {
		return nil, fmt.Errorf("query active runs for producer %s: %w", producerID, err)
	}
	defer runRows.Close()

	runIDs := make([]string, 0, 8)
	for runRows.Next() {
		var runID string
		if err := runRows.Scan(&runID); err != nil {
			return nil, fmt.Errorf("scan active run id: %w", err)
		}
		runID = strings.TrimSpace(runID)
		if runID != "" {
			runIDs = append(runIDs, runID)
		}
	}
	if err := runRows.Err(); err != nil {
		return nil, fmt.Errorf("read active run ids: %w", err)
	}
	if len(runIDs) == 0 {
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("commit empty cancel runs tx: %w", err)
		}
		committed = true
		return nil, nil
	}

	agentRows, err := tx.QueryContext(ctx, `
		SELECT DISTINCT d.subscriber_id
		FROM event_deliveries d
		JOIN events e ON e.event_id = d.event_id
		WHERE e.run_id::text = ANY($1::text[])
		  AND d.subscriber_type = 'agent'
		  AND d.status IN ('pending', 'in_progress')
		ORDER BY d.subscriber_id ASC
	`, pq.Array(runIDs))
	if err != nil {
		return nil, fmt.Errorf("query affected agents for runs: %w", err)
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

	sideEffects, err := marshalAgentReceiptSideEffects(newAgentReceiptSideEffects("dead_letter", "cancelled_by_kill_previous", 0, "cancelled by --kill-previous"))
	if err != nil {
		return nil, fmt.Errorf("marshal kill_previous receipt side effects: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `
		WITH targeted AS (
			SELECT
				d.delivery_id,
				d.event_id,
				d.subscriber_type,
				d.subscriber_id,
				e.entity_id,
				e.flow_instance
			FROM event_deliveries d
			JOIN events e ON e.event_id = d.event_id
			WHERE e.run_id::text = ANY($1::text[])
			  AND d.status IN ('pending', 'in_progress')
			FOR UPDATE OF d
		),
		updated AS (
			UPDATE event_deliveries d
			SET
				status = 'dead_letter',
				reason_code = 'cancelled_by_kill_previous',
				last_error = 'cancelled by --kill-previous',
				active_session_id = NULL,
				delivered_at = now()
			FROM targeted t
			WHERE d.delivery_id = t.delivery_id
			RETURNING t.event_id, t.subscriber_type, t.subscriber_id, t.entity_id, t.flow_instance
		)
		INSERT INTO event_receipts (
			event_id, subscriber_type, subscriber_id, entity_id, flow_instance,
			outcome, reason_code, side_effects, processed_at
		)
		SELECT
			u.event_id,
			u.subscriber_type,
			u.subscriber_id,
			u.entity_id,
			u.flow_instance,
			'dead_letter',
			'cancelled_by_kill_previous',
			$2::jsonb,
			now()
		FROM updated u
		ON CONFLICT (event_id, subscriber_id) DO UPDATE SET
			entity_id = EXCLUDED.entity_id,
			flow_instance = EXCLUDED.flow_instance,
			outcome = EXCLUDED.outcome,
			reason_code = EXCLUDED.reason_code,
			side_effects = EXCLUDED.side_effects,
			processed_at = now()
	`, pq.Array(runIDs), string(sideEffects)); err != nil {
		return nil, fmt.Errorf("cancel kill_previous deliveries/receipts: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit cancel runs tx: %w", err)
	}
	committed = true
	return affectedAgents, nil
}
