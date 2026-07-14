package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
)

func insertDecisionCardLifecycleOutbox(ctx context.Context, tx *sql.Tx, card decisioncard.Card, evt events.Event, postgres bool) error {
	scope, err := card.Anchor.Scope()
	if err != nil {
		return err
	}
	query := `INSERT INTO decision_card_lifecycle_outbox (
		event_id, card_id, run_id, bundle_hash, event_name, payload, entity_id, flow_instance, status, created_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)
	ON CONFLICT (event_id) DO NOTHING`
	if postgres {
		query = numberPostgresPlaceholders(strings.ReplaceAll(query, "?", "$%d"))
	}
	_, err = tx.ExecContext(ctx, query, evt.ID(), card.CardID, card.RunID, card.BundleHash, string(evt.Type()), string(evt.Payload()), nullString(scope.EntityID), nullString(scope.FlowInstance), evt.CreatedAt().UTC())
	return err
}

func (s *PostgresStore) ListPendingDecisionCardLifecycleEvents(ctx context.Context, bundleHash string, limit int) ([]events.Event, error) {
	if limit <= 0 {
		limit = 200
	}
	rows, err := s.DB.QueryContext(ctx, `
		SELECT event_id::text, run_id::text, event_name, payload, COALESCE(entity_id::text, ''), COALESCE(flow_instance, ''), created_at
		FROM decision_card_lifecycle_outbox
		WHERE bundle_hash = $1 AND status = 'pending'
		ORDER BY created_at ASC, event_id ASC
		LIMIT $2
	`, strings.TrimSpace(bundleHash), limit)
	if err != nil {
		return nil, err
	}
	return scanDecisionCardLifecycleEvents(rows, limit)
}

func (s *SQLiteRuntimeStore) ListPendingDecisionCardLifecycleEvents(ctx context.Context, bundleHash string, limit int) ([]events.Event, error) {
	if limit <= 0 {
		limit = 200
	}
	rows, err := s.DB.QueryContext(ctx, `
		SELECT event_id, run_id, event_name, payload, COALESCE(entity_id, ''), COALESCE(flow_instance, ''), created_at
		FROM decision_card_lifecycle_outbox
		WHERE bundle_hash = ? AND status = 'pending'
		ORDER BY created_at ASC, event_id ASC
		LIMIT ?
	`, strings.TrimSpace(bundleHash), limit)
	if err != nil {
		return nil, err
	}
	return scanDecisionCardLifecycleEvents(rows, limit)
}

func scanDecisionCardLifecycleEvents(rows *sql.Rows, limit int) ([]events.Event, error) {
	defer rows.Close()
	out := make([]events.Event, 0, limit)
	for rows.Next() {
		var eventID, runID, eventName, entityID, flowInstance string
		var payloadRaw, createdAtRaw any
		if err := rows.Scan(&eventID, &runID, &eventName, &payloadRaw, &entityID, &flowInstance, &createdAtRaw); err != nil {
			return nil, fmt.Errorf("scan decision card lifecycle event: %w", err)
		}
		createdAt, ok, err := sqliteTimeValue(createdAtRaw)
		if err != nil || !ok {
			return nil, fmt.Errorf("decode decision card lifecycle created_at: %w", err)
		}
		out = append(out, events.NewRuntimeControlEvent(eventID, events.EventType(eventName), "platform", "", sqliteJSONRawMessage(payloadRaw), 0, runID, "",
			events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), flowInstance), createdAt.UTC()))
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *PostgresStore) CompleteDecisionCardLifecycleEvent(ctx context.Context, eventID string, completedAt time.Time) error {
	return runPostgresDecisionCardMutation(ctx, s.DB, func(txctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(txctx, `UPDATE decision_card_lifecycle_outbox SET status = 'completed', completed_at = $2 WHERE event_id = $1 AND status = 'pending'`, strings.TrimSpace(eventID), completedAt.UTC())
		return err
	})
}

func (s *SQLiteRuntimeStore) CompleteDecisionCardLifecycleEvent(ctx context.Context, eventID string, completedAt time.Time) error {
	return s.runDecisionCardMutation(ctx, "sqlite complete decision card lifecycle event", func(txctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(txctx, `UPDATE decision_card_lifecycle_outbox SET status = 'completed', completed_at = ? WHERE event_id = ? AND status = 'pending'`, completedAt.UTC(), strings.TrimSpace(eventID))
		return err
	})
}
