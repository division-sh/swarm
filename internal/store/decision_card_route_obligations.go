package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

func insertDecisionRouteObligation(ctx context.Context, tx *sql.Tx, card decisioncard.Card, now time.Time, postgres bool) error {
	query := `INSERT INTO decision_card_route_obligations (
		event_id, card_id, run_id, status, attempt_count, next_attempt_at, created_at, updated_at
	) VALUES (?, ?, ?, 'pending', 0, ?, ?, ?)
	ON CONFLICT (event_id) DO NOTHING`
	if postgres {
		query = numberPostgresPlaceholders(strings.ReplaceAll(query, "?", "$%d"))
	}
	_, err := tx.ExecContext(ctx, query, card.DecisionEventID, card.CardID, card.RunID, now.UTC(), now.UTC(), now.UTC())
	if err != nil {
		return fmt.Errorf("create decision route obligation: %w", err)
	}
	return nil
}

func (s *PostgresStore) ListDueDecisionRouteObligations(ctx context.Context, now time.Time, limit int) ([]events.PersistedReplayEvent, error) {
	if limit <= 0 {
		limit = 200
	}
	var pending bool
	if err := s.DB.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM decision_card_route_obligations WHERE status = 'pending' AND next_attempt_at <= $1)`, now.UTC()).Scan(&pending); err != nil {
		return nil, err
	}
	if !pending {
		return nil, nil
	}
	rows, err := s.DB.QueryContext(ctx, `
		SELECT e.event_id::text, COALESCE(e.run_id::text, ''), e.event_name,
			COALESCE(e.produced_by, ''), COALESCE(e.entity_id::text, ''),
			COALESCE(e.flow_instance, ''), COALESCE(e.scope, 'global'), e.payload,
			e.created_at, COALESCE(e.source_event_id::text, ''), e.execution_mode,
			COALESCE(e.source_route, '{}'::jsonb), COALESCE(e.target_route, '{}'::jsonb),
			COALESCE(e.target_set, '[]'::jsonb)
		FROM decision_card_route_obligations o
		JOIN events e ON e.event_id = o.event_id
		WHERE o.status = 'pending' AND o.next_attempt_at <= $1
		ORDER BY o.attempt_count ASC, o.next_attempt_at ASC, o.created_at ASC, o.event_id ASC
		LIMIT $2
	`, now.UTC(), limit)
	if err != nil {
		return nil, fmt.Errorf("list due decision route obligations: %w", err)
	}
	return scanDecisionRouteObligationEvents(rows, limit)
}

func (s *SQLiteRuntimeStore) ListDueDecisionRouteObligations(ctx context.Context, now time.Time, limit int) ([]events.PersistedReplayEvent, error) {
	if limit <= 0 {
		limit = 200
	}
	var pending int
	if err := s.DB.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM decision_card_route_obligations WHERE status = 'pending' AND next_attempt_at <= ?)`, now.UTC()).Scan(&pending); err != nil {
		return nil, err
	}
	if pending == 0 {
		return nil, nil
	}
	rows, err := s.DB.QueryContext(ctx, `
		SELECT e.event_id, COALESCE(e.run_id, ''), e.event_name,
			COALESCE(e.produced_by, ''), COALESCE(e.entity_id, ''),
			COALESCE(e.flow_instance, ''), COALESCE(e.scope, 'global'), e.payload,
			e.created_at, COALESCE(e.source_event_id, ''), e.execution_mode,
			COALESCE(e.source_route, '{}'), COALESCE(e.target_route, '{}'),
			COALESCE(e.target_set, '[]')
		FROM decision_card_route_obligations o
		JOIN events e ON e.event_id = o.event_id
		WHERE o.status = 'pending' AND o.next_attempt_at <= ?
		ORDER BY o.attempt_count ASC, o.next_attempt_at ASC, o.created_at ASC, o.event_id ASC
		LIMIT ?
	`, now.UTC(), limit)
	if err != nil {
		return nil, fmt.Errorf("list due decision route obligations: %w", err)
	}
	return scanDecisionRouteObligationEvents(rows, limit)
}

func scanDecisionRouteObligationEvents(rows *sql.Rows, limit int) ([]events.PersistedReplayEvent, error) {
	defer rows.Close()
	out := make([]events.PersistedReplayEvent, 0, limit)
	for rows.Next() {
		var eventID, runID, eventName, producedBy, entityID, flowInstance, scope, sourceEventID, executionMode string
		var payloadRaw, createdAtRaw, sourceRouteRaw, targetRouteRaw, targetSetRaw any
		if err := rows.Scan(&eventID, &runID, &eventName, &producedBy, &entityID, &flowInstance, &scope,
			&payloadRaw, &createdAtRaw, &sourceEventID, &executionMode, &sourceRouteRaw, &targetRouteRaw, &targetSetRaw); err != nil {
			return nil, fmt.Errorf("scan decision route obligation: %w", err)
		}
		createdAt, ok, err := sqliteTimeValue(createdAtRaw)
		if err != nil || !ok {
			return nil, fmt.Errorf("decode decision route obligation created_at: %w", err)
		}
		evt := events.NewProjectionEvent(eventID, events.EventType(eventName), producedBy, "", sqliteJSONRawMessage(payloadRaw), 0,
			runID, sourceEventID, eventEnvelopeFromStorage(entityID, flowInstance, scope,
				sqliteJSONRawMessage(sourceRouteRaw), sqliteJSONRawMessage(targetRouteRaw), sqliteJSONRawMessage(targetSetRaw)), createdAt.UTC())
		evt, err = eventWithStoredExecutionMode(evt, executionMode)
		if err != nil {
			return nil, err
		}
		record := events.PersistedReplayEvent{Event: evt}
		if strings.TrimSpace(runID) == "" {
			record.ReplayFailure = replayAdmissionFailure("missing_canonical_run_id")
		}
		out = append(out, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read decision route obligations: %w", err)
	}
	return out, nil
}

func (s *PostgresStore) DeferDecisionRouteObligation(ctx context.Context, eventID string, nextAttemptAt time.Time, failure *runtimefailures.Envelope) error {
	return runPostgresDecisionCardMutation(ctx, s.DB, func(txctx context.Context, tx *sql.Tx) error {
		return deferDecisionRouteObligation(txctx, tx, eventID, nextAttemptAt, failure, true)
	})
}

func (s *SQLiteRuntimeStore) DeferDecisionRouteObligation(ctx context.Context, eventID string, nextAttemptAt time.Time, failure *runtimefailures.Envelope) error {
	return s.runDecisionCardMutation(ctx, "sqlite defer decision route obligation", func(txctx context.Context, tx *sql.Tx) error {
		return deferDecisionRouteObligation(txctx, tx, eventID, nextAttemptAt, failure, false)
	})
}

func deferDecisionRouteObligation(ctx context.Context, db decisionCardSQL, eventID string, nextAttemptAt time.Time, failure *runtimefailures.Envelope, postgres bool) error {
	raw, err := json.Marshal(failure)
	if err != nil {
		return err
	}
	query := `UPDATE decision_card_route_obligations
		SET attempt_count = attempt_count + 1, next_attempt_at = ?, last_failure = ?, updated_at = ?
		WHERE event_id = ? AND status = 'pending'`
	if postgres {
		query = `UPDATE decision_card_route_obligations
			SET attempt_count = attempt_count + 1, next_attempt_at = $1, last_failure = $2, updated_at = $3
			WHERE event_id = $4 AND status = 'pending'`
	}
	_, err = db.ExecContext(ctx, query, nextAttemptAt.UTC(), string(raw), time.Now().UTC(), strings.TrimSpace(eventID))
	return err
}

func (s *PostgresStore) QuarantineDecisionRouteObligation(ctx context.Context, eventID string, quarantinedAt time.Time, failure *runtimefailures.Envelope) error {
	return runPostgresDecisionCardMutation(ctx, s.DB, func(txctx context.Context, tx *sql.Tx) error {
		if err := s.UpsertPipelineReceiptTx(txctx, tx, eventID, "error", failure); err != nil {
			return err
		}
		_, err := tx.ExecContext(txctx, `UPDATE decision_card_route_obligations
			SET status = 'quarantined', quarantined_at = $2, last_failure = $3, updated_at = $2
			WHERE event_id = $1 AND status = 'pending'`, strings.TrimSpace(eventID), quarantinedAt.UTC(), storedDecisionRouteFailure(failure))
		return err
	})
}

func (s *SQLiteRuntimeStore) QuarantineDecisionRouteObligation(ctx context.Context, eventID string, quarantinedAt time.Time, failure *runtimefailures.Envelope) error {
	return s.runDecisionCardMutation(ctx, "sqlite quarantine decision route obligation", func(txctx context.Context, tx *sql.Tx) error {
		if err := s.UpsertPipelineReceiptTx(txctx, tx, eventID, "error", failure); err != nil {
			return err
		}
		_, err := tx.ExecContext(txctx, `UPDATE decision_card_route_obligations
			SET status = 'quarantined', quarantined_at = ?, last_failure = ?, updated_at = ?
			WHERE event_id = ? AND status = 'pending'`, quarantinedAt.UTC(), storedDecisionRouteFailure(failure), quarantinedAt.UTC(), strings.TrimSpace(eventID))
		return err
	})
}

func storedDecisionRouteFailure(failure *runtimefailures.Envelope) string {
	raw, _ := json.Marshal(failure)
	return string(raw)
}

func (s *PostgresStore) CompleteDecisionRouteObligation(ctx context.Context, eventID string, completedAt time.Time) error {
	return runPostgresDecisionCardMutation(ctx, s.DB, func(txctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(txctx, `UPDATE decision_card_route_obligations SET status = 'completed', completed_at = $2, updated_at = $2 WHERE event_id = $1 AND status = 'pending'`, strings.TrimSpace(eventID), completedAt.UTC())
		return err
	})
}

func (s *SQLiteRuntimeStore) CompleteDecisionRouteObligation(ctx context.Context, eventID string, completedAt time.Time) error {
	return s.runDecisionCardMutation(ctx, "sqlite complete decision route obligation", func(txctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(txctx, `UPDATE decision_card_route_obligations SET status = 'completed', completed_at = ?, updated_at = ? WHERE event_id = ? AND status = 'pending'`, completedAt.UTC(), completedAt.UTC(), strings.TrimSpace(eventID))
		return err
	})
}
