package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	runtimedestructivereset "swarm/internal/runtime/destructivereset"
	runtimerunquiescence "swarm/internal/runtime/runquiescence"
)

func (s *WorkflowInstanceStore) SystemNodeProcessed(ctx context.Context, nodeID, eventID string) (bool, error) {
	nodeID = strings.TrimSpace(nodeID)
	eventID = strings.TrimSpace(eventID)
	if s == nil || s.db == nil || nodeID == "" || eventID == "" {
		return false, nil
	}
	if s.isSQLite() {
		var ok bool
		err := dbQueryRowContext(ctx, s.db, `
			SELECT EXISTS(
				SELECT 1
				FROM event_receipts
				WHERE subscriber_type = 'node'
				  AND subscriber_id = ?
				  AND idempotency_key = ?
			)
		`, nodeID, SystemNodeReceiptIdempotencyKey(nodeID, eventID)).Scan(&ok)
		return ok, err
	}
	var ok bool
	err := dbQueryRowContext(ctx, s.db, `
		SELECT EXISTS(
			SELECT 1
			FROM event_receipts
			WHERE subscriber_type = 'node'
			  AND subscriber_id = $1
			  AND idempotency_key = $2
		)
	`, nodeID, SystemNodeReceiptIdempotencyKey(nodeID, eventID)).Scan(&ok)
	return ok, err
}

func (s *WorkflowInstanceStore) SystemNodeDeliveryQuiesced(ctx context.Context, nodeID, eventID string) (bool, error) {
	nodeID = strings.TrimSpace(nodeID)
	eventID = strings.TrimSpace(eventID)
	if s == nil || s.db == nil || nodeID == "" || eventID == "" {
		return false, nil
	}
	if _, err := uuid.Parse(eventID); err != nil {
		return false, nil
	}
	if s.isSQLite() {
		var ok bool
		err := dbQueryRowContext(ctx, s.db, `
			SELECT EXISTS (
				SELECT 1
				FROM event_deliveries
				WHERE event_id = ?
				  AND subscriber_type = 'node'
				  AND subscriber_id = ?
				  AND status = 'dead_letter'
				  AND reason_code IN (?, ?)
			)
		`, eventID, nodeID, runtimedestructivereset.QuiescenceReasonCode, runtimerunquiescence.ServeAbandonReasonCode).Scan(&ok)
		return ok, err
	}
	var ok bool
	err := dbQueryRowContext(ctx, s.db, `
		SELECT EXISTS (
			SELECT 1
			FROM event_deliveries
			WHERE event_id = $1::uuid
			  AND subscriber_type = 'node'
			  AND subscriber_id = $2
			  AND status = 'dead_letter'
			  AND reason_code IN ($3, $4)
		)
	`, eventID, nodeID, runtimedestructivereset.QuiescenceReasonCode, runtimerunquiescence.ServeAbandonReasonCode).Scan(&ok)
	return ok, err
}

func (s *WorkflowInstanceStore) MarkSystemNodeProcessedAndSettleDelivery(ctx context.Context, nodeID, eventID, sideEffects string) error {
	nodeID = strings.TrimSpace(nodeID)
	eventID = strings.TrimSpace(eventID)
	if s == nil || s.db == nil || nodeID == "" || eventID == "" {
		return nil
	}
	if s.isSQLite() {
		return s.markSQLiteSystemNodeProcessedAndSettleDelivery(ctx, nodeID, eventID, sideEffects)
	}
	return persistSystemNodeProcessedReceiptAndSettleDelivery(ctx, s.db, nodeID, eventID, sideEffects)
}

func (s *WorkflowInstanceStore) markSQLiteSystemNodeProcessedAndSettleDelivery(ctx context.Context, nodeID, eventID, sideEffects string) error {
	return s.RunInPipelineTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		now := time.Now().UTC()
		res, err := tx.ExecContext(txctx, `
			INSERT INTO event_receipts (
				receipt_id, event_id, subscriber_type, subscriber_id, entity_id, flow_instance,
				outcome, reason_code, side_effects, idempotency_key, processed_at
			)
			SELECT
				?, e.event_id, 'node', ?, e.entity_id, e.flow_instance,
				'no_op', 'idempotent_no_op', ?, ?, ?
			FROM events e
			WHERE e.event_id = ?
			ON CONFLICT(event_id, subscriber_type, subscriber_id) DO UPDATE SET
				entity_id = excluded.entity_id,
				flow_instance = excluded.flow_instance,
				outcome = excluded.outcome,
				reason_code = excluded.reason_code,
				side_effects = excluded.side_effects,
				idempotency_key = excluded.idempotency_key,
				processed_at = excluded.processed_at
		`, uuid.NewString(), nodeID, sqliteNodeJSON(sideEffects), SystemNodeReceiptIdempotencyKey(nodeID, eventID), now, eventID)
		if err != nil {
			return fmt.Errorf("upsert sqlite system node receipt: %w", err)
		}
		if rows, err := res.RowsAffected(); err == nil && rows == 0 {
			return fmt.Errorf("upsert sqlite system node receipt: event %s not found", eventID)
		}
		res, err = tx.ExecContext(txctx, `
			UPDATE event_deliveries
			SET status = 'delivered',
			    retry_count = COALESCE(retry_count, 0),
			    reason_code = 'node_processed',
			    last_error = NULL,
			    active_session_id = NULL,
			    started_at = COALESCE(started_at, created_at),
			    delivered_at = ?
			WHERE event_id = ?
			  AND subscriber_type = 'node'
			  AND subscriber_id = ?
			  AND (
				status IN ('pending', 'in_progress')
				OR (status = 'failed' AND COALESCE(retry_count, 0) < 2)
			  )
		`, now, eventID, nodeID)
		if err != nil {
			return fmt.Errorf("settle sqlite system node delivery: %w", err)
		}
		if rows, _ := res.RowsAffected(); rows > 0 {
			return nil
		}
		if _, err := tx.ExecContext(txctx, `
			INSERT INTO event_deliveries (
				delivery_id, run_id, event_id, subscriber_type, subscriber_id, status, retry_count,
				reason_code, last_error, active_session_id, started_at, delivered_at, created_at
			)
			SELECT
				?, e.run_id, e.event_id, 'node', ?, 'delivered', 0,
				'node_processed', NULL, NULL, ?, ?, ?
			FROM events e
			WHERE e.event_id = ?
			  AND NOT EXISTS (
				SELECT 1
				FROM event_deliveries d
				WHERE d.event_id = e.event_id
				  AND d.subscriber_type = 'node'
				  AND d.subscriber_id = ?
			  )
		`, uuid.NewString(), nodeID, now, now, now, eventID, nodeID); err != nil {
			return fmt.Errorf("insert sqlite settled system node delivery: %w", err)
		}
		return nil
	})
}

func sqliteNodeJSON(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "{}"
	}
	return raw
}
