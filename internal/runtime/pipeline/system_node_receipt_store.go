package pipeline

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimedestructivereset "github.com/division-sh/swarm/internal/runtime/destructivereset"
	runtimerunquiescence "github.com/division-sh/swarm/internal/runtime/runquiescence"
	"github.com/google/uuid"
)

var ErrSystemNodeDeliveryAuthorityMissing = errors.New("system node delivery authority missing")

func (s *WorkflowInstanceStore) SystemNodeDeliveryAuthorized(ctx context.Context, nodeID, eventID string, retryLimit int) (bool, error) {
	nodeID = strings.TrimSpace(nodeID)
	eventID = strings.TrimSpace(eventID)
	retryLimit = normalizeSystemNodeRetryLimit(retryLimit)
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
					  AND COALESCE(delivery_target_route, '{}') = '{}'
					  AND (
						status IN ('pending', 'in_progress')
						OR (status = 'failed' AND COALESCE(retry_count, 0) < ?)
					  )
				)
			`, eventID, nodeID, retryLimit).Scan(&ok)
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
				  AND COALESCE(delivery_target_route, '{}'::jsonb) = '{}'::jsonb
				  AND (
					status IN ('pending', 'in_progress')
					OR (status = 'failed' AND COALESCE(retry_count, 0) < $3)
				  )
			)
		`, eventID, nodeID, retryLimit).Scan(&ok)
	return ok, err
}

func (s *WorkflowInstanceStore) SystemNodeDeliveryAuthorizedForTarget(ctx context.Context, nodeID, eventID string, target events.RouteIdentity, retryLimit int) (bool, error) {
	target = target.Normalized()
	if target.Empty() {
		return s.SystemNodeDeliveryAuthorized(ctx, nodeID, eventID, retryLimit)
	}
	nodeID = strings.TrimSpace(nodeID)
	eventID = strings.TrimSpace(eventID)
	retryLimit = normalizeSystemNodeRetryLimit(retryLimit)
	if s == nil || s.db == nil || nodeID == "" || eventID == "" {
		return false, nil
	}
	if _, err := uuid.Parse(eventID); err != nil {
		return false, nil
	}
	targetJSON := systemNodeRouteIdentityJSON(target)
	if s.isSQLite() {
		var ok bool
		err := dbQueryRowContext(ctx, s.db, `
			SELECT EXISTS (
				SELECT 1
				FROM event_deliveries
				WHERE event_id = ?
				  AND subscriber_type = 'node'
				  AND subscriber_id = ?
				  AND COALESCE(delivery_target_route, '{}') = ?
				  AND (
					status IN ('pending', 'in_progress')
					OR (status = 'failed' AND COALESCE(retry_count, 0) < ?)
				  )
				)
			`, eventID, nodeID, targetJSON, retryLimit).Scan(&ok)
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
			  AND COALESCE(delivery_target_route, '{}'::jsonb) = $3::jsonb
			  AND (
				status IN ('pending', 'in_progress')
				OR (status = 'failed' AND COALESCE(retry_count, 0) < $4)
			  )
			)
		`, eventID, nodeID, targetJSON, retryLimit).Scan(&ok)
	return ok, err
}

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

func (s *WorkflowInstanceStore) SystemNodeProcessedForTarget(ctx context.Context, nodeID, eventID string, target events.RouteIdentity) (bool, error) {
	target = target.Normalized()
	if target.Empty() {
		return s.SystemNodeProcessed(ctx, nodeID, eventID)
	}
	nodeID = strings.TrimSpace(nodeID)
	eventID = strings.TrimSpace(eventID)
	if s == nil || s.db == nil || nodeID == "" || eventID == "" {
		return false, nil
	}
	idempotencyKey := systemNodeReceiptIdempotencyKeyForTarget(nodeID, eventID, target)
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
		`, nodeID, idempotencyKey).Scan(&ok)
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
	`, nodeID, idempotencyKey).Scan(&ok)
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

func (s *WorkflowInstanceStore) MarkSystemNodeProcessedAndSettleDeliveryForTarget(ctx context.Context, nodeID, eventID string, target events.RouteIdentity, sideEffects string) error {
	target = target.Normalized()
	if target.Empty() {
		return s.MarkSystemNodeProcessedAndSettleDelivery(ctx, nodeID, eventID, sideEffects)
	}
	nodeID = strings.TrimSpace(nodeID)
	eventID = strings.TrimSpace(eventID)
	if s == nil || s.db == nil || nodeID == "" || eventID == "" {
		return nil
	}
	if s.isSQLite() {
		return s.markSQLiteSystemNodeProcessedAndSettleDeliveryForTarget(ctx, nodeID, eventID, target, sideEffects)
	}
	return persistSystemNodeProcessedReceiptAndSettleDeliveryForTarget(ctx, s.db, nodeID, eventID, target, sideEffects)
}

func (s *WorkflowInstanceStore) MarkSystemNodeDeliveryInProgress(ctx context.Context, nodeID, eventID string, retryLimit int) error {
	nodeID = strings.TrimSpace(nodeID)
	eventID = strings.TrimSpace(eventID)
	retryLimit = normalizeSystemNodeRetryLimit(retryLimit)
	if s == nil || s.db == nil || nodeID == "" || eventID == "" {
		return nil
	}
	if s.isSQLite() {
		return s.markSQLiteSystemNodeDeliveryInProgress(ctx, nodeID, eventID, retryLimit)
	}
	return markPostgresSystemNodeDeliveryInProgress(ctx, s.db, nodeID, eventID, retryLimit)
}

func (s *WorkflowInstanceStore) MarkSystemNodeDeliveryInProgressForTarget(ctx context.Context, nodeID, eventID string, target events.RouteIdentity, retryLimit int) error {
	target = target.Normalized()
	if target.Empty() {
		return s.MarkSystemNodeDeliveryInProgress(ctx, nodeID, eventID, retryLimit)
	}
	nodeID = strings.TrimSpace(nodeID)
	eventID = strings.TrimSpace(eventID)
	retryLimit = normalizeSystemNodeRetryLimit(retryLimit)
	if s == nil || s.db == nil || nodeID == "" || eventID == "" {
		return nil
	}
	if s.isSQLite() {
		return s.markSQLiteSystemNodeDeliveryInProgressForTarget(ctx, nodeID, eventID, target, retryLimit)
	}
	return markPostgresSystemNodeDeliveryInProgressForTarget(ctx, s.db, nodeID, eventID, target, retryLimit)
}

func (s *WorkflowInstanceStore) MarkSystemNodeDeliveryFailed(ctx context.Context, nodeID, eventID, reasonCode, errText string, retryCount, retryLimit int) error {
	nodeID = strings.TrimSpace(nodeID)
	eventID = strings.TrimSpace(eventID)
	retryLimit = normalizeSystemNodeRetryLimit(retryLimit)
	if s == nil || s.db == nil || nodeID == "" || eventID == "" {
		return nil
	}
	if s.isSQLite() {
		return s.markSQLiteSystemNodeDeliveryFailed(ctx, nodeID, eventID, reasonCode, errText, retryCount, retryLimit)
	}
	return markPostgresSystemNodeDeliveryFailed(ctx, s.db, nodeID, eventID, reasonCode, errText, retryCount, retryLimit)
}

func (s *WorkflowInstanceStore) MarkSystemNodeDeliveryFailedForTarget(ctx context.Context, nodeID, eventID string, target events.RouteIdentity, reasonCode, errText string, retryCount, retryLimit int) error {
	target = target.Normalized()
	if target.Empty() {
		return s.MarkSystemNodeDeliveryFailed(ctx, nodeID, eventID, reasonCode, errText, retryCount, retryLimit)
	}
	nodeID = strings.TrimSpace(nodeID)
	eventID = strings.TrimSpace(eventID)
	retryLimit = normalizeSystemNodeRetryLimit(retryLimit)
	if s == nil || s.db == nil || nodeID == "" || eventID == "" {
		return nil
	}
	if s.isSQLite() {
		return s.markSQLiteSystemNodeDeliveryFailedForTarget(ctx, nodeID, eventID, target, reasonCode, errText, retryCount, retryLimit)
	}
	return markPostgresSystemNodeDeliveryFailedForTarget(ctx, s.db, nodeID, eventID, target, reasonCode, errText, retryCount, retryLimit)
}

func (s *WorkflowInstanceStore) MarkSystemNodeDeliveryDeadLetter(ctx context.Context, nodeID, eventID, reasonCode, errText string, retryCount int, sideEffects string) error {
	nodeID = strings.TrimSpace(nodeID)
	eventID = strings.TrimSpace(eventID)
	if s == nil || s.db == nil || nodeID == "" || eventID == "" {
		return nil
	}
	if s.isSQLite() {
		return s.markSQLiteSystemNodeDeliveryDeadLetter(ctx, nodeID, eventID, reasonCode, errText, retryCount, sideEffects)
	}
	return markPostgresSystemNodeDeliveryDeadLetter(ctx, s.db, nodeID, eventID, reasonCode, errText, retryCount, sideEffects)
}

func (s *WorkflowInstanceStore) MarkSystemNodeDeliveryDeadLetterForTarget(ctx context.Context, nodeID, eventID string, target events.RouteIdentity, reasonCode, errText string, retryCount int, sideEffects string) error {
	target = target.Normalized()
	if target.Empty() {
		return s.MarkSystemNodeDeliveryDeadLetter(ctx, nodeID, eventID, reasonCode, errText, retryCount, sideEffects)
	}
	nodeID = strings.TrimSpace(nodeID)
	eventID = strings.TrimSpace(eventID)
	if s == nil || s.db == nil || nodeID == "" || eventID == "" {
		return nil
	}
	if s.isSQLite() {
		return s.markSQLiteSystemNodeDeliveryDeadLetterForTarget(ctx, nodeID, eventID, target, reasonCode, errText, retryCount, sideEffects)
	}
	return markPostgresSystemNodeDeliveryDeadLetterForTarget(ctx, s.db, nodeID, eventID, target, reasonCode, errText, retryCount, sideEffects)
}

func (s *WorkflowInstanceStore) markSQLiteSystemNodeProcessedAndSettleDelivery(ctx context.Context, nodeID, eventID, sideEffects string) error {
	return s.runInPipelineTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		retryLimit := normalizeSystemNodeRetryLimit(DefaultSystemNodeRetryLimit)
		authorized, err := sqliteSystemNodeDeliveryAuthorizedTx(txctx, tx, nodeID, eventID, retryLimit)
		if err != nil {
			return fmt.Errorf("query sqlite system node delivery authority: %w", err)
		}
		if !authorized {
			return fmt.Errorf("%w: node %s event %s", ErrSystemNodeDeliveryAuthorityMissing, nodeID, eventID)
		}
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
					  AND COALESCE(delivery_target_route, '{}') = '{}'
					  AND (
						status IN ('pending', 'in_progress')
						OR (status = 'failed' AND COALESCE(retry_count, 0) < ?)
				  )
			`, now, eventID, nodeID, retryLimit)
		if err != nil {
			return fmt.Errorf("settle sqlite system node delivery: %w", err)
		}
		if rows, _ := res.RowsAffected(); rows > 0 {
			return nil
		}
		return nil
	})
}

func (s *WorkflowInstanceStore) markSQLiteSystemNodeProcessedAndSettleDeliveryForTarget(ctx context.Context, nodeID, eventID string, target events.RouteIdentity, sideEffects string) error {
	target = target.Normalized()
	targetJSON := systemNodeRouteIdentityJSON(target)
	return s.runInPipelineTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		retryLimit := normalizeSystemNodeRetryLimit(DefaultSystemNodeRetryLimit)
		authorized, err := sqliteSystemNodeDeliveryAuthorizedForTargetTx(txctx, tx, nodeID, eventID, target, retryLimit)
		if err != nil {
			return fmt.Errorf("query sqlite system node delivery authority: %w", err)
		}
		if !authorized {
			return fmt.Errorf("%w: node %s event %s target %s", ErrSystemNodeDeliveryAuthorityMissing, nodeID, eventID, targetJSON)
		}
		now := time.Now().UTC()
		idempotencyKey := systemNodeReceiptIdempotencyKeyForTarget(nodeID, eventID, target)
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
		`, uuid.NewString(), nodeID, sqliteNodeJSON(sideEffects), idempotencyKey, now, eventID)
		if err != nil {
			return fmt.Errorf("upsert sqlite targeted system node receipt: %w", err)
		}
		if rows, err := res.RowsAffected(); err == nil && rows == 0 {
			return fmt.Errorf("upsert sqlite targeted system node receipt: event %s not found", eventID)
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
			  AND COALESCE(delivery_target_route, '{}') = ?
			  AND (
				status IN ('pending', 'in_progress')
				OR (status = 'failed' AND COALESCE(retry_count, 0) < ?)
			  )
			`, now, eventID, nodeID, targetJSON, retryLimit)
		if err != nil {
			return fmt.Errorf("settle sqlite targeted system node delivery: %w", err)
		}
		if rows, _ := res.RowsAffected(); rows > 0 {
			return nil
		}
		return nil
	})
}

func (s *WorkflowInstanceStore) markSQLiteSystemNodeDeliveryInProgress(ctx context.Context, nodeID, eventID string, retryLimit int) error {
	retryLimit = normalizeSystemNodeRetryLimit(retryLimit)
	return s.runInPipelineTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		authorized, err := sqliteSystemNodeDeliveryAuthorizedTx(txctx, tx, nodeID, eventID, retryLimit)
		if err != nil {
			return fmt.Errorf("query sqlite system node delivery authority: %w", err)
		}
		if !authorized {
			return fmt.Errorf("%w: node %s event %s", ErrSystemNodeDeliveryAuthorityMissing, nodeID, eventID)
		}
		now := time.Now().UTC()
		res, err := tx.ExecContext(txctx, `
			UPDATE event_deliveries
			SET status = 'in_progress',
			    reason_code = 'node_processing',
			    last_error = NULL,
			    active_session_id = NULL,
			    started_at = COALESCE(started_at, ?),
			    delivered_at = NULL
				WHERE event_id = ?
				  AND subscriber_type = 'node'
				  AND subscriber_id = ?
				  AND COALESCE(delivery_target_route, '{}') = '{}'
				  AND (
					status IN ('pending', 'in_progress')
					OR (status = 'failed' AND COALESCE(retry_count, 0) < ?)
			  )
		`, now, eventID, nodeID, retryLimit)
		if err != nil {
			return fmt.Errorf("mark sqlite system node delivery in progress: %w", err)
		}
		if rows, _ := res.RowsAffected(); rows == 0 {
			return fmt.Errorf("%w: node %s event %s", ErrSystemNodeDeliveryAuthorityMissing, nodeID, eventID)
		}
		return nil
	})
}

func (s *WorkflowInstanceStore) markSQLiteSystemNodeDeliveryInProgressForTarget(ctx context.Context, nodeID, eventID string, target events.RouteIdentity, retryLimit int) error {
	retryLimit = normalizeSystemNodeRetryLimit(retryLimit)
	target = target.Normalized()
	targetJSON := systemNodeRouteIdentityJSON(target)
	return s.runInPipelineTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		authorized, err := sqliteSystemNodeDeliveryAuthorizedForTargetTx(txctx, tx, nodeID, eventID, target, retryLimit)
		if err != nil {
			return fmt.Errorf("query sqlite system node delivery authority: %w", err)
		}
		if !authorized {
			return fmt.Errorf("%w: node %s event %s target %s", ErrSystemNodeDeliveryAuthorityMissing, nodeID, eventID, targetJSON)
		}
		now := time.Now().UTC()
		res, err := tx.ExecContext(txctx, `
			UPDATE event_deliveries
			SET status = 'in_progress',
			    reason_code = 'node_processing',
			    last_error = NULL,
			    active_session_id = NULL,
			    started_at = COALESCE(started_at, ?),
			    delivered_at = NULL
			WHERE event_id = ?
			  AND subscriber_type = 'node'
			  AND subscriber_id = ?
			  AND COALESCE(delivery_target_route, '{}') = ?
			  AND (
				status IN ('pending', 'in_progress')
				OR (status = 'failed' AND COALESCE(retry_count, 0) < ?)
			  )
		`, now, eventID, nodeID, targetJSON, retryLimit)
		if err != nil {
			return fmt.Errorf("mark sqlite targeted system node delivery in progress: %w", err)
		}
		if rows, _ := res.RowsAffected(); rows == 0 {
			return fmt.Errorf("%w: node %s event %s target %s", ErrSystemNodeDeliveryAuthorityMissing, nodeID, eventID, targetJSON)
		}
		return nil
	})
}

func (s *WorkflowInstanceStore) markSQLiteSystemNodeDeliveryFailed(ctx context.Context, nodeID, eventID, reasonCode, errText string, retryCount, retryLimit int) error {
	retryLimit = normalizeSystemNodeRetryLimit(retryLimit)
	return s.runInPipelineTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		authorized, err := sqliteSystemNodeDeliveryAuthorizedTx(txctx, tx, nodeID, eventID, retryLimit)
		if err != nil {
			return fmt.Errorf("query sqlite system node delivery authority: %w", err)
		}
		if !authorized {
			return fmt.Errorf("%w: node %s event %s", ErrSystemNodeDeliveryAuthorityMissing, nodeID, eventID)
		}
		now := time.Now().UTC()
		res, err := tx.ExecContext(txctx, `
			UPDATE event_deliveries
			SET status = 'failed',
			    retry_count = ?,
			    reason_code = NULLIF(?, ''),
			    last_error = NULLIF(?, ''),
			    active_session_id = NULL,
			    started_at = COALESCE(started_at, created_at),
			    delivered_at = ?
				WHERE event_id = ?
				  AND subscriber_type = 'node'
				  AND subscriber_id = ?
				  AND COALESCE(delivery_target_route, '{}') = '{}'
				  AND status IN ('pending', 'in_progress', 'failed')
				  AND COALESCE(retry_count, 0) < ?
		`, sanitizedSystemNodeRetryCount(retryCount), sanitizeSystemNodeReasonCode(reasonCode, "handler_error"), strings.TrimSpace(errText), now, eventID, nodeID, retryLimit)
		if err != nil {
			return fmt.Errorf("mark sqlite system node delivery failed: %w", err)
		}
		if rows, _ := res.RowsAffected(); rows == 0 {
			return fmt.Errorf("%w: node %s event %s", ErrSystemNodeDeliveryAuthorityMissing, nodeID, eventID)
		}
		return nil
	})
}

func (s *WorkflowInstanceStore) markSQLiteSystemNodeDeliveryFailedForTarget(ctx context.Context, nodeID, eventID string, target events.RouteIdentity, reasonCode, errText string, retryCount, retryLimit int) error {
	retryLimit = normalizeSystemNodeRetryLimit(retryLimit)
	target = target.Normalized()
	targetJSON := systemNodeRouteIdentityJSON(target)
	return s.runInPipelineTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		authorized, err := sqliteSystemNodeDeliveryAuthorizedForTargetTx(txctx, tx, nodeID, eventID, target, retryLimit)
		if err != nil {
			return fmt.Errorf("query sqlite system node delivery authority: %w", err)
		}
		if !authorized {
			return fmt.Errorf("%w: node %s event %s target %s", ErrSystemNodeDeliveryAuthorityMissing, nodeID, eventID, targetJSON)
		}
		now := time.Now().UTC()
		res, err := tx.ExecContext(txctx, `
			UPDATE event_deliveries
			SET status = 'failed',
			    retry_count = ?,
			    reason_code = NULLIF(?, ''),
			    last_error = NULLIF(?, ''),
			    active_session_id = NULL,
			    started_at = COALESCE(started_at, created_at),
			    delivered_at = ?
			WHERE event_id = ?
			  AND subscriber_type = 'node'
			  AND subscriber_id = ?
			  AND COALESCE(delivery_target_route, '{}') = ?
			  AND status IN ('pending', 'in_progress', 'failed')
			  AND COALESCE(retry_count, 0) < ?
		`, sanitizedSystemNodeRetryCount(retryCount), sanitizeSystemNodeReasonCode(reasonCode, "handler_error"), strings.TrimSpace(errText), now, eventID, nodeID, targetJSON, retryLimit)
		if err != nil {
			return fmt.Errorf("mark sqlite targeted system node delivery failed: %w", err)
		}
		if rows, _ := res.RowsAffected(); rows == 0 {
			return fmt.Errorf("%w: node %s event %s target %s", ErrSystemNodeDeliveryAuthorityMissing, nodeID, eventID, targetJSON)
		}
		return nil
	})
}

func (s *WorkflowInstanceStore) markSQLiteSystemNodeDeliveryDeadLetter(ctx context.Context, nodeID, eventID, reasonCode, errText string, retryCount int, sideEffects string) error {
	return s.runInPipelineTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		exists, err := sqliteSystemNodeDeliveryRowExistsTx(txctx, tx, nodeID, eventID)
		if err != nil {
			return fmt.Errorf("query sqlite system node delivery row: %w", err)
		}
		if !exists {
			return fmt.Errorf("%w: node %s event %s", ErrSystemNodeDeliveryAuthorityMissing, nodeID, eventID)
		}
		now := time.Now().UTC()
		reasonCode = sanitizeSystemNodeReasonCode(reasonCode, "retry_exhausted")
		errText = strings.TrimSpace(errText)
		res, err := tx.ExecContext(txctx, `
			UPDATE event_deliveries
			SET status = 'dead_letter',
			    retry_count = ?,
			    reason_code = NULLIF(?, ''),
			    last_error = NULLIF(?, ''),
			    active_session_id = NULL,
			    started_at = COALESCE(started_at, created_at),
			    delivered_at = ?
				WHERE event_id = ?
				  AND subscriber_type = 'node'
				  AND subscriber_id = ?
				  AND COALESCE(delivery_target_route, '{}') = '{}'
				  AND status IN ('pending', 'in_progress', 'failed')
		`, sanitizedSystemNodeRetryCount(retryCount), reasonCode, errText, now, eventID, nodeID)
		if err != nil {
			return fmt.Errorf("dead-letter sqlite system node delivery: %w", err)
		}
		if rows, _ := res.RowsAffected(); rows == 0 {
			return fmt.Errorf("%w: node %s event %s", ErrSystemNodeDeliveryAuthorityMissing, nodeID, eventID)
		}
		res, err = tx.ExecContext(txctx, `
			INSERT INTO event_receipts (
				receipt_id, event_id, subscriber_type, subscriber_id, entity_id, flow_instance,
				outcome, reason_code, side_effects, idempotency_key, processed_at
			)
			SELECT
				?, e.event_id, 'node', ?, e.entity_id, e.flow_instance,
				'dead_letter', NULLIF(?, ''), ?, ?, ?
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
		`, uuid.NewString(), nodeID, reasonCode, sqliteNodeJSON(sideEffects), SystemNodeReceiptIdempotencyKey(nodeID, eventID), now, eventID)
		if err != nil {
			return fmt.Errorf("upsert sqlite system node dead-letter receipt: %w", err)
		}
		if rows, err := res.RowsAffected(); err == nil && rows == 0 {
			return fmt.Errorf("upsert sqlite system node dead-letter receipt: event %s not found", eventID)
		}
		return nil
	})
}

func (s *WorkflowInstanceStore) markSQLiteSystemNodeDeliveryDeadLetterForTarget(ctx context.Context, nodeID, eventID string, target events.RouteIdentity, reasonCode, errText string, retryCount int, sideEffects string) error {
	target = target.Normalized()
	targetJSON := systemNodeRouteIdentityJSON(target)
	return s.runInPipelineTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		exists, err := sqliteSystemNodeDeliveryRowExistsForTargetTx(txctx, tx, nodeID, eventID, target)
		if err != nil {
			return fmt.Errorf("query sqlite targeted system node delivery row: %w", err)
		}
		if !exists {
			return fmt.Errorf("%w: node %s event %s target %s", ErrSystemNodeDeliveryAuthorityMissing, nodeID, eventID, targetJSON)
		}
		now := time.Now().UTC()
		reasonCode = sanitizeSystemNodeReasonCode(reasonCode, "retry_exhausted")
		errText = strings.TrimSpace(errText)
		res, err := tx.ExecContext(txctx, `
			UPDATE event_deliveries
			SET status = 'dead_letter',
			    retry_count = ?,
			    reason_code = NULLIF(?, ''),
			    last_error = NULLIF(?, ''),
			    active_session_id = NULL,
			    started_at = COALESCE(started_at, created_at),
			    delivered_at = ?
			WHERE event_id = ?
			  AND subscriber_type = 'node'
			  AND subscriber_id = ?
			  AND COALESCE(delivery_target_route, '{}') = ?
			  AND status IN ('pending', 'in_progress', 'failed')
		`, sanitizedSystemNodeRetryCount(retryCount), reasonCode, errText, now, eventID, nodeID, targetJSON)
		if err != nil {
			return fmt.Errorf("dead-letter sqlite targeted system node delivery: %w", err)
		}
		if rows, _ := res.RowsAffected(); rows == 0 {
			return fmt.Errorf("%w: node %s event %s target %s", ErrSystemNodeDeliveryAuthorityMissing, nodeID, eventID, targetJSON)
		}
		idempotencyKey := systemNodeReceiptIdempotencyKeyForTarget(nodeID, eventID, target)
		res, err = tx.ExecContext(txctx, `
			INSERT INTO event_receipts (
				receipt_id, event_id, subscriber_type, subscriber_id, entity_id, flow_instance,
				outcome, reason_code, side_effects, idempotency_key, processed_at
			)
			SELECT
				?, e.event_id, 'node', ?, e.entity_id, e.flow_instance,
				'dead_letter', NULLIF(?, ''), ?, ?, ?
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
		`, uuid.NewString(), nodeID, reasonCode, sqliteNodeJSON(sideEffects), idempotencyKey, now, eventID)
		if err != nil {
			return fmt.Errorf("upsert sqlite targeted system node dead-letter receipt: %w", err)
		}
		if rows, err := res.RowsAffected(); err == nil && rows == 0 {
			return fmt.Errorf("upsert sqlite targeted system node dead-letter receipt: event %s not found", eventID)
		}
		return nil
	})
}

func sqliteSystemNodeDeliveryAuthorizedTx(ctx context.Context, tx *sql.Tx, nodeID, eventID string, retryLimit int) (bool, error) {
	if tx == nil {
		return false, nil
	}
	retryLimit = normalizeSystemNodeRetryLimit(retryLimit)
	if _, err := uuid.Parse(strings.TrimSpace(eventID)); err != nil {
		return false, nil
	}
	var ok bool
	err := tx.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM event_deliveries
			WHERE event_id = ?
			  AND subscriber_type = 'node'
			  AND subscriber_id = ?
			  AND COALESCE(delivery_target_route, '{}') = '{}'
			  AND (
				status IN ('pending', 'in_progress')
				OR (status = 'failed' AND COALESCE(retry_count, 0) < ?)
			  )
			)
	`, strings.TrimSpace(eventID), strings.TrimSpace(nodeID), retryLimit).Scan(&ok)
	return ok, err
}

func sqliteSystemNodeDeliveryAuthorizedForTargetTx(ctx context.Context, tx *sql.Tx, nodeID, eventID string, target events.RouteIdentity, retryLimit int) (bool, error) {
	if tx == nil {
		return false, nil
	}
	retryLimit = normalizeSystemNodeRetryLimit(retryLimit)
	if _, err := uuid.Parse(strings.TrimSpace(eventID)); err != nil {
		return false, nil
	}
	var ok bool
	err := tx.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM event_deliveries
			WHERE event_id = ?
			  AND subscriber_type = 'node'
			  AND subscriber_id = ?
			  AND COALESCE(delivery_target_route, '{}') = ?
			  AND (
				status IN ('pending', 'in_progress')
				OR (status = 'failed' AND COALESCE(retry_count, 0) < ?)
			  )
			)
	`, strings.TrimSpace(eventID), strings.TrimSpace(nodeID), systemNodeRouteIdentityJSON(target), retryLimit).Scan(&ok)
	return ok, err
}

func sqliteSystemNodeDeliveryRowExistsTx(ctx context.Context, tx *sql.Tx, nodeID, eventID string) (bool, error) {
	if tx == nil {
		return false, nil
	}
	if _, err := uuid.Parse(strings.TrimSpace(eventID)); err != nil {
		return false, nil
	}
	var ok bool
	err := tx.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM event_deliveries
			WHERE event_id = ?
			  AND subscriber_type = 'node'
			  AND subscriber_id = ?
			  AND COALESCE(delivery_target_route, '{}') = '{}'
		)
	`, strings.TrimSpace(eventID), strings.TrimSpace(nodeID)).Scan(&ok)
	return ok, err
}

func sqliteSystemNodeDeliveryRowExistsForTargetTx(ctx context.Context, tx *sql.Tx, nodeID, eventID string, target events.RouteIdentity) (bool, error) {
	if tx == nil {
		return false, nil
	}
	if _, err := uuid.Parse(strings.TrimSpace(eventID)); err != nil {
		return false, nil
	}
	var ok bool
	err := tx.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM event_deliveries
			WHERE event_id = ?
			  AND subscriber_type = 'node'
			  AND subscriber_id = ?
			  AND COALESCE(delivery_target_route, '{}') = ?
		)
	`, strings.TrimSpace(eventID), strings.TrimSpace(nodeID), systemNodeRouteIdentityJSON(target)).Scan(&ok)
	return ok, err
}

func systemNodeRouteIdentityJSON(route events.RouteIdentity) string {
	route = route.Normalized()
	if route.Empty() {
		return "{}"
	}
	payload := map[string]string{}
	if route.FlowInstance != "" {
		payload["flow_instance"] = route.FlowInstance
	}
	if route.EntityID != "" {
		payload["entity_id"] = route.EntityID
	}
	if route.FlowID != "" {
		payload["flow_id"] = route.FlowID
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "{}"
	}
	return string(encoded)
}

func systemNodeReceiptIdempotencyKeyForTarget(nodeID, eventID string, target events.RouteIdentity) string {
	base := SystemNodeReceiptIdempotencyKey(nodeID, eventID)
	target = target.Normalized()
	if target.Empty() {
		return base
	}
	return base + ":target:" + systemNodeRouteIdentityJSON(target)
}

func sqliteNodeJSON(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "{}"
	}
	return raw
}

func sanitizedSystemNodeRetryCount(retryCount int) int {
	if retryCount < 0 {
		return 0
	}
	return retryCount
}

func sanitizeSystemNodeReasonCode(reasonCode, fallback string) string {
	reasonCode = strings.TrimSpace(reasonCode)
	if reasonCode != "" {
		return reasonCode
	}
	fallback = strings.TrimSpace(fallback)
	if fallback != "" {
		return fallback
	}
	return "handler_error"
}

func normalizeSystemNodeRetryLimit(retryLimit int) int {
	if retryLimit <= 0 {
		return DefaultSystemNodeRetryLimit
	}
	return retryLimit
}
