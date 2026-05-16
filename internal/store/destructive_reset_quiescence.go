package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/lib/pq"
	"swarm/internal/runtime/destructivereset"
	runtimepipeline "swarm/internal/runtime/pipeline"
	storerunlifecycle "swarm/internal/store/runlifecycle"
)

const destructiveResetPipelineSubscriberID = "pipeline"

type destructiveResetDeliveryTarget struct {
	DeliveryID      string
	RunID           string
	EventID         string
	SubscriberType  string
	SubscriberID    string
	Status          string
	ReasonCode      string
	ActiveSessionID string
}

func (s *PostgresStore) ApplyDestructiveResetQuiescence(ctx context.Context, req destructivereset.QuiescenceRequest) (destructivereset.QuiescenceResult, error) {
	if s == nil || s.DB == nil {
		return destructivereset.QuiescenceResult{}, fmt.Errorf("postgres store is required")
	}
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return destructivereset.QuiescenceResult{}, err
	}
	if !caps.Events.HasRuns {
		return destructivereset.QuiescenceResult{}, fmt.Errorf("runs table is required")
	}
	if caps.Events.Deliveries != SchemaFlavorCanonical || !caps.Events.DeliveryRunID {
		if caps.Events.Deliveries != SchemaFlavorCanonical {
			return destructivereset.QuiescenceResult{}, unsupportedSchemaCapability("event_deliveries", caps.Events.Deliveries)
		}
		return destructivereset.QuiescenceResult{}, fmt.Errorf("destructive reset quiescence requires canonical event_deliveries.run_id")
	}
	if caps.Events.Receipts != SchemaFlavorCanonical {
		return destructivereset.QuiescenceResult{}, unsupportedSchemaCapability("event_receipts", caps.Events.Receipts)
	}
	runIDs := activeRunIDsFromResetPlan(req.Result.Plan)
	now := req.RequestedAt.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	out := destructivereset.QuiescenceResult{
		OperationName: strings.TrimSpace(req.Result.OperationName),
		DryRun:        req.Result.DryRun,
		AppliedAt:     now,
		ReasonCode:    destructivereset.QuiescenceReasonCode,
		ControlledBy:  destructivereset.QuiescenceControlledBy,
	}
	if out.OperationName == "" {
		out.OperationName = destructivereset.DefaultOperationName
	}
	if len(runIDs) == 0 {
		return out, nil
	}

	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return destructivereset.QuiescenceResult{}, fmt.Errorf("begin destructive reset quiescence tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	runs, err := s.lockDestructiveResetRunsTx(ctx, tx, runIDs)
	if err != nil {
		return destructivereset.QuiescenceResult{}, err
	}
	deliveries, err := s.lockDestructiveResetDeliveriesTx(ctx, tx, runIDs)
	if err != nil {
		return destructivereset.QuiescenceResult{}, err
	}
	for _, delivery := range deliveries {
		out.Deliveries = append(out.Deliveries, destructivereset.QuiescedDelivery{
			DeliveryID:      delivery.DeliveryID,
			RunID:           delivery.RunID,
			EventID:         delivery.EventID,
			SubscriberType:  delivery.SubscriberType,
			SubscriberID:    delivery.SubscriberID,
			PreviousStatus:  delivery.Status,
			Status:          "dead_letter",
			ReasonCode:      destructivereset.QuiescenceReasonCode,
			PreviousReason:  delivery.ReasonCode,
			ActiveSessionID: delivery.ActiveSessionID,
			Changed:         delivery.Status != "dead_letter" || delivery.ReasonCode != destructivereset.QuiescenceReasonCode,
		})
	}
	for _, run := range runs {
		nextStatus := run.Status
		changed := false
		if destructiveResetRunStatusActive(run.Status) {
			nextStatus = "cancelled"
			changed = true
		}
		out.Runs = append(out.Runs, destructivereset.QuiescedRun{
			RunID:          run.RunID,
			PreviousStatus: run.Status,
			Status:         nextStatus,
			ReasonCode:     destructivereset.QuiescenceReasonCode,
			Changed:        changed,
		})
	}
	if req.Result.DryRun {
		return out, nil
	}

	eventIDs := map[string]struct{}{}
	for _, delivery := range deliveries {
		if err := s.terminalizeDestructiveResetDeliveryTx(ctx, tx, delivery, now); err != nil {
			return destructivereset.QuiescenceResult{}, err
		}
		if delivery.EventID != "" {
			eventIDs[delivery.EventID] = struct{}{}
		}
	}
	for eventID := range eventIDs {
		if err := s.upsertDestructiveResetPipelineReceiptTx(ctx, tx, eventID, now); err != nil {
			return destructivereset.QuiescenceResult{}, err
		}
		out.PipelineReceiptCount++
	}
	for _, run := range runs {
		if !destructiveResetRunStatusActive(run.Status) {
			continue
		}
		if _, err := storerunlifecycle.MarkTerminal(ctx, tx, run.RunID, "cancelled", "", now, runLifecycleOptions(caps)); err != nil {
			return destructivereset.QuiescenceResult{}, fmt.Errorf("mark destructive reset run terminal: %w", err)
		}
		if err := s.upsertDestructiveResetRunControlTx(ctx, tx, run.RunID, now); err != nil {
			return destructivereset.QuiescenceResult{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		return destructivereset.QuiescenceResult{}, fmt.Errorf("commit destructive reset quiescence tx: %w", err)
	}
	return out, nil
}

func activeRunIDsFromResetPlan(plan destructivereset.Plan) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(plan.ActiveRuns))
	for _, run := range plan.ActiveRuns {
		if !destructiveResetRunStatusActive(run.Status) {
			continue
		}
		id := nullUUIDString(run.RunID)
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

func destructiveResetRunStatusActive(status string) bool {
	switch strings.TrimSpace(strings.ToLower(status)) {
	case "running", "paused":
		return true
	default:
		return false
	}
}

func (s *PostgresStore) lockDestructiveResetRunsTx(ctx context.Context, tx *sql.Tx, runIDs []string) ([]destructivereset.QuiescedRun, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT run_id::text, COALESCE(status, '')
		FROM runs
		WHERE run_id = ANY($1::uuid[])
		ORDER BY run_id::text
		FOR UPDATE
	`, pq.Array(runIDs))
	if err != nil {
		return nil, fmt.Errorf("lock destructive reset runs: %w", err)
	}
	defer rows.Close()
	var out []destructivereset.QuiescedRun
	for rows.Next() {
		var run destructivereset.QuiescedRun
		if err := rows.Scan(&run.RunID, &run.PreviousStatus); err != nil {
			return nil, fmt.Errorf("scan destructive reset run: %w", err)
		}
		run.RunID = strings.TrimSpace(run.RunID)
		run.PreviousStatus = strings.TrimSpace(run.PreviousStatus)
		run.Status = run.PreviousStatus
		out = append(out, run)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read destructive reset runs: %w", err)
	}
	return out, nil
}

func (s *PostgresStore) lockDestructiveResetDeliveriesTx(ctx context.Context, tx *sql.Tx, runIDs []string) ([]destructiveResetDeliveryTarget, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT
			d.delivery_id::text,
			d.run_id::text,
			d.event_id::text,
			COALESCE(d.subscriber_type, ''),
			COALESCE(d.subscriber_id, ''),
			COALESCE(d.status, ''),
			COALESCE(d.reason_code, ''),
			COALESCE(d.active_session_id::text, '')
		FROM event_deliveries d
		WHERE d.run_id = ANY($1::uuid[])
		  AND d.subscriber_type IN ('agent', 'node')
		  AND `+destructiveResetQuiescenceDeliveryPredicateSQL("d")+`
		ORDER BY d.run_id::text, d.event_id::text, d.subscriber_type, d.subscriber_id
		FOR UPDATE
	`, pq.Array(runIDs))
	if err != nil {
		return nil, fmt.Errorf("lock destructive reset deliveries: %w", err)
	}
	defer rows.Close()
	var out []destructiveResetDeliveryTarget
	for rows.Next() {
		var item destructiveResetDeliveryTarget
		if err := rows.Scan(&item.DeliveryID, &item.RunID, &item.EventID, &item.SubscriberType, &item.SubscriberID, &item.Status, &item.ReasonCode, &item.ActiveSessionID); err != nil {
			return nil, fmt.Errorf("scan destructive reset delivery: %w", err)
		}
		item.DeliveryID = strings.TrimSpace(item.DeliveryID)
		item.RunID = strings.TrimSpace(item.RunID)
		item.EventID = strings.TrimSpace(item.EventID)
		item.SubscriberType = strings.TrimSpace(item.SubscriberType)
		item.SubscriberID = strings.TrimSpace(item.SubscriberID)
		item.Status = strings.TrimSpace(item.Status)
		item.ReasonCode = strings.TrimSpace(item.ReasonCode)
		item.ActiveSessionID = strings.TrimSpace(item.ActiveSessionID)
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read destructive reset deliveries: %w", err)
	}
	return out, nil
}

func (s *PostgresStore) terminalizeDestructiveResetDeliveryTx(ctx context.Context, tx *sql.Tx, item destructiveResetDeliveryTarget, at time.Time) error {
	if _, err := tx.ExecContext(ctx, `
		UPDATE event_deliveries
		SET
			status = 'dead_letter',
			reason_code = $2,
			last_error = $3,
			active_session_id = NULL,
			delivered_at = COALESCE(delivered_at, $4)
		WHERE delivery_id = $1::uuid
		  AND `+destructiveResetQuiescenceDeliveryPredicateSQL("")+`
	`, item.DeliveryID, destructivereset.QuiescenceReasonCode, destructivereset.QuiescenceDeliveryNote, at.UTC()); err != nil {
		return fmt.Errorf("terminalize destructive reset delivery %s: %w", item.DeliveryID, err)
	}
	sideEffects, err := json.Marshal(map[string]any{
		"manager_status": "dead_letter",
		"reason_code":    destructivereset.QuiescenceReasonCode,
		"error":          destructivereset.QuiescenceDeliveryNote,
	})
	if err != nil {
		return fmt.Errorf("marshal destructive reset receipt side effects: %w", err)
	}
	idempotencyKey := ""
	if item.SubscriberType == "node" {
		idempotencyKey = runtimepipeline.SystemNodeReceiptIdempotencyKey(item.SubscriberID, item.EventID)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO event_receipts (
			event_id, subscriber_type, subscriber_id, entity_id, flow_instance,
			outcome, reason_code, side_effects, idempotency_key, processed_at
		)
		SELECT
			e.event_id, $2, $3, e.entity_id, e.flow_instance,
			'dead_letter', $4, $5::jsonb, NULLIF($6, ''), $7
		FROM events e
		WHERE e.event_id = $1::uuid
		ON CONFLICT (event_id, subscriber_id) DO UPDATE SET
			outcome = 'dead_letter',
			reason_code = $4,
			side_effects = $5::jsonb,
			idempotency_key = COALESCE(NULLIF($6, ''), event_receipts.idempotency_key),
			processed_at = $7
	`, item.EventID, item.SubscriberType, item.SubscriberID, destructivereset.QuiescenceReasonCode, string(sideEffects), idempotencyKey, at.UTC()); err != nil {
		return fmt.Errorf("upsert destructive reset delivery receipt: %w", err)
	}
	return nil
}

func (s *PostgresStore) upsertDestructiveResetPipelineReceiptTx(ctx context.Context, tx *sql.Tx, eventID string, at time.Time) error {
	sideEffects, err := marshalPipelineReceiptSideEffects(newPipelineReceiptSideEffects("dead_letter", destructivereset.QuiescenceReasonCode, destructivereset.QuiescenceDeliveryNote))
	if err != nil {
		return fmt.Errorf("marshal destructive reset pipeline receipt: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO event_receipts (
			event_id, subscriber_type, subscriber_id, entity_id, flow_instance,
			outcome, reason_code, side_effects, processed_at
		)
		SELECT
			e.event_id, 'platform', $2, e.entity_id, e.flow_instance,
			'dead_letter', $3, $4::jsonb, $5
		FROM events e
		WHERE e.event_id = $1::uuid
		ON CONFLICT (event_id, subscriber_id) DO UPDATE SET
			outcome = 'dead_letter',
			reason_code = $3,
			side_effects = $4::jsonb,
			processed_at = $5
	`, eventID, destructiveResetPipelineSubscriberID, destructivereset.QuiescenceReasonCode, string(sideEffects), at.UTC()); err != nil {
		return fmt.Errorf("upsert destructive reset pipeline receipt: %w", err)
	}
	return nil
}

func (s *PostgresStore) upsertDestructiveResetRunControlTx(ctx context.Context, tx *sql.Tx, runID string, at time.Time) error {
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO run_control_state (run_id, control_status, reason, controlled_by, updated_at, paused_at, stopped_at)
		VALUES ($1::uuid, 'stopped', $2, $3, $4, NULL, $4)
		ON CONFLICT (run_id) DO UPDATE SET
			control_status = 'stopped',
			reason = $2,
			controlled_by = $3,
			updated_at = $4,
			paused_at = NULL,
			stopped_at = COALESCE(run_control_state.stopped_at, $4)
	`, runID, destructivereset.QuiescenceReasonCode, destructivereset.QuiescenceControlledBy, at.UTC()); err != nil {
		return fmt.Errorf("persist destructive reset run control state: %w", err)
	}
	return nil
}

func destructiveResetDeliveryTerminal(status, reasonCode string) bool {
	return strings.TrimSpace(status) == "dead_letter" && strings.TrimSpace(reasonCode) == destructivereset.QuiescenceReasonCode
}

func destructiveResetQuiescenceDeliveryPredicateSQL(alias string) string {
	prefix := ""
	if strings.TrimSpace(alias) != "" {
		prefix = strings.TrimSpace(alias) + "."
	}
	return `(
			` + prefix + `status IN ('pending', 'in_progress')
			OR (
				` + prefix + `status = 'failed'
				AND COALESCE(` + prefix + `retry_count, 0) < 2
			)
		)`
}
