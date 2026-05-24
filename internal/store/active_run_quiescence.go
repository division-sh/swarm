package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/lib/pq"
	runtimedestructivereset "swarm/internal/runtime/destructivereset"
	runtimepipeline "swarm/internal/runtime/pipeline"
	runtimerunquiescence "swarm/internal/runtime/runquiescence"
	storerunlifecycle "swarm/internal/store/runlifecycle"
)

const activeRunQuiescencePipelineSubscriberID = "pipeline"

type activeRunQuiescenceDeliveryTarget struct {
	DeliveryID      string
	RunID           string
	EventID         string
	SubscriberType  string
	SubscriberID    string
	Status          string
	ReasonCode      string
	ActiveSessionID string
}

func (s *PostgresStore) ApplyServeAbandonActiveRunQuiescence(ctx context.Context, at time.Time) (runtimerunquiescence.Result, error) {
	return s.ApplyActiveRunQuiescence(ctx, runtimerunquiescence.Request{
		OperationName: runtimerunquiescence.ServeAbandonOperationName,
		RequestedAt:   at,
		AllActiveRuns: true,
		ReasonCode:    runtimerunquiescence.ServeAbandonReasonCode,
		ControlledBy:  runtimerunquiescence.ServeAbandonControlledBy,
		DeliveryNote:  runtimerunquiescence.ServeAbandonDeliveryNote,
	})
}

func (s *PostgresStore) ApplyActiveRunQuiescence(ctx context.Context, req runtimerunquiescence.Request) (runtimerunquiescence.Result, error) {
	if s == nil || s.DB == nil {
		return runtimerunquiescence.Result{}, fmt.Errorf("postgres store is required")
	}
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return runtimerunquiescence.Result{}, err
	}
	if !caps.Events.HasRuns {
		return runtimerunquiescence.Result{}, fmt.Errorf("runs table is required")
	}
	if caps.Events.Deliveries != SchemaFlavorCanonical || !caps.Events.DeliveryRunID {
		if caps.Events.Deliveries != SchemaFlavorCanonical {
			return runtimerunquiescence.Result{}, unsupportedSchemaCapability("event_deliveries", caps.Events.Deliveries)
		}
		return runtimerunquiescence.Result{}, fmt.Errorf("active run quiescence requires canonical event_deliveries.run_id")
	}
	if caps.Events.Receipts != SchemaFlavorCanonical {
		return runtimerunquiescence.Result{}, unsupportedSchemaCapability("event_receipts", caps.Events.Receipts)
	}
	now := req.RequestedAt.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	out := runtimerunquiescence.Result{
		OperationName: strings.TrimSpace(req.OperationName),
		DryRun:        req.DryRun,
		AppliedAt:     now,
		ReasonCode:    strings.TrimSpace(req.ReasonCode),
		ControlledBy:  strings.TrimSpace(req.ControlledBy),
	}
	if out.OperationName == "" {
		return out, fmt.Errorf("active run quiescence operation_name is required")
	}
	if out.ReasonCode == "" {
		return out, fmt.Errorf("active run quiescence reason_code is required")
	}
	if out.ControlledBy == "" {
		return out, fmt.Errorf("active run quiescence controlled_by is required")
	}
	deliveryNote := strings.TrimSpace(req.DeliveryNote)
	if deliveryNote == "" {
		deliveryNote = out.ReasonCode
	}

	runIDs := normalizeQuiescenceRunIDs(req.RunIDs)
	if len(runIDs) == 0 && !req.AllActiveRuns {
		return out, nil
	}

	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return runtimerunquiescence.Result{}, fmt.Errorf("begin active run quiescence tx: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	var runs []runtimerunquiescence.QuiescedRun
	if req.AllActiveRuns {
		runs, err = lockAllActiveQuiescenceRunsTx(ctx, tx)
	} else {
		runs, err = lockActiveRunQuiescenceRunsTx(ctx, tx, runIDs)
	}
	if err != nil {
		return runtimerunquiescence.Result{}, err
	}
	runIDs = quiescenceRunIDs(runs)
	if len(runIDs) == 0 {
		return out, nil
	}
	deliveries, err := lockActiveRunQuiescenceDeliveriesTx(ctx, tx, runIDs)
	if err != nil {
		return runtimerunquiescence.Result{}, err
	}
	for _, delivery := range deliveries {
		out.Deliveries = append(out.Deliveries, runtimerunquiescence.QuiescedDelivery{
			DeliveryID:      delivery.DeliveryID,
			RunID:           delivery.RunID,
			EventID:         delivery.EventID,
			SubscriberType:  delivery.SubscriberType,
			SubscriberID:    delivery.SubscriberID,
			PreviousStatus:  delivery.Status,
			Status:          "dead_letter",
			ReasonCode:      out.ReasonCode,
			PreviousReason:  delivery.ReasonCode,
			ActiveSessionID: delivery.ActiveSessionID,
			Changed:         delivery.Status != "dead_letter" || delivery.ReasonCode != out.ReasonCode,
		})
	}
	for _, run := range runs {
		nextStatus := run.Status
		changed := false
		if activeRunQuiescenceRunStatusActive(run.Status) {
			nextStatus = "cancelled"
			changed = true
		}
		out.Runs = append(out.Runs, runtimerunquiescence.QuiescedRun{
			RunID:          run.RunID,
			PreviousStatus: run.Status,
			Status:         nextStatus,
			ReasonCode:     out.ReasonCode,
			Changed:        changed,
		})
	}
	if req.DryRun {
		return out, nil
	}

	eventIDs := map[string]struct{}{}
	for _, delivery := range deliveries {
		if err := terminalizeActiveRunQuiescenceDeliveryTx(ctx, tx, delivery, out.ReasonCode, deliveryNote, now); err != nil {
			return runtimerunquiescence.Result{}, err
		}
		if delivery.EventID != "" {
			eventIDs[delivery.EventID] = struct{}{}
		}
	}
	for eventID := range eventIDs {
		if err := upsertActiveRunQuiescencePipelineReceiptTx(ctx, tx, eventID, out.ReasonCode, deliveryNote, now); err != nil {
			return runtimerunquiescence.Result{}, err
		}
		out.PipelineReceiptCount++
	}
	for _, run := range runs {
		if !activeRunQuiescenceRunStatusActive(run.Status) {
			continue
		}
		if _, err := storerunlifecycle.MarkTerminal(ctx, tx, run.RunID, "cancelled", "", now, runLifecycleOptions(caps)); err != nil {
			return runtimerunquiescence.Result{}, fmt.Errorf("mark active run quiescence run terminal: %w", err)
		}
		if err := upsertActiveRunQuiescenceRunControlTx(ctx, tx, run.RunID, out.ReasonCode, out.ControlledBy, now); err != nil {
			return runtimerunquiescence.Result{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		return runtimerunquiescence.Result{}, fmt.Errorf("commit active run quiescence tx: %w", err)
	}
	committed = true
	return out, nil
}

func normalizeQuiescenceRunIDs(runIDs []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(runIDs))
	for _, raw := range runIDs {
		id := nullUUIDString(raw)
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

func quiescenceRunIDs(runs []runtimerunquiescence.QuiescedRun) []string {
	out := make([]string, 0, len(runs))
	for _, run := range runs {
		if id := nullUUIDString(run.RunID); id != "" {
			out = append(out, id)
		}
	}
	return out
}

func activeRunQuiescenceRunStatusActive(status string) bool {
	switch strings.TrimSpace(strings.ToLower(status)) {
	case "running", "paused":
		return true
	default:
		return false
	}
}

func lockAllActiveQuiescenceRunsTx(ctx context.Context, tx *sql.Tx) ([]runtimerunquiescence.QuiescedRun, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT run_id::text, COALESCE(status, '')
		FROM runs
		WHERE lower(COALESCE(status, '')) IN ('running', 'paused')
		ORDER BY run_id::text
		FOR UPDATE
	`)
	if err != nil {
		return nil, fmt.Errorf("lock all active quiescence runs: %w", err)
	}
	return scanActiveRunQuiescenceRuns(rows)
}

func lockActiveRunQuiescenceRunsTx(ctx context.Context, tx *sql.Tx, runIDs []string) ([]runtimerunquiescence.QuiescedRun, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT run_id::text, COALESCE(status, '')
		FROM runs
		WHERE run_id = ANY($1::uuid[])
		  AND lower(COALESCE(status, '')) IN ('running', 'paused')
		ORDER BY run_id::text
		FOR UPDATE
	`, pq.Array(runIDs))
	if err != nil {
		return nil, fmt.Errorf("lock active quiescence runs: %w", err)
	}
	return scanActiveRunQuiescenceRuns(rows)
}

func scanActiveRunQuiescenceRuns(rows *sql.Rows) ([]runtimerunquiescence.QuiescedRun, error) {
	defer rows.Close()
	var out []runtimerunquiescence.QuiescedRun
	for rows.Next() {
		var run runtimerunquiescence.QuiescedRun
		if err := rows.Scan(&run.RunID, &run.PreviousStatus); err != nil {
			return nil, fmt.Errorf("scan active quiescence run: %w", err)
		}
		run.RunID = strings.TrimSpace(run.RunID)
		run.PreviousStatus = strings.TrimSpace(run.PreviousStatus)
		run.Status = run.PreviousStatus
		out = append(out, run)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read active quiescence runs: %w", err)
	}
	return out, nil
}

func lockActiveRunQuiescenceDeliveriesTx(ctx context.Context, tx *sql.Tx, runIDs []string) ([]activeRunQuiescenceDeliveryTarget, error) {
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
		  AND `+activeRunQuiescenceDeliveryPredicateSQL("d")+`
		ORDER BY d.run_id::text, d.event_id::text, d.subscriber_type, d.subscriber_id
		FOR UPDATE
	`, pq.Array(runIDs))
	if err != nil {
		return nil, fmt.Errorf("lock active run quiescence deliveries: %w", err)
	}
	defer rows.Close()
	var out []activeRunQuiescenceDeliveryTarget
	for rows.Next() {
		var item activeRunQuiescenceDeliveryTarget
		if err := rows.Scan(&item.DeliveryID, &item.RunID, &item.EventID, &item.SubscriberType, &item.SubscriberID, &item.Status, &item.ReasonCode, &item.ActiveSessionID); err != nil {
			return nil, fmt.Errorf("scan active run quiescence delivery: %w", err)
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
		return nil, fmt.Errorf("read active run quiescence deliveries: %w", err)
	}
	return out, nil
}

func terminalizeActiveRunQuiescenceDeliveryTx(ctx context.Context, tx *sql.Tx, item activeRunQuiescenceDeliveryTarget, reasonCode, note string, at time.Time) error {
	if _, err := tx.ExecContext(ctx, `
		UPDATE event_deliveries
		SET
			status = 'dead_letter',
			reason_code = $2,
			last_error = $3,
			active_session_id = NULL,
			delivered_at = COALESCE(delivered_at, $4)
		WHERE delivery_id = $1::uuid
		  AND `+activeRunQuiescenceDeliveryPredicateSQL("")+`
	`, item.DeliveryID, reasonCode, note, at.UTC()); err != nil {
		return fmt.Errorf("terminalize active run quiescence delivery %s: %w", item.DeliveryID, err)
	}
	sideEffects, err := json.Marshal(map[string]any{
		"manager_status": "dead_letter",
		"reason_code":    reasonCode,
		"error":          note,
	})
	if err != nil {
		return fmt.Errorf("marshal active run quiescence receipt side effects: %w", err)
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
		ON CONFLICT (event_id, subscriber_type, subscriber_id) DO UPDATE SET
			outcome = 'dead_letter',
			reason_code = $4,
			side_effects = $5::jsonb,
			idempotency_key = COALESCE(NULLIF($6, ''), event_receipts.idempotency_key),
			processed_at = $7
	`, item.EventID, item.SubscriberType, item.SubscriberID, reasonCode, string(sideEffects), idempotencyKey, at.UTC()); err != nil {
		return fmt.Errorf("upsert active run quiescence delivery receipt: %w", err)
	}
	return nil
}

func upsertActiveRunQuiescencePipelineReceiptTx(ctx context.Context, tx *sql.Tx, eventID, reasonCode, note string, at time.Time) error {
	sideEffects, err := marshalPipelineReceiptSideEffects(newPipelineReceiptSideEffects("dead_letter", reasonCode, note))
	if err != nil {
		return fmt.Errorf("marshal active run quiescence pipeline receipt: %w", err)
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
		ON CONFLICT (event_id, subscriber_type, subscriber_id) DO UPDATE SET
			outcome = 'dead_letter',
			reason_code = $3,
			side_effects = $4::jsonb,
			processed_at = $5
	`, eventID, activeRunQuiescencePipelineSubscriberID, reasonCode, string(sideEffects), at.UTC()); err != nil {
		return fmt.Errorf("upsert active run quiescence pipeline receipt: %w", err)
	}
	return nil
}

func upsertActiveRunQuiescenceRunControlTx(ctx context.Context, tx *sql.Tx, runID, reasonCode, controlledBy string, at time.Time) error {
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
	`, runID, reasonCode, controlledBy, at.UTC()); err != nil {
		return fmt.Errorf("persist active run quiescence run control state: %w", err)
	}
	return nil
}

func activeRunQuiescenceDeliveryPredicateSQL(alias string) string {
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

func activeRunQuiescenceDeliveryTerminal(status, reasonCode string) bool {
	if strings.TrimSpace(status) != "dead_letter" {
		return false
	}
	reasonCode = strings.TrimSpace(reasonCode)
	for _, terminalReason := range activeRunQuiescenceTerminalReasonCodes() {
		if reasonCode == terminalReason {
			return true
		}
	}
	return false
}

func activeRunQuiescenceTerminalReasonCodes() []string {
	return []string{
		runtimedestructivereset.QuiescenceReasonCode,
		runtimerunquiescence.ServeAbandonReasonCode,
	}
}
