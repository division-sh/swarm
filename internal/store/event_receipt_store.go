package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"swarm/internal/events"
	"swarm/internal/runtime/core/eventidentity"
	runtimemanager "swarm/internal/runtime/manager"
)

func (s *PostgresStore) MarkEventDeliveryInProgress(ctx context.Context, eventID, agentID, sessionID string) error {
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return err
	}
	eventID = strings.TrimSpace(eventID)
	agentID = strings.TrimSpace(agentID)
	if eventID == "" || agentID == "" {
		return fmt.Errorf("mark event delivery in progress: eventID and agentID required")
	}
	sessionID = sanitizeOptionalUUID(sessionID)
	switch caps.Events.Deliveries {
	case SchemaFlavorCanonical:
		return s.markEventDeliveryInProgressSpec(ctx, eventID, agentID, sessionID)
	default:
		return unsupportedSchemaCapability("event_deliveries", caps.Events.Deliveries)
	}
}

func (s *PostgresStore) UpsertEventReceipt(ctx context.Context, eventID, agentID string, status runtimemanager.ReceiptStatus, errText string) error {
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return err
	}
	eventID = strings.TrimSpace(eventID)
	agentID = strings.TrimSpace(agentID)
	if eventID == "" || agentID == "" {
		return fmt.Errorf("upsert event receipt: eventID and agentID required")
	}
	if status == "" {
		return fmt.Errorf("upsert event receipt: status required")
	}
	switch caps.Events.Receipts {
	case SchemaFlavorCanonical:
		return s.upsertAgentReceiptSpec(ctx, eventID, agentID, status, errText)
	default:
		return unsupportedSchemaCapability("event_receipts", caps.Events.Receipts)
	}
}

func (s *PostgresStore) ListPendingEventsForAgent(ctx context.Context, agentID string, since time.Time, limit int) ([]events.Event, error) {
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return nil, err
	}
	if agentID == "" {
		return nil, nil
	}
	if limit <= 0 {
		limit = 200
	}
	if since.IsZero() {
		since = time.Now().Add(-30 * 24 * time.Hour)
	}
	switch {
	case caps.Events.Log == SchemaFlavorCanonical && caps.Events.Deliveries == SchemaFlavorCanonical && caps.Events.Receipts == SchemaFlavorCanonical:
		return s.listPendingEventsForAgentSpec(ctx, agentID, since, limit)
	case caps.Events.Log != SchemaFlavorCanonical:
		return nil, unsupportedSchemaCapability("events", caps.Events.Log)
	case caps.Events.Deliveries != SchemaFlavorCanonical:
		return nil, unsupportedSchemaCapability("event_deliveries", caps.Events.Deliveries)
	case caps.Events.Receipts != SchemaFlavorCanonical:
		return nil, unsupportedSchemaCapability("event_receipts", caps.Events.Receipts)
	}
	return nil, nil
}

func (s *PostgresStore) ListPendingSubscribedEvents(
	ctx context.Context,
	agentID string,
	subscriptions []events.EventType,
	since time.Time,
	limit int,
) ([]events.Event, error) {
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return nil, err
	}
	if agentID == "" || len(subscriptions) == 0 {
		return nil, nil
	}
	if limit <= 0 {
		limit = 500
	}
	if since.IsZero() {
		since = time.Now().Add(-30 * 24 * time.Hour)
	}

	switch {
	case caps.Events.Log == SchemaFlavorCanonical && caps.Events.Deliveries == SchemaFlavorCanonical && caps.Events.Receipts == SchemaFlavorCanonical:
		return s.listPendingSubscribedEventsSpec(ctx, agentID, subscriptions, since, limit)
	case caps.Events.Log != SchemaFlavorCanonical:
		return nil, unsupportedSchemaCapability("events", caps.Events.Log)
	case caps.Events.Deliveries != SchemaFlavorCanonical:
		return nil, unsupportedSchemaCapability("event_deliveries", caps.Events.Deliveries)
	case caps.Events.Receipts != SchemaFlavorCanonical:
		return nil, unsupportedSchemaCapability("event_receipts", caps.Events.Receipts)
	}
	return nil, nil
}

func (s *PostgresStore) GetEventReceipt(ctx context.Context, eventID, agentID string) (runtimemanager.EventReceipt, bool, error) {
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return runtimemanager.EventReceipt{}, false, err
	}
	eventID = strings.TrimSpace(eventID)
	agentID = strings.TrimSpace(agentID)
	if eventID == "" || agentID == "" {
		return runtimemanager.EventReceipt{}, false, fmt.Errorf("event_id and agent_id are required")
	}
	switch caps.Events.Receipts {
	case SchemaFlavorCanonical:
		return s.getEventReceiptSpec(ctx, eventID, agentID)
	default:
		return runtimemanager.EventReceipt{}, false, unsupportedSchemaCapability("event_receipts", caps.Events.Receipts)
	}
}

func (s *PostgresStore) upsertAgentReceiptSpec(ctx context.Context, eventID, agentID string, status runtimemanager.ReceiptStatus, errText string) error {
	var retryCount int
	_ = s.DB.QueryRowContext(ctx, `
		SELECT COALESCE((side_effects->>'retry_count')::int, 0)
		FROM event_receipts
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'agent'
		  AND subscriber_id = $2
	`, eventID, agentID).Scan(&retryCount)
	if status == runtimemanager.ReceiptStatusError {
		retryCount++
	}
	finalStatus := status
	if status == runtimemanager.ReceiptStatusError && retryCount >= 2 {
		finalStatus = runtimemanager.ReceiptStatusDeadLetter
	}
	reasonCode := managerReceiptReasonCode(finalStatus, errText)
	sideEffects, err := json.Marshal(map[string]any{
		"manager_status": finalStatus,
		"reason_code":    reasonCode,
		"retry_count":    retryCount,
		"error":          strings.TrimSpace(errText),
	})
	if err != nil {
		return fmt.Errorf("marshal event receipt side effects: %w", err)
	}
	outcome := mapManagerReceiptStatusToOutcome(finalStatus)
	const q = `
		INSERT INTO event_receipts (
			event_id, subscriber_type, subscriber_id, entity_id, flow_instance,
			outcome, reason_code, side_effects, processed_at
		)
		SELECT
			e.event_id, 'agent', $2, e.entity_id, e.flow_instance,
			$3, NULLIF($4,''), $5::jsonb, now()
		FROM events e
		WHERE e.event_id = $1::uuid
		ON CONFLICT (event_id, subscriber_id) DO UPDATE SET
			entity_id = EXCLUDED.entity_id,
			flow_instance = EXCLUDED.flow_instance,
			outcome = EXCLUDED.outcome,
			reason_code = EXCLUDED.reason_code,
			side_effects = EXCLUDED.side_effects,
			processed_at = now()
	`
	if _, err := s.DB.ExecContext(ctx, q, eventID, agentID, outcome, reasonCode, string(sideEffects)); err != nil {
		return fmt.Errorf("upsert event receipt: %w", err)
	}
	if err := s.syncAgentDeliverySpec(ctx, eventID, agentID, finalStatus, reasonCode, retryCount, errText); err != nil {
		return err
	}
	return nil
}

func (s *PostgresStore) syncAgentDeliverySpec(
	ctx context.Context,
	eventID, agentID string,
	status runtimemanager.ReceiptStatus,
	reasonCode string,
	retryCount int,
	errText string,
) error {
	deliveryStatus := "delivered"
	switch status {
	case runtimemanager.ReceiptStatusError:
		deliveryStatus = "failed"
	case runtimemanager.ReceiptStatusDeadLetter:
		deliveryStatus = "dead_letter"
	}
	const q = `
		UPDATE event_deliveries
		SET
			status = $3,
			retry_count = $4,
			reason_code = NULLIF($5, ''),
			last_error = NULLIF($6, ''),
			active_session_id = NULL,
			delivered_at = now()
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'agent'
		  AND subscriber_id = $2
	`
	if _, err := s.DB.ExecContext(ctx, q, eventID, agentID, deliveryStatus, retryCount, reasonCode, strings.TrimSpace(errText)); err != nil {
		return fmt.Errorf("sync event delivery: %w", err)
	}
	return nil
}

func (s *PostgresStore) markEventDeliveryInProgressSpec(ctx context.Context, eventID, agentID, sessionID string) error {
	const q = `
		UPDATE event_deliveries
		SET
			status = 'in_progress',
			reason_code = 'agent_processing',
			last_error = NULL,
			active_session_id = COALESCE(NULLIF($3, '')::uuid, active_session_id),
			started_at = COALESCE(started_at, now()),
			delivered_at = NULL
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'agent'
		  AND subscriber_id = $2
	`
	if _, err := s.DB.ExecContext(ctx, q, eventID, agentID, sessionID); err != nil {
		return fmt.Errorf("mark event delivery in progress: %w", err)
	}
	return nil
}

func (s *PostgresStore) listPendingEventsForAgentSpec(ctx context.Context, agentID string, since time.Time, limit int) ([]events.Event, error) {
	q := fmt.Sprintf(`
		SELECT
			e.event_id::text, COALESCE(e.run_id::text, ''), e.event_name, COALESCE(e.produced_by, ''),
			COALESCE(e.entity_id::text, ''), COALESCE(e.flow_instance, ''), COALESCE(e.scope, 'global'),
			e.payload, e.created_at,
			COALESCE(e.source_event_id::text, '')
		FROM event_deliveries d
		INNER JOIN events e ON e.event_id = d.event_id
		LEFT JOIN event_receipts r
			ON r.event_id = d.event_id
			AND r.subscriber_type = 'agent'
			AND r.subscriber_id = d.subscriber_id
		WHERE d.subscriber_type = 'agent'
		  AND d.subscriber_id = $1
		  AND e.created_at >= $2
		  AND (
				r.event_id IS NULL
				OR (
					COALESCE(r.side_effects->>'manager_status', '') = 'error'
					AND COALESCE((r.side_effects->>'retry_count')::int, 0) <= 1
					AND (
						(COALESCE((r.side_effects->>'retry_count')::int, 0) = 1 AND r.processed_at <= now() - interval '1 minute')
					)
				)
			)
		ORDER BY e.created_at ASC
		LIMIT $3
	`)
	rows, err := s.DB.QueryContext(ctx, q, agentID, since, limit)
	if err != nil {
		return nil, fmt.Errorf("query pending events for %s: %w", agentID, err)
	}
	defer rows.Close()
	return scanSpecPendingEvents(rows)
}

func (s *PostgresStore) listPendingSubscribedEventsSpec(ctx context.Context, agentID string, subscriptions []events.EventType, since time.Time, limit int) ([]events.Event, error) {
	q := fmt.Sprintf(`
		SELECT
			e.event_id::text, COALESCE(e.run_id::text, ''), e.event_name, COALESCE(e.produced_by, ''),
			COALESCE(e.entity_id::text, ''), COALESCE(e.flow_instance, ''), COALESCE(e.scope, 'global'),
			e.payload, e.created_at,
			COALESCE(e.source_event_id::text, '')
		FROM events e
		LEFT JOIN event_receipts r
			ON r.event_id = e.event_id
			AND r.subscriber_type = 'agent'
			AND r.subscriber_id = $1
		WHERE e.created_at >= $2
		  AND (
				NOT EXISTS (
					SELECT 1
					FROM event_deliveries d_any
					WHERE d_any.event_id = e.event_id
				)
				OR EXISTS (
					SELECT 1
					FROM event_deliveries d_me
					WHERE d_me.event_id = e.event_id
					  AND d_me.subscriber_type = 'agent'
					  AND d_me.subscriber_id = $1
				)
			)
		  AND (
				r.event_id IS NULL
				OR (
					COALESCE(r.side_effects->>'manager_status', '') = 'error'
					AND COALESCE((r.side_effects->>'retry_count')::int, 0) <= 1
					AND (
						(COALESCE((r.side_effects->>'retry_count')::int, 0) = 1 AND r.processed_at <= now() - interval '1 minute')
					)
				)
			)
		ORDER BY e.created_at ASC
		LIMIT $3
	`)
	rows, err := s.DB.QueryContext(ctx, q, agentID, since, limit)
	if err != nil {
		return nil, fmt.Errorf("query pending subscribed events for %s: %w", agentID, err)
	}
	defer rows.Close()
	out, err := scanSpecPendingEvents(rows)
	if err != nil {
		return nil, err
	}
	filtered := make([]events.Event, 0, len(out))
	for _, evt := range out {
		for _, subscription := range subscriptions {
			if eventidentity.MatchPattern(string(subscription), string(evt.Type)) {
				filtered = append(filtered, evt)
				break
			}
		}
	}
	return filtered, nil
}

func (s *PostgresStore) getEventReceiptSpec(ctx context.Context, eventID, agentID string) (runtimemanager.EventReceipt, bool, error) {
	var (
		outcome     string
		sideEffects []byte
	)
	if err := s.DB.QueryRowContext(ctx, `
		SELECT outcome, COALESCE(side_effects, '{}'::jsonb)
		FROM event_receipts
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'agent'
		  AND subscriber_id = $2
	`, eventID, agentID).Scan(&outcome, &sideEffects); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return runtimemanager.EventReceipt{}, false, nil
		}
		return runtimemanager.EventReceipt{}, false, fmt.Errorf("get event receipt: %w", err)
	}
	receipt := runtimemanager.EventReceipt{
		EventID: eventID,
		AgentID: agentID,
		Status:  mapOutcomeToManagerReceiptStatus(outcome),
	}
	if len(sideEffects) > 0 {
		var payload map[string]any
		if json.Unmarshal(sideEffects, &payload) == nil {
			if raw, ok := payload["manager_status"].(string); ok && strings.TrimSpace(raw) != "" {
				receipt.Status = runtimemanager.ReceiptStatus(strings.TrimSpace(raw))
			}
			switch raw := payload["retry_count"].(type) {
			case float64:
				receipt.RetryCount = int(raw)
			}
			if raw, ok := payload["error"].(string); ok {
				receipt.Error = strings.TrimSpace(raw)
			}
		}
	}
	return receipt, true, nil
}

func scanLegacyPendingEvents(rows *sql.Rows) ([]events.Event, error) {
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
		evt = evt.WithEnvelope(events.EventEnvelope{EntityID: legacyEntityID})
		out = append(out, evt)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read pending events rows: %w", err)
	}
	return out, nil
}

func scanSpecPendingEvents(rows *sql.Rows) ([]events.Event, error) {
	out := make([]events.Event, 0)
	for rows.Next() {
		var evt events.Event
		var entityID, flowInstance, scope string
		if err := rows.Scan(
			&evt.ID,
			&evt.RunID,
			&evt.Type,
			&evt.SourceAgent,
			&entityID,
			&flowInstance,
			&scope,
			&evt.Payload,
			&evt.CreatedAt,
			&evt.ParentEventID,
		); err != nil {
			return nil, fmt.Errorf("scan pending event: %w", err)
		}
		evt = evt.WithEnvelope(events.EventEnvelope{
			EntityID:     entityID,
			FlowInstance: flowInstance,
			Scope:        events.EventScope(scope),
		})
		out = append(out, evt)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read pending events rows: %w", err)
	}
	return out, nil
}

func mapManagerReceiptStatusToOutcome(status runtimemanager.ReceiptStatus) string {
	switch status {
	case runtimemanager.ReceiptStatusError, runtimemanager.ReceiptStatusDeadLetter:
		return "dead_letter"
	default:
		return "success"
	}
}

func mapOutcomeToManagerReceiptStatus(outcome string) runtimemanager.ReceiptStatus {
	switch strings.TrimSpace(strings.ToLower(outcome)) {
	case "dead_letter":
		return runtimemanager.ReceiptStatusDeadLetter
	default:
		return runtimemanager.ReceiptStatusProcessed
	}
}

func managerReceiptReasonCode(status runtimemanager.ReceiptStatus, errText string) string {
	if strings.TrimSpace(errText) != "" {
		switch status {
		case runtimemanager.ReceiptStatusDeadLetter:
			return "retry_exhausted"
		case runtimemanager.ReceiptStatusError:
			return "handler_error"
		default:
			return "runtime_handled"
		}
	}
	switch status {
	case runtimemanager.ReceiptStatusDeadLetter:
		return "retry_exhausted"
	default:
		return "agent_processed"
	}
}
