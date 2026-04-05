package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"swarm/internal/events"
	runtimecorrelation "swarm/internal/runtime/correlation"
)

type rowQueryer interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func (s *PostgresStore) AppendEvent(ctx context.Context, evt events.Event) error {
	return s.AppendEventTx(ctx, nil, evt)
}

func (s *PostgresStore) BeginEventTx(ctx context.Context) (*sql.Tx, error) {
	return s.DB.BeginTx(ctx, nil)
}

func (s *PostgresStore) AppendEventTx(ctx context.Context, tx *sql.Tx, evt events.Event) error {
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return err
	}
	return withEventStoreRetry(ctx, tx, func() error {
		evt = s.enrichEventCorrelation(ctx, caps, chooseRowQueryer(s.DB, tx), evt)
		switch caps.Events.Log {
		case SchemaFlavorCanonical:
			return s.appendEventSpec(ctx, caps, tx, evt)
		default:
			return unsupportedSchemaCapability("events", caps.Events.Log)
		}
	})
}

func (s *PostgresStore) PersistEventWithDeliveries(ctx context.Context, evt events.Event, agentIDs []string) error {
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return err
	}
	evt = s.enrichEventCorrelation(ctx, caps, chooseRowQueryer(s.DB, nil), evt)
	switch {
	case caps.Events.Log == SchemaFlavorCanonical && caps.Events.Deliveries == SchemaFlavorCanonical:
		return s.persistEventWithDeliveriesSpec(ctx, caps, evt, agentIDs)
	case caps.Events.Log != SchemaFlavorCanonical:
		return unsupportedSchemaCapability("events", caps.Events.Log)
	case caps.Events.Deliveries != SchemaFlavorCanonical:
		return unsupportedSchemaCapability("event_deliveries", caps.Events.Deliveries)
	}
	return nil
}

func (s *PostgresStore) enrichEventCorrelation(ctx context.Context, caps StoreSchemaCapabilities, q rowQueryer, evt events.Event) events.Event {
	parentID := strings.TrimSpace(evt.ParentEventID)
	if parentID == "" {
		if inbound, ok := runtimecorrelation.InboundEventFromContext(ctx); ok {
			if inboundID := strings.TrimSpace(inbound.ID); inboundID != "" && inboundID != strings.TrimSpace(evt.ID) {
				parentID = inboundID
				evt.ParentEventID = inboundID
			}
		}
	}
	if strings.TrimSpace(evt.RunID) == "" && parentID != "" {
		if runID := lookupEventRunID(ctx, caps, q, parentID); runID != "" {
			evt.RunID = runID
		}
	}
	_, evt = runtimecorrelation.CorrelateEvent(ctx, evt)
	return evt
}

func sanitizeOptionalUUID(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if _, err := uuid.Parse(raw); err != nil {
		return ""
	}
	return raw
}

func (s *PostgresStore) InsertEventDeliveries(ctx context.Context, eventID string, agentIDs []string) error {
	return s.InsertEventDeliveriesTx(ctx, nil, eventID, agentIDs)
}

func (s *PostgresStore) InsertEventDeliveriesTx(ctx context.Context, tx *sql.Tx, eventID string, agentIDs []string) error {
	if len(agentIDs) == 0 {
		return nil
	}
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return err
	}
	switch {
	case caps.Events.Deliveries == SchemaFlavorCanonical && caps.Events.Log == SchemaFlavorCanonical:
		return s.insertEventDeliveriesSpec(ctx, caps, tx, eventID, agentIDs)
	case caps.Events.Deliveries != SchemaFlavorCanonical:
		return unsupportedSchemaCapability("event_deliveries", caps.Events.Deliveries)
	case caps.Events.Log != SchemaFlavorCanonical:
		return unsupportedSchemaCapability("events", caps.Events.Log)
	}
	return nil
}

func (s *PostgresStore) UpsertPipelineReceipt(ctx context.Context, eventID, status, errText string) error {
	return s.UpsertPipelineReceiptTx(ctx, nil, eventID, status, errText)
}

func (s *PostgresStore) UpsertPipelineReceiptTx(ctx context.Context, tx *sql.Tx, eventID, status, errText string) error {
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return err
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return nil
	}
	status = strings.TrimSpace(strings.ToLower(status))
	if status == "" {
		status = "processed"
	}
	if strings.TrimSpace(errText) != "" && status == "processed" {
		status = "error"
	}
	if caps.Events.Receipts != SchemaFlavorCanonical || caps.Events.Log != SchemaFlavorCanonical {
		if caps.Events.Receipts != SchemaFlavorCanonical {
			return unsupportedSchemaCapability("event_receipts", caps.Events.Receipts)
		}
		return unsupportedSchemaCapability("events", caps.Events.Log)
	}
	return s.upsertPipelineReceiptSpec(ctx, tx, eventID, status, errText)
}

func (s *PostgresStore) ListEventsMissingPipelineReceipt(ctx context.Context, since time.Time, limit int) ([]events.PersistedReplayEvent, error) {
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 200
	}
	if since.IsZero() {
		since = time.Now().Add(-24 * time.Hour)
	}
	if caps.Events.Receipts != SchemaFlavorCanonical || caps.Events.Log != SchemaFlavorCanonical {
		if caps.Events.Receipts != SchemaFlavorCanonical {
			return nil, unsupportedSchemaCapability("event_receipts", caps.Events.Receipts)
		}
		return nil, unsupportedSchemaCapability("events", caps.Events.Log)
	}
	return s.listEventsMissingPipelineReceiptSpec(ctx, caps, since, limit)
}

func (s *PostgresStore) EventExists(ctx context.Context, eventID string) (bool, error) {
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return false, err
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return false, nil
	}
	var exists bool
	query := `SELECT EXISTS(SELECT 1 FROM events WHERE event_id = $1::uuid)`
	switch caps.Events.Log {
	case SchemaFlavorCanonical:
	default:
		return false, unsupportedSchemaCapability("events", caps.Events.Log)
	}
	if err := s.DB.QueryRowContext(ctx, query, eventID).Scan(&exists); err != nil {
		return false, fmt.Errorf("event exists lookup: %w", err)
	}
	return exists, nil
}

func (s *PostgresStore) ListEventDeliveryRecipients(ctx context.Context, eventID string) ([]string, error) {
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return nil, err
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return nil, nil
	}
	query := `
		SELECT subscriber_id
		FROM event_deliveries
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'agent'
		ORDER BY subscriber_id ASC
	`
	switch caps.Events.Deliveries {
	case SchemaFlavorCanonical:
	default:
		return nil, unsupportedSchemaCapability("event_deliveries", caps.Events.Deliveries)
	}
	rows, err := s.DB.QueryContext(ctx, query, eventID)
	if err != nil {
		return nil, fmt.Errorf("list event delivery recipients: %w", err)
	}
	defer rows.Close()

	recipients := make([]string, 0, 8)
	for rows.Next() {
		var agentID string
		if err := rows.Scan(&agentID); err != nil {
			return nil, fmt.Errorf("scan event delivery recipient: %w", err)
		}
		agentID = strings.TrimSpace(agentID)
		if agentID != "" {
			recipients = append(recipients, agentID)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read event delivery recipients: %w", err)
	}
	return recipients, nil
}

func (s *PostgresStore) appendEventSpec(ctx context.Context, caps StoreSchemaCapabilities, tx *sql.Tx, evt events.Event) error {
	id, runID, name, entityID, flowInstance, scope, payload, chainDepth, producedBy, producedByType, sourceEventID, createdAt := eventStorageEnvelope(evt)
	execFn := s.DB.ExecContext
	if tx != nil {
		execFn = tx.ExecContext
	}
	q := `
		INSERT INTO events (
			event_id, event_name, entity_id, flow_instance, scope, payload,
			chain_depth, produced_by, produced_by_type, source_event_id, created_at
		)
		VALUES (
			$1::uuid, $2, NULLIF($3,'')::uuid, NULLIF($4,''), $5, $6::jsonb,
			$7, NULLIF($8,''), $9, NULLIF($10,'')::uuid, $11
		)
		ON CONFLICT (event_id) DO NOTHING
	`
	args := []any{id, name, entityID, flowInstance, scope, string(payload), chainDepth, producedBy, producedByType, sourceEventID, createdAt}
	if caps.Events.LogRunID {
		if err := s.ensureRunRow(ctx, caps, tx, runID); err != nil {
			return err
		}
		q = `
			INSERT INTO events (
				event_id, run_id, event_name, entity_id, flow_instance, scope, payload,
				chain_depth, produced_by, produced_by_type, source_event_id, created_at
			)
			VALUES (
				$1::uuid, NULLIF($2,'')::uuid, $3, NULLIF($4,'')::uuid, NULLIF($5,''), $6, $7::jsonb,
				$8, NULLIF($9,''), $10, NULLIF($11,'')::uuid, $12
			)
			ON CONFLICT (event_id) DO NOTHING
		`
		args = []any{id, runID, name, entityID, flowInstance, scope, string(payload), chainDepth, producedBy, producedByType, sourceEventID, createdAt}
	}
	if _, err := execFn(ctx, q, args...); err != nil {
		return fmt.Errorf("append event: %w", err)
	}
	return nil
}

func (s *PostgresStore) persistEventWithDeliveriesSpec(ctx context.Context, caps StoreSchemaCapabilities, evt events.Event, agentIDs []string) error {
	return withEventStoreRetry(ctx, nil, func() error {
		tx, err := s.DB.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("begin event tx: %w", err)
		}
		defer func() { _ = tx.Rollback() }()
		if err := s.appendEventSpec(ctx, caps, tx, evt); err != nil {
			return err
		}
		if err := s.insertEventDeliveriesSpec(ctx, caps, tx, evt.ID, agentIDs); err != nil {
			return err
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit event tx: %w", err)
		}
		return nil
	})
}

func withEventStoreRetry(ctx context.Context, tx *sql.Tx, fn func() error) error {
	if fn == nil {
		return nil
	}
	attempts := 1
	if tx == nil {
		attempts = 3
	}
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(100 * time.Millisecond):
			}
		}
		lastErr = fn()
		if !isTransientEventStoreConnectionError(lastErr) {
			return lastErr
		}
	}
	return lastErr
}

func isTransientEventStoreConnectionError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, sql.ErrConnDone) {
		return true
	}
	return strings.Contains(strings.ToLower(err.Error()), "bad connection")
}

func (s *PostgresStore) insertEventDeliveriesSpec(ctx context.Context, caps StoreSchemaCapabilities, tx *sql.Tx, eventID string, agentIDs []string) error {
	execFn := s.DB.ExecContext
	if tx != nil {
		execFn = tx.ExecContext
	}
	q := `
		INSERT INTO event_deliveries (event_id, subscriber_type, subscriber_id, reason_code, created_at)
		VALUES ($1::uuid, 'agent', $2, 'matched_agent_subscription', now())
		ON CONFLICT DO NOTHING
	`
	if caps.Events.DeliveryRunID {
		q = `
			INSERT INTO event_deliveries (run_id, event_id, subscriber_type, subscriber_id, reason_code, created_at)
			SELECT e.run_id, e.event_id, 'agent', $2, 'matched_agent_subscription', now()
			FROM events e
			WHERE e.event_id = $1::uuid
			ON CONFLICT DO NOTHING
		`
	}
	seen := make(map[string]struct{}, len(agentIDs))
	for _, agentID := range agentIDs {
		agentID = strings.TrimSpace(agentID)
		if agentID == "" {
			continue
		}
		if _, ok := seen[agentID]; ok {
			continue
		}
		seen[agentID] = struct{}{}
		if _, err := execFn(ctx, q, eventID, agentID); err != nil {
			return fmt.Errorf("insert event delivery (agent=%s): %w", agentID, err)
		}
	}
	return nil
}

func (s *PostgresStore) upsertPipelineReceiptSpec(ctx context.Context, tx *sql.Tx, eventID, status, errText string) error {
	reasonCode := pipelineReceiptReasonCode(status, errText)
	sideEffects, err := marshalPipelineReceiptSideEffects(newPipelineReceiptSideEffects(status, reasonCode, errText))
	if err != nil {
		return fmt.Errorf("marshal pipeline receipt side effects: %w", err)
	}
	outcome := mapPipelineStatusToOutcome(status)
	const q = `
		INSERT INTO event_receipts (
			event_id, subscriber_type, subscriber_id, entity_id, flow_instance,
			outcome, reason_code, side_effects, processed_at
		)
		SELECT
			e.event_id, 'platform', 'pipeline', e.entity_id, e.flow_instance,
			$2, NULLIF($3,''), $4::jsonb, now()
		FROM events e
		WHERE e.event_id = $1::uuid
		ON CONFLICT (event_id, subscriber_id) DO UPDATE SET
			outcome = EXCLUDED.outcome,
			reason_code = EXCLUDED.reason_code,
			side_effects = EXCLUDED.side_effects,
			processed_at = now()
	`
	execFn := s.DB.ExecContext
	if tx != nil {
		execFn = tx.ExecContext
	}
	if _, err := execFn(ctx, q, eventID, outcome, reasonCode, string(sideEffects)); err != nil {
		return fmt.Errorf("upsert pipeline receipt: %w", err)
	}
	return nil
}

func (s *PostgresStore) listEventsMissingPipelineReceiptSpec(ctx context.Context, caps StoreSchemaCapabilities, since time.Time, limit int) ([]events.PersistedReplayEvent, error) {
	runIDExpr := `COALESCE(e.run_id::text, '')`
	if !caps.Events.LogRunID {
		runIDExpr = `''`
	}
	rows, err := s.DB.QueryContext(ctx, fmt.Sprintf(`
		SELECT
			e.event_id::text, %s, e.event_name, COALESCE(e.produced_by, ''),
			COALESCE(e.entity_id::text, ''), COALESCE(e.flow_instance, ''), COALESCE(e.scope, 'global'),
			e.payload, e.created_at, COALESCE(e.source_event_id::text, '')
		FROM events e
		LEFT JOIN event_receipts r
			ON r.event_id = e.event_id
			AND r.subscriber_type = 'platform'
			AND r.subscriber_id = 'pipeline'
		WHERE r.event_id IS NULL
		  AND e.created_at >= $1
		ORDER BY e.created_at ASC
		LIMIT $2
	`, runIDExpr), since, limit)
	if err != nil {
		return nil, fmt.Errorf("list events missing pipeline receipt: %w", err)
	}
	defer rows.Close()

	out := make([]events.PersistedReplayEvent, 0, limit)
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
			return nil, fmt.Errorf("scan missing pipeline receipt event: %w", err)
		}
		evt = evt.WithEnvelope(events.EventEnvelope{
			EntityID:     entityID,
			FlowInstance: flowInstance,
			Scope:        events.EventScope(scope),
		})
		record := events.PersistedReplayEvent{Event: evt}
		if !caps.Events.LogRunID {
			record.ReplayError = "missing run_id schema capability"
		} else if strings.TrimSpace(evt.RunID) == "" {
			record.ReplayError = "missing canonical run_id"
		}
		out = append(out, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read missing pipeline receipt events: %w", err)
	}
	return out, nil
}

func chooseRowQueryer(db *sql.DB, tx *sql.Tx) rowQueryer {
	if tx != nil {
		return tx
	}
	return db
}

func lookupEventRunID(ctx context.Context, caps StoreSchemaCapabilities, q rowQueryer, eventID string) string {
	eventID = strings.TrimSpace(eventID)
	if q == nil || eventID == "" || caps.Events.Log != SchemaFlavorCanonical || !caps.Events.LogRunID {
		return ""
	}
	var runID string
	if err := q.QueryRowContext(ctx, `
		SELECT COALESCE(run_id::text, '')
		FROM events
		WHERE event_id = $1::uuid
		LIMIT 1
	`, eventID).Scan(&runID); err != nil {
		return ""
	}
	return strings.TrimSpace(runID)
}

func (s *PostgresStore) ensureRunRow(ctx context.Context, caps StoreSchemaCapabilities, tx *sql.Tx, runID string) error {
	runID = nullUUIDString(runID)
	if runID == "" || !caps.Events.HasRuns {
		return nil
	}
	execFn := s.DB.ExecContext
	if tx != nil {
		execFn = tx.ExecContext
	}
	if _, err := execFn(ctx, `
		INSERT INTO runs (run_id, status, started_at)
		VALUES ($1::uuid, 'running', now())
		ON CONFLICT (run_id) DO NOTHING
	`, runID); err != nil {
		return fmt.Errorf("ensure run row: %w", err)
	}
	return nil
}

func canonicalRunTerminalStatus(raw string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "completed":
		return "completed", nil
	case "failed":
		return "failed", nil
	case "cancelled":
		return "cancelled", nil
	case "forked":
		return "forked", nil
	default:
		return "", fmt.Errorf("unsupported terminal run status %q", raw)
	}
}

func (s *PostgresStore) MarkRunTerminal(ctx context.Context, runID, status, errorSummary string, endedAt time.Time) error {
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return err
	}
	runID = nullUUIDString(runID)
	if runID == "" {
		return fmt.Errorf("run_id is required")
	}
	if !caps.Events.HasRuns {
		return fmt.Errorf("runs table is required")
	}
	status, err = canonicalRunTerminalStatus(status)
	if err != nil {
		return err
	}
	errorSummary = strings.TrimSpace(errorSummary)
	if status != "failed" {
		errorSummary = ""
	}
	if endedAt.IsZero() {
		endedAt = time.Now().UTC()
	}
	result, err := s.DB.ExecContext(ctx, `
		UPDATE runs
		SET status = $2,
		    error_summary = NULLIF($3, ''),
		    ended_at = COALESCE(ended_at, $4)
		WHERE run_id = $1::uuid
		  AND (status IN ('running', 'paused') OR status = $2)
	`, runID, status, errorSummary, endedAt.UTC())
	if err != nil {
		return fmt.Errorf("mark run terminal: %w", err)
	}
	if rows, err := result.RowsAffected(); err == nil && rows > 0 {
		return nil
	}

	var currentStatus string
	err = s.DB.QueryRowContext(ctx, `
		SELECT COALESCE(status, '')
		FROM runs
		WHERE run_id = $1::uuid
	`, runID).Scan(&currentStatus)
	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("run %s not found", runID)
		}
		return fmt.Errorf("load run terminal state: %w", err)
	}
	currentStatus = strings.TrimSpace(currentStatus)
	if currentStatus == status {
		return nil
	}
	return fmt.Errorf("run %s already terminal with status %s", runID, currentStatus)
}

func runIDOrEventID(runID, eventID string) string {
	if runID = nullUUIDString(runID); runID != "" {
		return runID
	}
	return nullUUIDString(eventID)
}

func eventStorageEnvelope(evt events.Event) (id string, runID string, eventName string, entityID string, flowInstance string, scope string, payload []byte, chainDepth int, producedBy string, producedByType string, sourceEventID string, createdAt time.Time) {
	id = strings.TrimSpace(evt.ID)
	if id == "" {
		id = uuid.NewString()
	}
	runID = runIDOrEventID(evt.RunID, id)
	eventName = strings.TrimSpace(string(evt.Type))
	payload = eventPayloadForStorage(evt)
	envelope := evt.NormalizedEnvelope()
	entityID = sanitizeOptionalUUID(envelope.EntityID)
	flowInstance = envelope.FlowInstance
	scope = string(envelope.Scope)
	if scope == "" {
		scope = string(events.EventScopeGlobal)
	}
	chainDepth = evt.ChainDepth
	if chainDepth < 0 {
		chainDepth = 0
	}
	producedBy = strings.TrimSpace(evt.SourceAgent)
	producedByType = "agent"
	if producedBy == "" || producedBy == "runtime" {
		producedByType = "platform"
	}
	sourceEventID = sanitizeOptionalUUID(evt.ParentEventID)
	createdAt = evt.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}
	return
}

func eventPayloadForStorage(evt events.Event) []byte {
	taskID := sanitizeOptionalUUID(evt.TaskID)
	if taskID == "" {
		if len(evt.Payload) == 0 {
			return []byte("{}")
		}
		return evt.Payload
	}
	payload := map[string]any{}
	if len(evt.Payload) > 0 {
		if err := json.Unmarshal(evt.Payload, &payload); err != nil || payload == nil {
			return evt.Payload
		}
	}
	if _, exists := payload["task_id"]; !exists {
		payload["task_id"] = taskID
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return evt.Payload
	}
	return encoded
}

func mapPipelineStatusToOutcome(status string) string {
	switch strings.TrimSpace(strings.ToLower(status)) {
	case "error", "dead_letter":
		return "dead_letter"
	default:
		return "success"
	}
}

func pipelineReceiptReasonCode(status, errText string) string {
	status = strings.TrimSpace(strings.ToLower(status))
	if strings.TrimSpace(errText) != "" {
		return "pipeline_error"
	}
	switch status {
	case "dead_letter":
		return "pipeline_dead_letter"
	case "error":
		return "pipeline_error"
	default:
		return "pipeline_persisted"
	}
}
