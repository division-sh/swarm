package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
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
	evt = s.enrichEventCorrelation(ctx, chooseRowQueryer(s.DB, tx), evt)
	if eventSchemaAvailable(ctx, chooseRowQueryer(s.DB, tx)) {
		if err := s.appendEventSpec(ctx, tx, evt); err == nil {
			return nil
		} else if !shouldFallbackLegacyEventsSchema(err) {
			return err
		}
	}

	id := evt.ID
	if id == "" {
		id = uuid.NewString()
	}
	taskID := sanitizeOptionalUUID(evt.TaskID)
	entityID := sanitizeOptionalUUID(evt.EntityID())
	createdAt := evt.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now()
	}

	const q = `
		INSERT INTO events (id, type, source_agent, task_id, entity_id, payload, created_at)
		VALUES ($1::uuid, $2, $3, NULLIF($4,'')::uuid, NULLIF($5,'')::uuid, $6, $7)
		ON CONFLICT (id) DO NOTHING
	`
	execFn := s.DB.ExecContext
	if tx != nil {
		execFn = tx.ExecContext
	}
	if _, err := execFn(ctx, q, id, string(evt.Type), evt.SourceAgent, taskID, entityID, evt.Payload, createdAt); err != nil {
		return fmt.Errorf("append event: %w", err)
	}
	return nil
}

func (s *PostgresStore) PersistEventWithDeliveries(ctx context.Context, evt events.Event, agentIDs []string) error {
	evt = s.enrichEventCorrelation(ctx, chooseRowQueryer(s.DB, nil), evt)
	if err := s.persistEventWithDeliveriesSpec(ctx, evt, agentIDs); err == nil {
		return nil
	} else if !shouldFallbackLegacyEventsSchema(err) {
		return err
	}

	id := evt.ID
	if id == "" {
		id = uuid.NewString()
	}
	taskID := sanitizeOptionalUUID(evt.TaskID)
	entityID := sanitizeOptionalUUID(evt.EntityID())
	createdAt := evt.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now()
	}

	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin event tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	const insertEvent = `
		INSERT INTO events (id, type, source_agent, task_id, entity_id, payload, created_at)
		VALUES ($1::uuid, $2, $3, NULLIF($4,'')::uuid, NULLIF($5,'')::uuid, $6, $7)
		ON CONFLICT (id) DO NOTHING
	`
	if _, err := tx.ExecContext(ctx, insertEvent, id, string(evt.Type), evt.SourceAgent, taskID, entityID, evt.Payload, createdAt); err != nil {
		return fmt.Errorf("append event: %w", err)
	}

	const insertDelivery = `
		INSERT INTO event_deliveries (event_id, agent_id, created_at)
		VALUES ($1::uuid, $2, now())
		ON CONFLICT (event_id, agent_id) DO NOTHING
	`
	for _, agentID := range agentIDs {
		agentID = strings.TrimSpace(agentID)
		if agentID == "" {
			continue
		}
		if _, err := tx.ExecContext(ctx, insertDelivery, id, agentID); err != nil {
			return fmt.Errorf("insert event delivery (agent=%s): %w", agentID, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit event tx: %w", err)
	}
	return nil
}

func (s *PostgresStore) enrichEventCorrelation(ctx context.Context, q rowQueryer, evt events.Event) events.Event {
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
		if runID := lookupEventRunID(ctx, q, parentID); runID != "" {
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
	if eventDeliveriesSpecAvailable(ctx, chooseRowQueryer(s.DB, tx)) {
		if err := s.insertEventDeliveriesSpec(ctx, tx, eventID, agentIDs); err == nil {
			return nil
		} else if !shouldFallbackLegacyEventsSchema(err) {
			return err
		}
	}

	const q = `
		INSERT INTO event_deliveries (event_id, agent_id, created_at)
		VALUES ($1::uuid, $2, now())
		ON CONFLICT (event_id, agent_id) DO NOTHING
	`
	execFn := s.DB.ExecContext
	if tx != nil {
		execFn = tx.ExecContext
	}
	for _, agentID := range agentIDs {
		if _, err := execFn(ctx, q, eventID, agentID); err != nil {
			return fmt.Errorf("insert event delivery (agent=%s): %w", agentID, err)
		}
	}
	return nil
}

func (s *PostgresStore) UpsertPipelineReceipt(ctx context.Context, eventID, status, errText string) error {
	return s.UpsertPipelineReceiptTx(ctx, nil, eventID, status, errText)
}

func (s *PostgresStore) UpsertPipelineReceiptTx(ctx context.Context, tx *sql.Tx, eventID, status, errText string) error {
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
	return s.upsertPipelineReceiptSpec(ctx, tx, eventID, status, errText)
}

func (s *PostgresStore) ListEventsMissingPipelineReceipt(ctx context.Context, since time.Time, limit int) ([]events.Event, error) {
	if limit <= 0 {
		limit = 200
	}
	if since.IsZero() {
		since = time.Now().Add(-24 * time.Hour)
	}

	return s.listEventsMissingPipelineReceiptSpec(ctx, since, limit)
}

func (s *PostgresStore) EventExists(ctx context.Context, eventID string) (bool, error) {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return false, nil
	}
	var exists bool
	if err := s.DB.QueryRowContext(ctx, `
		SELECT EXISTS(SELECT 1 FROM events WHERE event_id = $1::uuid)
	`, eventID).Scan(&exists); err == nil {
		return exists, nil
	} else if !shouldFallbackLegacyEventsSchema(err) {
		return false, fmt.Errorf("event exists lookup: %w", err)
	}
	if err := s.DB.QueryRowContext(ctx, `
		SELECT EXISTS(SELECT 1 FROM events WHERE id = $1::uuid)
	`, eventID).Scan(&exists); err != nil {
		return false, fmt.Errorf("event exists lookup: %w", err)
	}
	return exists, nil
}

func (s *PostgresStore) ListEventDeliveryRecipients(ctx context.Context, eventID string) ([]string, error) {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return nil, nil
	}
	rows, err := s.DB.QueryContext(ctx, `
		SELECT subscriber_id
		FROM event_deliveries
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'agent'
		ORDER BY subscriber_id ASC
	`, eventID)
	if err != nil {
		if !shouldFallbackLegacyEventsSchema(err) {
			return nil, fmt.Errorf("list event delivery recipients: %w", err)
		}
		rows, err = s.DB.QueryContext(ctx, `
			SELECT agent_id
			FROM event_deliveries
			WHERE event_id = $1::uuid
			ORDER BY agent_id ASC
		`, eventID)
		if err != nil {
			return nil, fmt.Errorf("list event delivery recipients: %w", err)
		}
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

func (s *PostgresStore) appendEventSpec(ctx context.Context, tx *sql.Tx, evt events.Event) error {
	id, runID, name, entityID, flowInstance, scope, payload, chainDepth, producedBy, producedByType, sourceEventID, createdAt := eventStorageEnvelope(evt)
	execFn := s.DB.ExecContext
	if tx != nil {
		execFn = tx.ExecContext
	}
	hasRunID := columnExists(ctx, chooseRowQueryer(s.DB, tx), "events", "run_id")
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
	if hasRunID {
		if err := s.ensureRunRow(ctx, tx, runID); err != nil {
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

func (s *PostgresStore) persistEventWithDeliveriesSpec(ctx context.Context, evt events.Event, agentIDs []string) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin event tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := s.appendEventSpec(ctx, tx, evt); err != nil {
		return err
	}
	if err := s.insertEventDeliveriesSpec(ctx, tx, evt.ID, agentIDs); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit event tx: %w", err)
	}
	return nil
}

func (s *PostgresStore) insertEventDeliveriesSpec(ctx context.Context, tx *sql.Tx, eventID string, agentIDs []string) error {
	execFn := s.DB.ExecContext
	if tx != nil {
		execFn = tx.ExecContext
	}
	q := `
		INSERT INTO event_deliveries (event_id, subscriber_type, subscriber_id, reason_code, created_at)
		VALUES ($1::uuid, 'agent', $2, 'matched_agent_subscription', now())
		ON CONFLICT DO NOTHING
	`
	useRunID := columnExists(ctx, chooseRowQueryer(s.DB, tx), "event_deliveries", "run_id")
	if useRunID {
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
	sideEffects, err := json.Marshal(map[string]any{
		"manager_status": strings.TrimSpace(status),
		"reason_code":    reasonCode,
		"error":          strings.TrimSpace(errText),
	})
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

func (s *PostgresStore) listEventsMissingPipelineReceiptSpec(ctx context.Context, since time.Time, limit int) ([]events.Event, error) {
	rows, err := s.DB.QueryContext(ctx, `
		SELECT
			e.event_id::text, e.event_name, COALESCE(e.produced_by, ''),
			COALESCE(e.entity_id::text, ''), e.payload, e.created_at
		FROM events e
		LEFT JOIN event_receipts r
			ON r.event_id = e.event_id
			AND r.subscriber_type = 'platform'
			AND r.subscriber_id = 'pipeline'
		WHERE r.event_id IS NULL
		  AND e.created_at >= $1
		ORDER BY e.created_at ASC
		LIMIT $2
	`, since, limit)
	if err != nil {
		return nil, fmt.Errorf("list events missing pipeline receipt: %w", err)
	}
	defer rows.Close()

	out := make([]events.Event, 0, limit)
	for rows.Next() {
		var evt events.Event
		var entityID string
		if err := rows.Scan(
			&evt.ID,
			&evt.Type,
			&evt.SourceAgent,
			&entityID,
			&evt.Payload,
			&evt.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan missing pipeline receipt event: %w", err)
		}
		evt = evt.WithEntityID(entityID)
		out = append(out, evt)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read missing pipeline receipt events: %w", err)
	}
	return out, nil
}

func shouldFallbackLegacyEventsSchema(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(strings.TrimSpace(err.Error()))
	fallback := strings.Contains(msg, `event_id`) ||
		strings.Contains(msg, `run_id`) ||
		strings.Contains(msg, `event_name`) ||
		strings.Contains(msg, `produced_by`) ||
		strings.Contains(msg, `produced_by_type`) ||
		strings.Contains(msg, `subscriber_type`) ||
		strings.Contains(msg, `subscriber_id`) ||
		strings.Contains(msg, `active_session_id`) ||
		strings.Contains(msg, `started_at`) ||
		strings.Contains(msg, `reason_code`) ||
		strings.Contains(msg, `scope`) ||
		strings.Contains(msg, `flow_instance`) ||
		strings.Contains(msg, `outcome`) ||
		strings.Contains(msg, `side_effects`) ||
		strings.Contains(msg, `duration_ms`) ||
		strings.Contains(msg, `relation "event_receipts" does not exist`) ||
		strings.Contains(msg, `relation "event_deliveries" does not exist`)
	if fallback {
		log.Printf("store legacy schema fallback triggered err=%v", err)
	}
	return fallback
}

func chooseRowQueryer(db *sql.DB, tx *sql.Tx) rowQueryer {
	if tx != nil {
		return tx
	}
	return db
}

func eventSchemaAvailable(ctx context.Context, q rowQueryer) bool {
	return columnExists(ctx, q, "events", "event_id")
}

func eventDeliveriesSpecAvailable(ctx context.Context, q rowQueryer) bool {
	return columnExists(ctx, q, "event_deliveries", "subscriber_id")
}

func eventReceiptsSpecAvailable(ctx context.Context, q rowQueryer) bool {
	return columnExists(ctx, q, "event_receipts", "subscriber_id")
}

func columnExists(ctx context.Context, q rowQueryer, tableName, columnName string) bool {
	if q == nil {
		return false
	}
	var exists bool
	if err := q.QueryRowContext(ctx, `
		SELECT EXISTS(
			SELECT 1
			FROM information_schema.columns
			WHERE table_schema = 'public'
			  AND table_name = $1
			  AND column_name = $2
		)
	`, tableName, columnName).Scan(&exists); err != nil {
		return false
	}
	return exists
}

func lookupEventRunID(ctx context.Context, q rowQueryer, eventID string) string {
	eventID = strings.TrimSpace(eventID)
	if q == nil || eventID == "" {
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

func (s *PostgresStore) ensureRunRow(ctx context.Context, tx *sql.Tx, runID string) error {
	runID = nullUUIDString(runID)
	if runID == "" || !columnExists(ctx, chooseRowQueryer(s.DB, tx), "runs", "run_id") {
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
	entityID = sanitizeOptionalUUID(evt.EntityID())
	flowInstance = eventPayloadString(payload, "flow_instance")
	scope = "global"
	if entityID != "" {
		scope = "entity"
	} else if flowInstance != "" {
		scope = "flow"
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

func eventPayloadString(raw []byte, key string) string {
	if len(raw) == 0 {
		return ""
	}
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil || payload == nil {
		return ""
	}
	value, _ := payload[strings.TrimSpace(key)].(string)
	return strings.TrimSpace(value)
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
