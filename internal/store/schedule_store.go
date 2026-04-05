package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	runtimepipeline "swarm/internal/runtime/pipeline"
)

func (s *PostgresStore) UpsertSchedule(ctx context.Context, sc runtimepipeline.Schedule) error {
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return err
	}
	if strings.TrimSpace(sc.AgentID) == "" || strings.TrimSpace(sc.EventType) == "" {
		return fmt.Errorf("agent_id and event_type are required")
	}
	if strings.TrimSpace(sc.Mode) == "" {
		sc.Mode = "once"
	}
	entityID := sc.EffectiveEntityID()
	sc.EntityID = entityID
	flowInstance := sc.EffectiveFlowInstance()
	sc.FlowInstance = flowInstance

	switch caps.Schedules {
	case SchemaFlavorCanonical:
		return s.upsertScheduleSpec(ctx, sc)
	default:
		return unsupportedSchemaCapability("timers/schedules", caps.Schedules)
	}
}

func (s *PostgresStore) CancelSchedule(ctx context.Context, agentID, eventType string) error {
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return err
	}
	if strings.TrimSpace(agentID) == "" || strings.TrimSpace(eventType) == "" {
		return fmt.Errorf("agent_id and event_type are required")
	}
	switch caps.Schedules {
	case SchemaFlavorCanonical:
		return s.cancelScheduleSpec(ctx, agentID, eventType)
	default:
		return unsupportedSchemaCapability("timers/schedules", caps.Schedules)
	}
}

func (s *PostgresStore) CancelScheduleExact(ctx context.Context, sc runtimepipeline.Schedule) error {
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return err
	}
	if strings.TrimSpace(sc.AgentID) == "" || strings.TrimSpace(sc.EventType) == "" {
		return fmt.Errorf("agent_id and event_type are required")
	}
	entityID := sc.EffectiveEntityID()
	sc.EntityID = entityID
	flowInstance := sc.EffectiveFlowInstance()
	sc.FlowInstance = flowInstance
	switch caps.Schedules {
	case SchemaFlavorCanonical:
		return s.cancelScheduleExactSpec(ctx, sc)
	default:
		return unsupportedSchemaCapability("timers/schedules", caps.Schedules)
	}
}

func (s *PostgresStore) LoadActiveSchedules(ctx context.Context) ([]runtimepipeline.Schedule, error) {
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return nil, err
	}
	switch caps.Schedules {
	case SchemaFlavorCanonical:
		return s.loadActiveSchedulesSpec(ctx)
	default:
		return nil, unsupportedSchemaCapability("timers/schedules", caps.Schedules)
	}
}

func (s *PostgresStore) MarkScheduleFired(ctx context.Context, sc runtimepipeline.Schedule) error {
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return err
	}
	if strings.TrimSpace(sc.AgentID) == "" || strings.TrimSpace(sc.EventType) == "" {
		return nil
	}
	switch caps.Schedules {
	case SchemaFlavorCanonical:
		return s.markScheduleFiredSpec(ctx, sc)
	default:
		return unsupportedSchemaCapability("timers/schedules", caps.Schedules)
	}
}

func (s *PostgresStore) MarkScheduleFiredExact(ctx context.Context, sc runtimepipeline.Schedule) error {
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return err
	}
	if strings.TrimSpace(sc.AgentID) == "" || strings.TrimSpace(sc.EventType) == "" {
		return nil
	}
	entityID := sc.EffectiveEntityID()
	sc.EntityID = entityID
	flowInstance := sc.EffectiveFlowInstance()
	sc.FlowInstance = flowInstance
	switch caps.Schedules {
	case SchemaFlavorCanonical:
		return s.markScheduleFiredExactSpec(ctx, sc)
	default:
		return unsupportedSchemaCapability("timers/schedules", caps.Schedules)
	}
}

func persistedSchedulePayload(sc runtimepipeline.Schedule) []byte {
	payload := sc.Payload
	if len(payload) == 0 {
		payload = []byte("{}")
	}
	taskID := strings.TrimSpace(sc.TaskID)
	if taskID == "" {
		return payload
	}
	var decoded map[string]any
	if err := json.Unmarshal(payload, &decoded); err != nil || decoded == nil {
		return payload
	}
	decoded["__schedule_task_id"] = taskID
	encoded, err := json.Marshal(decoded)
	if err != nil {
		return payload
	}
	return encoded
}

func exactScheduleTaskIDSQL() string {
	return `COALESCE(fire_payload->>'__schedule_task_id', '')`
}

func (s *PostgresStore) upsertScheduleSpec(ctx context.Context, sc runtimepipeline.Schedule) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin timer tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	payload := persistedSchedulePayload(sc)
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`
		UPDATE timers
		SET status = 'cancelled'
		WHERE owner_agent = $1
		  AND fire_event = $2
		  AND entity_id IS NOT DISTINCT FROM NULLIF($3,'')::uuid
		  AND flow_instance IS NOT DISTINCT FROM NULLIF($4,'')
		  AND %s = $5
		  AND status = 'active'
	`, exactScheduleTaskIDSQL()), sc.AgentID, sc.EventType, sc.EntityID, sc.FlowInstance, strings.TrimSpace(sc.TaskID)); err != nil {
		return fmt.Errorf("deactivate previous timer: %w", err)
	}

	fireAt := sc.At
	if fireAt.IsZero() {
		fireAt = time.Now().UTC()
	}
	recurring := strings.EqualFold(strings.TrimSpace(sc.Mode), "cron")
	taskType := "timer"
	if recurring {
		taskType = "scheduled_task"
		if sc.EntityID == "" {
			taskType = "global_recurring"
		}
	}
	timerName := strings.TrimSpace(sc.TaskID)
	if timerName == "" {
		timerName = strings.TrimSpace(sc.EventType)
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO timers (
			timer_name, entity_id, flow_instance, fire_event, fire_payload,
			fire_at, recurring, recurrence_cron, recurrence_interval,
			owner_node, owner_agent, task_type, status
		)
		VALUES (
			$1, NULLIF($2,'')::uuid, NULLIF($3,''), $4, $5::jsonb,
			$6, $7, NULLIF($8,''), NULL,
			NULL, $9, $10, 'active'
		)
	`, timerName, sc.EntityID, sc.FlowInstance, sc.EventType, string(payload), fireAt, recurring, sc.Cron, sc.AgentID, taskType)
	if err != nil {
		return fmt.Errorf("insert timer: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit timer tx: %w", err)
	}
	return nil
}

func (s *PostgresStore) cancelScheduleSpec(ctx context.Context, agentID, eventType string) error {
	_, err := s.DB.ExecContext(ctx, `
		UPDATE timers
		SET status = 'cancelled'
		WHERE owner_agent = $1
		  AND fire_event = $2
		  AND status = 'active'
	`, agentID, eventType)
	if err != nil {
		return fmt.Errorf("cancel timer: %w", err)
	}
	return nil
}

func (s *PostgresStore) cancelScheduleExactSpec(ctx context.Context, sc runtimepipeline.Schedule) error {
	_, err := s.DB.ExecContext(ctx, fmt.Sprintf(`
		UPDATE timers
		SET status = 'cancelled'
		WHERE owner_agent = $1
		  AND fire_event = $2
		  AND entity_id IS NOT DISTINCT FROM NULLIF($3,'')::uuid
		  AND flow_instance IS NOT DISTINCT FROM NULLIF($4,'')
		  AND %s = $5
		  AND status = 'active'
	`, exactScheduleTaskIDSQL()), sc.AgentID, sc.EventType, sc.EntityID, sc.FlowInstance, strings.TrimSpace(sc.TaskID))
	if err != nil {
		return fmt.Errorf("cancel exact timer: %w", err)
	}
	return nil
}

func (s *PostgresStore) loadActiveSchedulesSpec(ctx context.Context) ([]runtimepipeline.Schedule, error) {
	rows, err := s.DB.QueryContext(ctx, `
		SELECT
			owner_agent,
			fire_event,
			CASE WHEN recurring THEN 'cron' ELSE 'once' END,
			COALESCE(recurrence_cron, ''),
			fire_at,
			COALESCE(entity_id::text, ''),
			COALESCE(flow_instance, ''),
			fire_payload
		FROM timers
		WHERE status = 'active'
		ORDER BY created_at ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("query active timers: %w", err)
	}
	defer rows.Close()

	out := make([]runtimepipeline.Schedule, 0)
	for rows.Next() {
		var (
			sc      runtimepipeline.Schedule
			fireAt  time.Time
			payload []byte
		)
		if err := rows.Scan(
			&sc.AgentID,
			&sc.EventType,
			&sc.Mode,
			&sc.Cron,
			&fireAt,
			&sc.EntityID,
			&sc.FlowInstance,
			&payload,
		); err != nil {
			return nil, fmt.Errorf("scan active timer: %w", err)
		}
		sc.At = fireAt
		sc.TaskID, sc.Payload = extractPersistedScheduleTaskID(payload)
		out = append(out, sc)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate active timers: %w", err)
	}
	return out, nil
}

func (s *PostgresStore) markScheduleFiredSpec(ctx context.Context, sc runtimepipeline.Schedule) error {
	status := "fired"
	if !strings.EqualFold(strings.TrimSpace(sc.Mode), "once") {
		status = "active"
	}
	_, err := s.DB.ExecContext(ctx, `
		UPDATE timers
		SET status = $3,
		    fired_at = now()
		WHERE owner_agent = $1
		  AND fire_event = $2
		  AND status = 'active'
	`, sc.AgentID, sc.EventType, status)
	if err != nil {
		return fmt.Errorf("mark timer fired: %w", err)
	}
	return nil
}

func (s *PostgresStore) markScheduleFiredExactSpec(ctx context.Context, sc runtimepipeline.Schedule) error {
	status := "fired"
	if !strings.EqualFold(strings.TrimSpace(sc.Mode), "once") {
		status = "active"
	}
	_, err := s.DB.ExecContext(ctx, fmt.Sprintf(`
		UPDATE timers
		SET status = $6,
		    fired_at = now()
		WHERE owner_agent = $1
		  AND fire_event = $2
		  AND entity_id IS NOT DISTINCT FROM NULLIF($3,'')::uuid
		  AND flow_instance IS NOT DISTINCT FROM NULLIF($4,'')
		  AND %s = $5
		  AND status = 'active'
	`, exactScheduleTaskIDSQL()), sc.AgentID, sc.EventType, sc.EntityID, sc.FlowInstance, strings.TrimSpace(sc.TaskID), status)
	if err != nil {
		return fmt.Errorf("mark exact timer fired: %w", err)
	}
	return nil
}

func extractPersistedScheduleTaskID(payload []byte) (string, []byte) {
	if len(payload) == 0 {
		return "", payload
	}
	var decoded map[string]any
	if err := json.Unmarshal(payload, &decoded); err != nil || decoded == nil {
		return "", payload
	}
	taskID, _ := decoded["__schedule_task_id"].(string)
	delete(decoded, "__schedule_task_id")
	encoded, err := json.Marshal(decoded)
	if err != nil {
		return strings.TrimSpace(taskID), payload
	}
	return strings.TrimSpace(taskID), encoded
}
