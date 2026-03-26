package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	runtimepipeline "swarm/internal/runtime/pipeline"
)

func (s *PostgresStore) UpsertSchedule(ctx context.Context, sc runtimepipeline.Schedule) error {
	if strings.TrimSpace(sc.AgentID) == "" || strings.TrimSpace(sc.EventType) == "" {
		return fmt.Errorf("agent_id and event_type are required")
	}
	if strings.TrimSpace(sc.Mode) == "" {
		sc.Mode = "once"
	}
	entityID := sc.EffectiveEntityID()
	sc.EntityID = entityID

	if err := s.upsertScheduleSpec(ctx, sc); err == nil {
		return nil
	} else if !shouldFallbackLegacyTimersSchema(err) {
		return err
	}

	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin schedule tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	payload := persistedSchedulePayload(sc)

	if _, err := tx.ExecContext(ctx, `
		UPDATE schedules
		SET active = false,
		    cancelled_at = now()
		WHERE agent_id = $1
		  AND event_type = $2
		  AND entity_id IS NOT DISTINCT FROM NULLIF($3,'')::uuid
		  AND COALESCE(payload->>'__schedule_task_id', '') = $4
		  AND active = true
	`, sc.AgentID, sc.EventType, sc.EntityID, strings.TrimSpace(sc.TaskID)); err != nil {
		return fmt.Errorf("deactivate previous schedule: %w", err)
	}

	var atTime any
	var nextFire any
	if !sc.At.IsZero() {
		atTime = sc.At
		nextFire = sc.At
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO schedules (
			agent_id, entity_id, event_type, mode, cron_expr,
			at_time, next_fire_at, payload, active, created_at
		)
		VALUES (
			$1, NULLIF($2,'')::uuid, $3, $4, NULLIF($5,''),
			$6, $7, $8::jsonb, true, now()
		)
	`, sc.AgentID, sc.EntityID, sc.EventType, sc.Mode, sc.Cron, atTime, nextFire, string(payload)); err != nil {
		return fmt.Errorf("insert schedule: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit schedule tx: %w", err)
	}
	return nil
}

func (s *PostgresStore) CancelSchedule(ctx context.Context, agentID, eventType string) error {
	if strings.TrimSpace(agentID) == "" || strings.TrimSpace(eventType) == "" {
		return fmt.Errorf("agent_id and event_type are required")
	}
	if err := s.cancelScheduleSpec(ctx, agentID, eventType); err == nil {
		return nil
	} else if !shouldFallbackLegacyTimersSchema(err) {
		return err
	}
	_, err := s.DB.ExecContext(ctx, `
		UPDATE schedules
		SET active = false,
		    cancelled_at = now()
		WHERE agent_id = $1
		  AND event_type = $2
		  AND active = true
	`, agentID, eventType)
	if err != nil {
		return fmt.Errorf("cancel schedule: %w", err)
	}
	return nil
}

func (s *PostgresStore) CancelScheduleExact(ctx context.Context, sc runtimepipeline.Schedule) error {
	if strings.TrimSpace(sc.AgentID) == "" || strings.TrimSpace(sc.EventType) == "" {
		return fmt.Errorf("agent_id and event_type are required")
	}
	entityID := sc.EffectiveEntityID()
	sc.EntityID = entityID
	if err := s.cancelScheduleExactSpec(ctx, sc); err == nil {
		return nil
	} else if !shouldFallbackLegacyTimersSchema(err) {
		return err
	}
	_, err := s.DB.ExecContext(ctx, `
		UPDATE schedules
		SET active = false,
		    cancelled_at = now()
		WHERE agent_id = $1
		  AND event_type = $2
		  AND entity_id IS NOT DISTINCT FROM NULLIF($3,'')::uuid
		  AND COALESCE(payload->>'__schedule_task_id', '') = $4
		  AND active = true
	`, sc.AgentID, sc.EventType, entityID, strings.TrimSpace(sc.TaskID))
	if err != nil {
		return fmt.Errorf("cancel exact schedule: %w", err)
	}
	return nil
}

func (s *PostgresStore) LoadActiveSchedules(ctx context.Context) ([]runtimepipeline.Schedule, error) {
	schedules, err := s.loadActiveSchedulesSpec(ctx)
	if err == nil {
		return schedules, nil
	}
	if !shouldFallbackLegacyTimersSchema(err) {
		return nil, err
	}

	rows, err := s.DB.QueryContext(ctx, `
		SELECT
			agent_id,
			event_type,
			mode,
			COALESCE(cron_expr, ''),
			at_time,
			COALESCE(entity_id::text, ''),
			payload
		FROM schedules
		WHERE active = true
	`)
	if err != nil {
		return nil, fmt.Errorf("query active schedules: %w", err)
	}
	defer rows.Close()

	out := make([]runtimepipeline.Schedule, 0)
	for rows.Next() {
		var sc runtimepipeline.Schedule
		var at sql.NullTime
		if err := rows.Scan(
			&sc.AgentID,
			&sc.EventType,
			&sc.Mode,
			&sc.Cron,
			&at,
			&sc.EntityID,
			&sc.Payload,
		); err != nil {
			return nil, fmt.Errorf("scan active schedule: %w", err)
		}
		sc.TaskID, sc.Payload = extractPersistedScheduleTaskID(sc.Payload)
		if at.Valid {
			sc.At = at.Time
		}
		out = append(out, sc)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate active schedules: %w", err)
	}
	return out, nil
}

func (s *PostgresStore) MarkScheduleFired(ctx context.Context, sc runtimepipeline.Schedule) error {
	if strings.TrimSpace(sc.AgentID) == "" || strings.TrimSpace(sc.EventType) == "" {
		return nil
	}
	if err := s.markScheduleFiredSpec(ctx, sc); err == nil {
		return nil
	} else if !shouldFallbackLegacyTimersSchema(err) {
		return err
	}
	if sc.Mode == "once" {
		_, err := s.DB.ExecContext(ctx, `
			UPDATE schedules
			SET active = false,
			    last_fired_at = now(),
			    next_fire_at = NULL
			WHERE agent_id = $1
			  AND event_type = $2
			  AND active = true
		`, sc.AgentID, sc.EventType)
		if err != nil {
			return fmt.Errorf("mark once schedule fired: %w", err)
		}
		return nil
	}
	_, err := s.DB.ExecContext(ctx, `
		UPDATE schedules
		SET last_fired_at = now()
		WHERE agent_id = $1
		  AND event_type = $2
		  AND active = true
	`, sc.AgentID, sc.EventType)
	if err != nil {
		return fmt.Errorf("mark recurring schedule fired: %w", err)
	}
	return nil
}

func (s *PostgresStore) MarkScheduleFiredExact(ctx context.Context, sc runtimepipeline.Schedule) error {
	if strings.TrimSpace(sc.AgentID) == "" || strings.TrimSpace(sc.EventType) == "" {
		return nil
	}
	entityID := sc.EffectiveEntityID()
	sc.EntityID = entityID
	if err := s.markScheduleFiredExactSpec(ctx, sc); err == nil {
		return nil
	} else if !shouldFallbackLegacyTimersSchema(err) {
		return err
	}
	if sc.Mode == "once" {
		_, err := s.DB.ExecContext(ctx, `
			UPDATE schedules
			SET active = false,
			    last_fired_at = now(),
			    next_fire_at = NULL
			WHERE agent_id = $1
			  AND event_type = $2
			  AND entity_id IS NOT DISTINCT FROM NULLIF($3,'')::uuid
			  AND COALESCE(payload->>'__schedule_task_id', '') = $4
			  AND active = true
		`, sc.AgentID, sc.EventType, entityID, strings.TrimSpace(sc.TaskID))
		if err != nil {
			return fmt.Errorf("mark exact once schedule fired: %w", err)
		}
		return nil
	}
	_, err := s.DB.ExecContext(ctx, `
		UPDATE schedules
		SET last_fired_at = now()
		WHERE agent_id = $1
		  AND event_type = $2
		  AND entity_id IS NOT DISTINCT FROM NULLIF($3,'')::uuid
		  AND COALESCE(payload->>'__schedule_task_id', '') = $4
		  AND active = true
	`, sc.AgentID, sc.EventType, entityID, strings.TrimSpace(sc.TaskID))
	if err != nil {
		return fmt.Errorf("mark exact schedule fired: %w", err)
	}
	return nil
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

func (s *PostgresStore) upsertScheduleSpec(ctx context.Context, sc runtimepipeline.Schedule) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin timer tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	payload := persistedSchedulePayload(sc)
	if _, err := tx.ExecContext(ctx, `
		UPDATE timers
		SET status = 'cancelled'
		WHERE owner_agent = $1
		  AND fire_event = $2
		  AND entity_id IS NOT DISTINCT FROM NULLIF($3,'')::uuid
		  AND COALESCE(fire_payload->>'__schedule_task_id', '') = $4
		  AND status = 'active'
	`, sc.AgentID, sc.EventType, sc.EntityID, strings.TrimSpace(sc.TaskID)); err != nil {
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
			$1, NULLIF($2,'')::uuid, NULL, $3, $4::jsonb,
			$5, $6, NULLIF($7,''), NULL,
			NULL, $8, $9, 'active'
		)
	`, timerName, sc.EntityID, sc.EventType, string(payload), fireAt, recurring, sc.Cron, sc.AgentID, taskType)
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
	_, err := s.DB.ExecContext(ctx, `
		UPDATE timers
		SET status = 'cancelled'
		WHERE owner_agent = $1
		  AND fire_event = $2
		  AND entity_id IS NOT DISTINCT FROM NULLIF($3,'')::uuid
		  AND COALESCE(fire_payload->>'__schedule_task_id', '') = $4
		  AND status = 'active'
	`, sc.AgentID, sc.EventType, sc.EntityID, strings.TrimSpace(sc.TaskID))
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
	_, err := s.DB.ExecContext(ctx, `
		UPDATE timers
		SET status = $5,
		    fired_at = now()
		WHERE owner_agent = $1
		  AND fire_event = $2
		  AND entity_id IS NOT DISTINCT FROM NULLIF($3,'')::uuid
		  AND COALESCE(fire_payload->>'__schedule_task_id', '') = $4
		  AND status = 'active'
	`, sc.AgentID, sc.EventType, sc.EntityID, strings.TrimSpace(sc.TaskID), status)
	if err != nil {
		return fmt.Errorf("mark exact timer fired: %w", err)
	}
	return nil
}

func shouldFallbackLegacyTimersSchema(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(strings.TrimSpace(err.Error()))
	return strings.Contains(msg, `relation "timers" does not exist`) ||
		strings.Contains(msg, `column "owner_agent"`) ||
		strings.Contains(msg, `column "fire_event"`) ||
		strings.Contains(msg, `column "fire_payload"`) ||
		strings.Contains(msg, `column "recurrence_cron"`) ||
		strings.Contains(msg, `column "status"`) ||
		strings.Contains(msg, `column "timer_name"`)
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
