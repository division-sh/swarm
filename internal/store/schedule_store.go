package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runforkrevision "github.com/division-sh/swarm/internal/runtime/runforkrevision"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
)

func (s *PostgresStore) UpsertSchedule(ctx context.Context, sc runtimepipeline.Schedule) error {
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	if strings.TrimSpace(sc.AgentID) == "" || strings.TrimSpace(sc.EventType) == "" {
		return fmt.Errorf("agent_id and event_type are required")
	}
	if sc.EffectiveTimerID() != "" {
		return fmt.Errorf("workflow timer activations must be persisted by WorkflowTimerLifecycle")
	}
	if _, err := genericScheduleTimerName(sc); err != nil {
		return err
	}
	if strings.TrimSpace(sc.Mode) == "" {
		sc.Mode = "once"
	}
	sc = scheduleWithContextRunID(ctx, sc)
	if sc.Context.Empty() {
		sc.Context = events.DeliveryContextFromContext(ctx)
	}
	sc.NormalizeDeliveryContext()
	if !sc.Context.Empty() && strings.EqualFold(strings.TrimSpace(sc.Mode), "cron") {
		return fmt.Errorf("recurring schedules cannot carry an open reply context")
	}
	entityID := sc.EffectiveEntityID()
	sc.EntityID = entityID
	sc.NormalizeRunID()
	flowInstance := sc.EffectiveFlowInstance()
	sc.FlowInstance = flowInstance

	return s.upsertScheduleSpec(ctx, sc)
}

func (s *PostgresStore) CancelSchedule(ctx context.Context, agentID, eventType string) error {
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	if strings.TrimSpace(agentID) == "" || strings.TrimSpace(eventType) == "" {
		return fmt.Errorf("agent_id and event_type are required")
	}
	return s.cancelScheduleSpec(ctx, runtimecorrelation.RunIDFromContext(ctx), agentID, eventType)
}

func (s *PostgresStore) CancelScheduleExact(ctx context.Context, sc runtimepipeline.Schedule) error {
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	if strings.TrimSpace(sc.AgentID) == "" || strings.TrimSpace(sc.EventType) == "" {
		return fmt.Errorf("agent_id and event_type are required")
	}
	if sc.EffectiveTimerID() != "" {
		return fmt.Errorf("workflow timer cancellation must be owned by WorkflowTimerLifecycle")
	}
	entityID := sc.EffectiveEntityID()
	sc.EntityID = entityID
	sc = scheduleWithContextRunID(ctx, sc)
	sc.NormalizeRunID()
	flowInstance := sc.EffectiveFlowInstance()
	sc.FlowInstance = flowInstance
	return s.cancelScheduleExactSpec(ctx, sc)
}

func (s *PostgresStore) LoadActiveSchedules(ctx context.Context) ([]runtimepipeline.Schedule, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	return s.loadActiveSchedulesSpec(ctx)
}

func (s *PostgresStore) MarkScheduleFired(ctx context.Context, sc runtimepipeline.Schedule) error {
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	if strings.TrimSpace(sc.AgentID) == "" || strings.TrimSpace(sc.EventType) == "" {
		return nil
	}
	sc = scheduleWithContextRunID(ctx, sc)
	sc.NormalizeRunID()
	return s.markScheduleFiredSpec(ctx, sc)
}

func (s *PostgresStore) MarkScheduleFiredExact(ctx context.Context, sc runtimepipeline.Schedule) error {
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	if strings.TrimSpace(sc.AgentID) == "" || strings.TrimSpace(sc.EventType) == "" {
		return nil
	}
	if sc.EffectiveTimerID() != "" {
		return fmt.Errorf("workflow timer completion must be owned by WorkflowTimerLifecycle")
	}
	entityID := sc.EffectiveEntityID()
	sc.EntityID = entityID
	sc = scheduleWithContextRunID(ctx, sc)
	sc.NormalizeRunID()
	flowInstance := sc.EffectiveFlowInstance()
	sc.FlowInstance = flowInstance
	return s.markScheduleFiredExactSpec(ctx, sc)
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

func scheduleWithContextRunID(ctx context.Context, sc runtimepipeline.Schedule) runtimepipeline.Schedule {
	if strings.TrimSpace(sc.RunID) == "" {
		sc.RunID = runtimecorrelation.RunIDFromContext(ctx)
	}
	return sc
}

func genericScheduleTimerName(sc runtimepipeline.Schedule) (string, error) {
	taskID := strings.TrimSpace(sc.TaskID)
	if _, ok := timeridentity.ParseWorkflowTimerOccurrenceTaskID(taskID); ok {
		return "", fmt.Errorf("workflow timer occurrences must be persisted by WorkflowTimerLifecycle")
	}
	timerName := taskID
	if timerName == "" {
		timerName = strings.TrimSpace(sc.EventType)
	}
	if strings.HasPrefix(timerName, timeridentity.WorkflowTimerActivationTaskPrefix()) {
		return "", fmt.Errorf("generic schedule timer name %q uses reserved workflow timer prefix", timerName)
	}
	return timerName, nil
}

func (s *PostgresStore) upsertScheduleSpec(ctx context.Context, sc runtimepipeline.Schedule) error {
	return s.runScheduleTransaction(ctx, "timer", func(tx *sql.Tx) error {
		if strings.TrimSpace(sc.RunID) != "" {
			if err := storerunlifecycle.RequireActive(ctx, tx, sc.RunID, storerunlifecycle.DialectPostgres); err != nil {
				return err
			}
		}

		payload := persistedSchedulePayload(sc)
		if _, err := tx.ExecContext(ctx, fmt.Sprintf(`
			UPDATE timers
			SET status = 'cancelled'
		WHERE run_id IS NOT DISTINCT FROM NULLIF($1,'')::uuid
		  AND owner_agent = $2
		  AND fire_event = $3
		  AND entity_id IS NOT DISTINCT FROM NULLIF($4,'')::uuid
			  AND flow_instance IS NOT DISTINCT FROM NULLIF($5,'')
			  AND %s = $6
			  AND strpos(timer_name, $7) <> 1
			  AND status = 'active'
		`, exactScheduleTaskIDSQL()), sc.RunID, sc.AgentID, sc.EventType, sc.EntityID, sc.FlowInstance, strings.TrimSpace(sc.TaskID), timeridentity.WorkflowTimerActivationTaskPrefix()); err != nil {
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
		timerName, err := genericScheduleTimerName(sc)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `
			INSERT INTO timers (
			run_id, timer_name, entity_id, flow_instance, fire_event, fire_payload,
			fire_at, recurring, recurrence_cron, recurrence_interval,
			owner_node, owner_agent, reply_context_id, task_type, status
		)
		VALUES (
			NULLIF($1,'')::uuid, $2, NULLIF($3,'')::uuid, NULLIF($4,''), $5, $6::jsonb,
			$7, $8, NULLIF($9,''), NULL,
			NULL, $10, NULLIF($11, ''), $12, 'active'
		)
		`, sc.RunID, timerName, sc.EntityID, sc.FlowInstance, sc.EventType, string(payload), fireAt, recurring, sc.Cron, sc.AgentID, sc.Context.ReplyContextID(), taskType)
		if err != nil {
			return fmt.Errorf("insert timer: %w", err)
		}
		if strings.TrimSpace(sc.RunID) != "" {
			if _, err := runforkrevision.Capture(ctx, tx, sc.RunID, runforkrevision.FamilyTimers); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *PostgresStore) cancelScheduleSpec(ctx context.Context, runID, agentID, eventType string) error {
	return s.runScheduleMutation(ctx, runID, "cancel timer", func(tx *sql.Tx) (bool, error) {
		result, err := tx.ExecContext(ctx, `
			UPDATE timers
			SET status = 'cancelled'
			WHERE run_id IS NOT DISTINCT FROM NULLIF($1,'')::uuid
			  AND owner_agent = $2
			  AND fire_event = $3
			  AND strpos(timer_name, $4) <> 1
			  AND status = 'active'
		`, runID, agentID, eventType, timeridentity.WorkflowTimerActivationTaskPrefix())
		return scheduleMutationChanged(result, err)
	})
}

func (s *PostgresStore) cancelScheduleExactSpec(ctx context.Context, sc runtimepipeline.Schedule) error {
	return s.runScheduleMutation(ctx, sc.RunID, "cancel exact timer", func(tx *sql.Tx) (bool, error) {
		result, err := tx.ExecContext(ctx, fmt.Sprintf(`
			UPDATE timers
			SET status = 'cancelled'
		WHERE run_id IS NOT DISTINCT FROM NULLIF($1,'')::uuid
		  AND owner_agent = $2
		  AND fire_event = $3
		  AND entity_id IS NOT DISTINCT FROM NULLIF($4,'')::uuid
			  AND flow_instance IS NOT DISTINCT FROM NULLIF($5,'')
			  AND %s = $6
			  AND strpos(timer_name, $7) <> 1
			  AND status = 'active'
		`, exactScheduleTaskIDSQL()), sc.RunID, sc.AgentID, sc.EventType, sc.EntityID, sc.FlowInstance, strings.TrimSpace(sc.TaskID), timeridentity.WorkflowTimerActivationTaskPrefix())
		return scheduleMutationChanged(result, err)
	})
}

func (s *PostgresStore) loadActiveSchedulesSpec(ctx context.Context) ([]runtimepipeline.Schedule, error) {
	rows, err := s.DB.QueryContext(ctx, `
		SELECT
			COALESCE(t.run_id::text, ''),
			t.owner_agent,
			t.fire_event,
			t.recurring,
			COALESCE(t.recurrence_cron, ''),
			COALESCE(t.recurrence_interval, ''),
			t.fire_at,
			COALESCE(t.entity_id::text, ''),
			COALESCE(t.flow_instance, ''),
			t.fire_payload,
			COALESCE(t.reply_context_id, '')
		FROM timers t
		LEFT JOIN runs run ON run.run_id = t.run_id
		WHERE t.status = 'active'
		  AND t.owner_agent IS NOT NULL
		  AND strpos(t.timer_name, $1) <> 1
		  AND (t.run_id IS NULL OR run.status IN ('running', 'paused'))
		ORDER BY t.created_at ASC
	`, timeridentity.WorkflowTimerActivationTaskPrefix())
	if err != nil {
		return nil, fmt.Errorf("query active timers: %w", err)
	}
	defer rows.Close()

	out := make([]runtimepipeline.Schedule, 0)
	for rows.Next() {
		var (
			sc                 runtimepipeline.Schedule
			recurring          bool
			recurrenceCron     string
			recurrenceInterval string
			fireAt             time.Time
			payload            []byte
			replyContextID     string
		)
		if err := rows.Scan(
			&sc.RunID,
			&sc.AgentID,
			&sc.EventType,
			&recurring,
			&recurrenceCron,
			&recurrenceInterval,
			&fireAt,
			&sc.EntityID,
			&sc.FlowInstance,
			&payload,
			&replyContextID,
		); err != nil {
			return nil, fmt.Errorf("scan active timer: %w", err)
		}
		sc.At = fireAt
		sc.TaskID, sc.Payload = extractPersistedScheduleTaskID(payload)
		sc.Mode, sc.Cron, err = persistedScheduleMode(recurring, recurrenceCron, recurrenceInterval)
		if err != nil {
			return nil, fmt.Errorf("load generic timer recurrence: %w", err)
		}
		if replyContextID != "" {
			sc.Context = events.DeliveryContext{Reply: &events.ReplyContextRef{ID: replyContextID}}
		}
		out = append(out, sc)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate active timers: %w", err)
	}
	return out, nil
}

func (s *PostgresStore) markScheduleFiredSpec(ctx context.Context, sc runtimepipeline.Schedule) error {
	return s.runScheduleMutation(ctx, sc.RunID, "mark timer fired", func(tx *sql.Tx) (bool, error) {
		result, err := tx.ExecContext(ctx, `
			UPDATE timers
			SET status = CASE WHEN recurring THEN 'active' ELSE 'fired' END,
			    fired_at = now()
			WHERE run_id IS NOT DISTINCT FROM NULLIF($1,'')::uuid
			  AND owner_agent = $2
			  AND fire_event = $3
			  AND strpos(timer_name, $4) <> 1
			  AND status = 'active'
		`, sc.RunID, sc.AgentID, sc.EventType, timeridentity.WorkflowTimerActivationTaskPrefix())
		return scheduleMutationChanged(result, err)
	})
}

func (s *PostgresStore) markScheduleFiredExactSpec(ctx context.Context, sc runtimepipeline.Schedule) error {
	return s.runScheduleMutation(ctx, sc.RunID, "mark exact timer fired", func(tx *sql.Tx) (bool, error) {
		result, err := tx.ExecContext(ctx, fmt.Sprintf(`
			UPDATE timers
			SET status = CASE WHEN recurring THEN 'active' ELSE 'fired' END,
			    fired_at = now()
		WHERE run_id IS NOT DISTINCT FROM NULLIF($1,'')::uuid
		  AND owner_agent = $2
		  AND fire_event = $3
		  AND entity_id IS NOT DISTINCT FROM NULLIF($4,'')::uuid
			  AND flow_instance IS NOT DISTINCT FROM NULLIF($5,'')
			  AND %s = $6
			  AND strpos(timer_name, $7) <> 1
			  AND status = 'active'
		`, exactScheduleTaskIDSQL()), sc.RunID, sc.AgentID, sc.EventType, sc.EntityID, sc.FlowInstance, strings.TrimSpace(sc.TaskID), timeridentity.WorkflowTimerActivationTaskPrefix())
		return scheduleMutationChanged(result, err)
	})
}

func persistedScheduleMode(recurring bool, recurrenceCron, recurrenceInterval string) (string, string, error) {
	if !recurring {
		return "once", "", nil
	}
	recurrenceCron = strings.TrimSpace(recurrenceCron)
	recurrenceInterval = strings.TrimSpace(recurrenceInterval)
	if recurrenceCron != "" && recurrenceInterval != "" {
		return "", "", fmt.Errorf("recurring timer has both cron and interval recurrence")
	}
	if recurrenceCron != "" {
		return "cron", recurrenceCron, nil
	}
	if interval, ok := timeridentity.ParseDelayDuration(recurrenceInterval); ok {
		return "cron", "@every " + interval.String(), nil
	}
	return "", "", fmt.Errorf("recurring timer is missing a valid recurrence")
}

func (s *PostgresStore) runScheduleMutation(ctx context.Context, runID, label string, mutate func(*sql.Tx) (bool, error)) error {
	return s.runScheduleTransaction(ctx, label, func(tx *sql.Tx) error {
		if strings.TrimSpace(runID) != "" {
			if err := storerunlifecycle.RequireActive(ctx, tx, runID, storerunlifecycle.DialectPostgres); err != nil {
				return err
			}
		}
		changed, err := mutate(tx)
		if err != nil {
			return fmt.Errorf("%s: %w", label, err)
		}
		if changed && strings.TrimSpace(runID) != "" {
			if _, err := runforkrevision.Capture(ctx, tx, runID, runforkrevision.FamilyTimers); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *PostgresStore) runScheduleTransaction(ctx context.Context, label string, mutate func(*sql.Tx) error) error {
	if tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx); ok && tx != nil {
		return mutate(tx)
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin %s: %w", label, err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := mutate(tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit %s: %w", label, err)
	}
	return nil
}

func scheduleMutationChanged(result sql.Result, err error) (bool, error) {
	if err != nil {
		return false, err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return affected > 0, nil
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
