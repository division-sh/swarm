package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	timerobligationadapter "github.com/division-sh/swarm/internal/persistence/timerobligationadapter"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimetimerobligation "github.com/division-sh/swarm/internal/runtime/timerobligation"
	runforkrevision "github.com/division-sh/swarm/internal/store/internal/runforkrevision"
)

func (s *PostgresStore) ReadTimerObligations(ctx context.Context, scope runtimetimerobligation.Scope, observedAt time.Time) (runtimetimerobligation.Snapshot, error) {
	if s == nil || s.backend == nil {
		return runtimetimerobligation.Snapshot{}, fmt.Errorf("timer obligation reader requires PostgreSQL store")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return runtimetimerobligation.Snapshot{}, err
	}
	return timerobligationadapter.Read(ctx, s.backend, timerobligationadapter.DialectPostgres, scope, observedAt)
}

func (s *PostgresStore) UpsertSchedule(ctx context.Context, sc runtimepipeline.Schedule) error {
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	if strings.TrimSpace(sc.AgentID) == "" || strings.TrimSpace(sc.EventType) == "" {
		return fmt.Errorf("agent_id and event_type are required")
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
	if err := sc.NormalizeOwner(); err != nil {
		return err
	}
	var err error
	sc, err = sc.AdmitEventIdentity()
	if err != nil {
		return err
	}

	return s.upsertScheduleSpec(ctx, sc)
}

func (s *PostgresStore) CancelScheduleExact(ctx context.Context, sc runtimepipeline.Schedule) error {
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	if strings.TrimSpace(sc.AgentID) == "" || strings.TrimSpace(sc.EventType) == "" {
		return fmt.Errorf("agent_id and event_type are required")
	}
	entityID := sc.EffectiveEntityID()
	sc.EntityID = entityID
	sc = scheduleWithContextRunID(ctx, sc)
	sc.NormalizeRunID()
	flowInstance := sc.EffectiveFlowInstance()
	sc.FlowInstance = flowInstance
	if err := sc.NormalizeOwner(); err != nil {
		return err
	}
	var err error
	sc, err = sc.AdmitEventIdentity()
	if err != nil {
		return err
	}
	return s.cancelScheduleExactSpec(ctx, sc)
}

func (s *PostgresStore) LoadActiveSchedules(ctx context.Context) ([]runtimepipeline.Schedule, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	return s.loadActiveSchedulesSpec(ctx)
}

func (s *PostgresStore) MarkScheduleFiredExact(ctx context.Context, sc runtimepipeline.Schedule) error {
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	if strings.TrimSpace(sc.AgentID) == "" || strings.TrimSpace(sc.EventType) == "" {
		return nil
	}
	entityID := sc.EffectiveEntityID()
	sc.EntityID = entityID
	sc = scheduleWithContextRunID(ctx, sc)
	sc.NormalizeRunID()
	flowInstance := sc.EffectiveFlowInstance()
	sc.FlowInstance = flowInstance
	if err := sc.NormalizeOwner(); err != nil {
		return err
	}
	var err error
	sc, err = sc.AdmitEventIdentity()
	if err != nil {
		return err
	}
	return s.markScheduleFiredExactSpec(ctx, sc)
}

func scheduleAgentIdentityFields(sc runtimepipeline.Schedule) (agentidentity.StorageFields, error) {
	if err := sc.NormalizeOwner(); err != nil {
		return agentidentity.StorageFields{}, err
	}
	if sc.OwnerKind == runtimepipeline.ScheduleOwnerSystem {
		return agentidentity.StorageFields{}, nil
	}
	return sc.AgentIdentity.StorageFields()
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

func persistedScheduleRoutingSource(sc runtimepipeline.Schedule) ([]byte, error) {
	if err := sc.ValidateRoutingSource(); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(sc.RoutingSource)
	if err != nil {
		return nil, fmt.Errorf("encode schedule routing source: %w", err)
	}
	return raw, nil
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
	timerName := taskID
	if timerName == "" {
		timerName = strings.TrimSpace(sc.EventType)
	}
	return timerName, nil
}

func (s *PostgresStore) upsertScheduleSpec(ctx context.Context, sc runtimepipeline.Schedule) error {
	return s.runScheduleTransaction(ctx, "timer", func(tx *sql.Tx) error {
		if strings.TrimSpace(sc.RunID) != "" {
			if err := requirePostgresRunActive(ctx, tx, sc.RunID); err != nil {
				return err
			}
		}

		identityFields, err := scheduleAgentIdentityFields(sc)
		if err != nil {
			return err
		}
		payload := persistedSchedulePayload(sc)
		if _, err := tx.ExecContext(ctx, fmt.Sprintf(`
			UPDATE timers
			SET status = 'cancelled'
		WHERE run_id IS NOT DISTINCT FROM NULLIF($1,'')::uuid
		  AND owner_agent = $2
		  AND owner_kind = $7
		  AND agent_name_owner IS NOT DISTINCT FROM NULLIF($8, '')
		  AND agent_name_source IS NOT DISTINCT FROM NULLIF($9, '')
		  AND agent_route_presence IS NOT DISTINCT FROM NULLIF($10, '')
		  AND agent_flow_scope_key IS NOT DISTINCT FROM NULLIF($11, '')
		  AND agent_flow_instance_id IS NOT DISTINCT FROM NULLIF($12, '')
		  AND fire_event = $3
		  AND entity_id IS NOT DISTINCT FROM NULLIF($4,'')::uuid
			  AND flow_instance IS NOT DISTINCT FROM NULLIF($5,'')
			  AND %s = $6
			  AND task_type IN ('timer', 'scheduled_task', 'deadline', 'global_recurring')
			  AND status = 'active'
		`, exactScheduleTaskIDSQL()), sc.RunID, sc.AgentID, sc.EventType, sc.EntityID, sc.FlowInstance, strings.TrimSpace(sc.TaskID),
			sc.OwnerKind, identityFields.NameOwner, identityFields.NameSource, identityFields.RoutePresence, identityFields.FlowScopeKey, identityFields.FlowInstanceID); err != nil {
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
		routingSource, err := persistedScheduleRoutingSource(sc)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `
			INSERT INTO timers (
			run_id, timer_name, entity_id, flow_instance, fire_event, fire_payload, routing_source,
			fire_at, recurring, recurrence_cron, recurrence_interval,
			owner_node, owner_agent, owner_kind, agent_name_owner, agent_name_source,
			agent_route_presence, agent_flow_scope_key, agent_flow_instance_id,
			reply_context_id, task_type, status
		)
		VALUES (
			NULLIF($1,'')::uuid, $2, NULLIF($3,'')::uuid, NULLIF($4,''), $5, $6::jsonb, $7::jsonb,
			$8, $9, NULLIF($10,''), NULL,
			NULL, $11, $12, NULLIF($13, ''), NULLIF($14, ''),
			NULLIF($15, ''), NULLIF($16, ''), NULLIF($17, ''),
			NULLIF($18, ''), $19, 'active'
		)
		`, sc.RunID, timerName, sc.EntityID, sc.FlowInstance, sc.EventType, string(payload), string(routingSource), fireAt, recurring, sc.Cron, sc.AgentID,
			sc.OwnerKind, identityFields.NameOwner, identityFields.NameSource, identityFields.RoutePresence, identityFields.FlowScopeKey,
			identityFields.FlowInstanceID, sc.Context.ReplyContextID(), taskType)
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

func (s *PostgresStore) cancelScheduleExactSpec(ctx context.Context, sc runtimepipeline.Schedule) error {
	return s.runScheduleMutation(ctx, sc.RunID, "cancel exact timer", func(tx *sql.Tx) (bool, error) {
		identityFields, err := scheduleAgentIdentityFields(sc)
		if err != nil {
			return false, err
		}
		result, err := tx.ExecContext(ctx, fmt.Sprintf(`
			UPDATE timers
			SET status = 'cancelled'
		WHERE run_id IS NOT DISTINCT FROM NULLIF($1,'')::uuid
		  AND owner_agent = $2
		  AND owner_kind = $7
		  AND agent_name_owner IS NOT DISTINCT FROM NULLIF($8, '')
		  AND agent_name_source IS NOT DISTINCT FROM NULLIF($9, '')
		  AND agent_route_presence IS NOT DISTINCT FROM NULLIF($10, '')
		  AND agent_flow_scope_key IS NOT DISTINCT FROM NULLIF($11, '')
		  AND agent_flow_instance_id IS NOT DISTINCT FROM NULLIF($12, '')
		  AND fire_event = $3
		  AND entity_id IS NOT DISTINCT FROM NULLIF($4,'')::uuid
			  AND flow_instance IS NOT DISTINCT FROM NULLIF($5,'')
			  AND %s = $6
			  AND task_type IN ('timer', 'scheduled_task', 'deadline', 'global_recurring')
			  AND status = 'active'
		`, exactScheduleTaskIDSQL()), sc.RunID, sc.AgentID, sc.EventType, sc.EntityID, sc.FlowInstance, strings.TrimSpace(sc.TaskID),
			sc.OwnerKind, identityFields.NameOwner, identityFields.NameSource, identityFields.RoutePresence, identityFields.FlowScopeKey, identityFields.FlowInstanceID)
		return scheduleMutationChanged(result, err)
	})
}

func (s *PostgresStore) loadActiveSchedulesSpec(ctx context.Context) ([]runtimepipeline.Schedule, error) {
	rows, err := s.backend.QueryContext(ctx, `
		SELECT
			COALESCE(t.run_id::text, ''),
			t.owner_agent,
			t.owner_kind,
			COALESCE(t.agent_name_owner, ''),
			COALESCE(t.agent_name_source, ''),
			COALESCE(t.agent_route_presence, ''),
			COALESCE(t.agent_flow_scope_key, ''),
			COALESCE(t.agent_flow_instance_id, ''),
			t.fire_event,
			t.recurring,
			COALESCE(t.recurrence_cron, ''),
			COALESCE(t.recurrence_interval, ''),
			t.fire_at,
			COALESCE(t.entity_id::text, ''),
			COALESCE(t.flow_instance, ''),
			t.fire_payload,
			t.routing_source,
			COALESCE(t.reply_context_id, '')
		FROM timers t
		LEFT JOIN runs run ON run.run_id = t.run_id
		WHERE t.status = 'active'
		  AND t.owner_agent IS NOT NULL
		  AND t.task_type IN ('timer', 'scheduled_task', 'deadline', 'global_recurring')
		  AND (t.run_id IS NULL OR run.status IN (`+runLifecycleActiveStateSQLValues+`))
		ORDER BY t.created_at ASC
	`)
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
			routingSourceRaw   []byte
			replyContextID     string
			nameOwner          string
			nameSource         string
			routePresence      string
			flowScopeKey       string
			flowInstanceID     string
		)
		if err := rows.Scan(
			&sc.RunID,
			&sc.AgentID,
			&sc.OwnerKind,
			&nameOwner,
			&nameSource,
			&routePresence,
			&flowScopeKey,
			&flowInstanceID,
			&sc.EventType,
			&recurring,
			&recurrenceCron,
			&recurrenceInterval,
			&fireAt,
			&sc.EntityID,
			&sc.FlowInstance,
			&payload,
			&routingSourceRaw,
			&replyContextID,
		); err != nil {
			return nil, fmt.Errorf("scan active timer: %w", err)
		}
		sc.At = fireAt
		if err := json.Unmarshal(routingSourceRaw, &sc.RoutingSource); err != nil {
			return nil, fmt.Errorf("load schedule routing source: %w", err)
		}
		sc.TaskID, sc.Payload = extractPersistedScheduleTaskID(payload)
		sc.Mode, sc.Cron, err = persistedScheduleMode(recurring, recurrenceCron, recurrenceInterval)
		if err != nil {
			return nil, fmt.Errorf("load generic timer recurrence: %w", err)
		}
		if replyContextID != "" {
			sc.Context = events.DeliveryContext{Reply: &events.ReplyContextRef{ID: replyContextID}}
		}
		if sc.OwnerKind == runtimepipeline.ScheduleOwnerAgent {
			sc.AgentIdentity, err = agentidentity.FromStorageFields(agentidentity.StorageFields{
				AgentID:          sc.AgentID,
				NameOwner:        nameOwner,
				NameSource:       nameSource,
				RoutePresence:    routePresence,
				FlowScopeKey:     flowScopeKey,
				FlowInstanceID:   flowInstanceID,
				FlowInstancePath: sc.FlowInstance,
			})
			if err != nil {
				return nil, fmt.Errorf("load schedule agent identity: %w", err)
			}
		}
		if err := sc.NormalizeOwner(); err != nil {
			return nil, fmt.Errorf("load schedule owner: %w", err)
		}
		sc, err = sc.AdmitEventIdentity()
		if err != nil {
			return nil, fmt.Errorf("load schedule event identity: %w", err)
		}
		out = append(out, sc)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate active timers: %w", err)
	}
	return out, nil
}

func (s *PostgresStore) markScheduleFiredExactSpec(ctx context.Context, sc runtimepipeline.Schedule) error {
	return s.runScheduleMutation(ctx, sc.RunID, "mark exact timer fired", func(tx *sql.Tx) (bool, error) {
		identityFields, err := scheduleAgentIdentityFields(sc)
		if err != nil {
			return false, err
		}
		result, err := tx.ExecContext(ctx, fmt.Sprintf(`
			UPDATE timers
			SET status = CASE WHEN recurring THEN 'active' ELSE 'fired' END,
			    fired_at = now()
		WHERE run_id IS NOT DISTINCT FROM NULLIF($1,'')::uuid
		  AND owner_agent = $2
		  AND owner_kind = $7
		  AND agent_name_owner IS NOT DISTINCT FROM NULLIF($8, '')
		  AND agent_name_source IS NOT DISTINCT FROM NULLIF($9, '')
		  AND agent_route_presence IS NOT DISTINCT FROM NULLIF($10, '')
		  AND agent_flow_scope_key IS NOT DISTINCT FROM NULLIF($11, '')
		  AND agent_flow_instance_id IS NOT DISTINCT FROM NULLIF($12, '')
		  AND fire_event = $3
		  AND entity_id IS NOT DISTINCT FROM NULLIF($4,'')::uuid
			  AND flow_instance IS NOT DISTINCT FROM NULLIF($5,'')
			  AND %s = $6
			  AND task_type IN ('timer', 'scheduled_task', 'deadline', 'global_recurring')
			  AND status = 'active'
		`, exactScheduleTaskIDSQL()), sc.RunID, sc.AgentID, sc.EventType, sc.EntityID, sc.FlowInstance, strings.TrimSpace(sc.TaskID),
			sc.OwnerKind, identityFields.NameOwner, identityFields.NameSource, identityFields.RoutePresence, identityFields.FlowScopeKey, identityFields.FlowInstanceID)
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
			if err := requirePostgresRunActive(ctx, tx, runID); err != nil {
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
	tx, err := s.backend.BeginTx(ctx, nil)
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
