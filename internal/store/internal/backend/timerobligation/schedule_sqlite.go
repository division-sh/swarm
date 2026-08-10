package timerobligation

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	runtimetimerobligation "github.com/division-sh/swarm/internal/runtime/timerobligation"
	storerunstate "github.com/division-sh/swarm/internal/store/internal/backend/runstate"
	"github.com/google/uuid"
)

type sqliteScheduleExecutor interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func (s *ScheduleSQLiteOwner) ReadTimerObligations(ctx context.Context, scope runtimetimerobligation.Scope, observedAt time.Time) (runtimetimerobligation.Snapshot, error) {
	if s == nil || s.backend == nil {
		return runtimetimerobligation.Snapshot{}, fmt.Errorf("timer obligation reader requires SQLite store")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return runtimetimerobligation.Snapshot{}, err
	}
	return s.Read(ctx, scope, observedAt)
}

func (s *ScheduleSQLiteOwner) UpsertSchedule(ctx context.Context, sc runtimepipeline.Schedule) error {
	if strings.TrimSpace(sc.AgentID) == "" || strings.TrimSpace(sc.EventType) == "" {
		return fmt.Errorf("agent_id and event_type are required")
	}
	timerName, err := genericScheduleTimerName(sc)
	if err != nil {
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
	sc.NormalizeRunID()
	sc.NormalizeEntityID()
	sc.NormalizeFlowInstance()
	if err := sc.NormalizeOwner(); err != nil {
		return err
	}
	if err := sc.NormalizeExecutionMode(); err != nil {
		return err
	}
	sc, err = sc.AdmitEventIdentity()
	if err != nil {
		return err
	}
	identityFields, err := scheduleAgentIdentityFields(sc)
	if err != nil {
		return err
	}
	routingSource, err := persistedScheduleRoutingSource(sc)
	if err != nil {
		return err
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
	if err := s.backend.RunTransaction(ctx, "sqlite schedule upsert", func(txctx context.Context, tx *sql.Tx) error {
		if strings.TrimSpace(sc.RunID) != "" {
			if err := storerunstate.RequireSQLiteActiveTx(txctx, tx, sc.RunID); err != nil {
				return err
			}
		}
		if err := cancelSQLiteScheduleExactInTx(txctx, tx, sc); err != nil {
			return err
		}
		_, err := tx.ExecContext(txctx, `
			INSERT INTO timers (
				timer_id, run_id, timer_name, entity_id, flow_instance, fire_event, fire_payload, routing_source, execution_mode,
				fire_at, recurring, recurrence_cron, owner_agent, owner_kind, agent_name_owner,
				agent_name_source, agent_route_presence, agent_flow_scope_key, agent_flow_instance_id,
				reply_context_id, task_type, status, created_at
			)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULLIF(?, ''), NULLIF(?, ''),
			        NULLIF(?, ''), NULLIF(?, ''), NULLIF(?, ''), NULLIF(?, ''), ?, 'active', ?)
		`, uuid.NewString(), sqliteNullUUID(sc.RunID), timerName, sqliteNullUUID(sc.EntityID), sqliteNullString(sc.FlowInstance),
			sc.EventType, string(persistedSchedulePayload(sc)), string(routingSource), sc.ExecutionMode, fireAt.UTC(), recurring, sqliteNullString(sc.Cron), sc.AgentID, sc.OwnerKind,
			identityFields.NameOwner, identityFields.NameSource, identityFields.RoutePresence, identityFields.FlowScopeKey,
			identityFields.FlowInstanceID, sc.Context.ReplyContextID(), taskType, time.Now().UTC())
		return err
	}); err != nil {
		return fmt.Errorf("insert sqlite timer: %w", err)
	}
	return nil
}

func (s *ScheduleSQLiteOwner) CancelScheduleExact(ctx context.Context, sc runtimepipeline.Schedule) error {
	return s.cancelSQLiteScheduleExact(ctx, sc, true)
}

func (s *ScheduleSQLiteOwner) cancelSQLiteScheduleExact(ctx context.Context, sc runtimepipeline.Schedule, requireActive bool) error {
	sc = scheduleWithContextRunID(ctx, sc)
	sc.NormalizeRunID()
	sc.NormalizeEntityID()
	sc.NormalizeFlowInstance()
	if err := sc.NormalizeOwner(); err != nil {
		return err
	}
	var err error
	sc, err = sc.AdmitEventIdentity()
	if err != nil {
		return err
	}
	if err := s.backend.RunTransaction(ctx, "sqlite schedule cancel", func(txctx context.Context, tx *sql.Tx) error {
		if requireActive && strings.TrimSpace(sc.RunID) != "" {
			if err := storerunstate.RequireSQLiteActiveTx(txctx, tx, sc.RunID); err != nil {
				return err
			}
		}
		return cancelSQLiteScheduleExactInTx(txctx, tx, sc)
	}); err != nil {
		return fmt.Errorf("cancel sqlite timer exact: %w", err)
	}
	return nil
}

func cancelSQLiteScheduleExactInTx(ctx context.Context, tx *sql.Tx, sc runtimepipeline.Schedule) error {
	identityFields, err := scheduleAgentIdentityFields(sc)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
			UPDATE timers
			SET status = 'cancelled'
			WHERE COALESCE(run_id, '') = COALESCE(?, '')
			  AND owner_agent = ?
			  AND owner_kind = ?
			  AND COALESCE(agent_name_owner, '') = ?
			  AND COALESCE(agent_name_source, '') = ?
			  AND COALESCE(agent_route_presence, '') = ?
			  AND COALESCE(agent_flow_scope_key, '') = ?
			  AND COALESCE(agent_flow_instance_id, '') = ?
			  AND fire_event = ?
			  AND COALESCE(entity_id, '') = COALESCE(?, '')
			  AND COALESCE(flow_instance, '') = COALESCE(?, '')
			  AND COALESCE(json_extract(fire_payload, '$.__schedule_task_id'), '') = ?
			  AND task_type IN ('timer', 'scheduled_task', 'deadline', 'global_recurring')
			  AND status = 'active'
		`, sqliteNullUUID(sc.RunID), sc.AgentID, sc.OwnerKind, identityFields.NameOwner, identityFields.NameSource,
		identityFields.RoutePresence, identityFields.FlowScopeKey, identityFields.FlowInstanceID,
		sc.EventType, sqliteNullUUID(sc.EntityID), sqliteNullString(sc.FlowInstance), strings.TrimSpace(sc.TaskID))
	return err
}

func (s *ScheduleSQLiteOwner) CancelScheduleExactTerminal(ctx context.Context, sc runtimepipeline.Schedule) error {
	return s.cancelSQLiteScheduleExact(ctx, sc, true)
}

func (s *ScheduleSQLiteOwner) LoadActiveSchedules(ctx context.Context) ([]runtimepipeline.Schedule, error) {
	exec := sqliteScheduleExecutor(s.backend)
	rows, err := exec.QueryContext(ctx, `
		SELECT COALESCE(t.run_id, ''), COALESCE(t.owner_agent, ''), t.owner_kind,
		       COALESCE(t.agent_name_owner, ''), COALESCE(t.agent_name_source, ''),
		       COALESCE(t.agent_route_presence, ''), COALESCE(t.agent_flow_scope_key, ''),
		       COALESCE(t.agent_flow_instance_id, ''), t.fire_event, t.recurring,
		       COALESCE(t.recurrence_cron, ''), COALESCE(t.recurrence_interval, ''),
		       t.fire_at, COALESCE(t.entity_id, ''), COALESCE(t.flow_instance, ''), COALESCE(t.fire_payload, '{}'), t.routing_source, t.execution_mode, COALESCE(t.reply_context_id, '')
		FROM timers t
		LEFT JOIN runs run ON run.run_id = t.run_id
		WHERE t.status = 'active'
		  AND COALESCE(t.owner_agent, '') <> ''
		  AND t.task_type IN ('timer', 'scheduled_task', 'deadline', 'global_recurring')
		  AND (t.run_id IS NULL OR run.status IN (`+storerunstate.ActiveStateSQLValues+`))
		ORDER BY t.fire_at ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("load sqlite active schedules: %w", err)
	}
	defer rows.Close()
	out := make([]runtimepipeline.Schedule, 0)
	for rows.Next() {
		var sc runtimepipeline.Schedule
		var recurring bool
		var recurrenceCron, recurrenceInterval string
		var nameOwner, nameSource, routePresence, flowScopeKey, flowInstanceID string
		var fireAt any
		var payload any
		var routingSourceRaw any
		var replyContextID string
		if err := rows.Scan(&sc.RunID, &sc.AgentID, &sc.OwnerKind, &nameOwner, &nameSource, &routePresence,
			&flowScopeKey, &flowInstanceID, &sc.EventType, &recurring, &recurrenceCron, &recurrenceInterval,
			&fireAt, &sc.EntityID, &sc.FlowInstance, &payload, &routingSourceRaw, &sc.ExecutionMode, &replyContextID); err != nil {
			return nil, fmt.Errorf("scan sqlite schedule: %w", err)
		}
		if at, err := timerTimeValue(fireAt); err != nil {
			return nil, fmt.Errorf("scan sqlite schedule fire_at: %w", err)
		} else {
			sc.At = at
		}
		sc.Payload = jsonRawMessageValue(payload)
		if err := json.Unmarshal(jsonRawMessageValue(routingSourceRaw), &sc.RoutingSource); err != nil {
			return nil, fmt.Errorf("load sqlite schedule routing source: %w", err)
		}
		sc.Mode, sc.Cron, err = persistedScheduleMode(recurring, recurrenceCron, recurrenceInterval)
		if err != nil {
			return nil, fmt.Errorf("load sqlite generic timer recurrence: %w", err)
		}
		sc.TaskID = scheduleTaskIDFromPayload(sc.Payload)
		if replyContextID != "" {
			sc.Context = events.DeliveryContext{Reply: &events.ReplyContextRef{ID: replyContextID}}
		}
		if sc.OwnerKind == runtimepipeline.ScheduleOwnerAgent {
			sc.AgentIdentity, err = agentidentity.FromStorageFields(agentidentity.StorageFields{
				AgentID: sc.AgentID, NameOwner: nameOwner, NameSource: nameSource,
				RoutePresence: routePresence, FlowScopeKey: flowScopeKey,
				FlowInstanceID: flowInstanceID, FlowInstancePath: sc.FlowInstance,
			})
			if err != nil {
				return nil, fmt.Errorf("load sqlite schedule agent identity: %w", err)
			}
		}
		if err := sc.NormalizeOwner(); err != nil {
			return nil, fmt.Errorf("load sqlite schedule owner: %w", err)
		}
		if err := sc.NormalizeExecutionMode(); err != nil {
			return nil, fmt.Errorf("load sqlite schedule execution mode: %w", err)
		}
		sc, err = sc.AdmitEventIdentity()
		if err != nil {
			return nil, fmt.Errorf("load sqlite schedule event identity: %w", err)
		}
		out = append(out, sc)
	}
	return out, rows.Err()
}

func (s *ScheduleSQLiteOwner) MarkScheduleFiredExact(ctx context.Context, sc runtimepipeline.Schedule) error {
	sc = scheduleWithContextRunID(ctx, sc)
	sc.NormalizeRunID()
	sc.NormalizeEntityID()
	sc.NormalizeFlowInstance()
	if err := sc.NormalizeOwner(); err != nil {
		return err
	}
	var err error
	sc, err = sc.AdmitEventIdentity()
	if err != nil {
		return err
	}
	identityFields, err := scheduleAgentIdentityFields(sc)
	if err != nil {
		return err
	}
	if err := s.backend.RunTransaction(ctx, "sqlite schedule fired", func(txctx context.Context, tx *sql.Tx) error {
		if strings.TrimSpace(sc.RunID) != "" {
			if err := storerunstate.RequireSQLiteActiveTx(txctx, tx, sc.RunID); err != nil {
				return err
			}
		}
		_, err := tx.ExecContext(txctx, `
			UPDATE timers
			SET status = CASE WHEN recurring THEN 'active' ELSE 'fired' END, fired_at = ?
			WHERE COALESCE(run_id, '') = COALESCE(?, '')
			  AND owner_agent = ?
			  AND owner_kind = ?
			  AND COALESCE(agent_name_owner, '') = ?
			  AND COALESCE(agent_name_source, '') = ?
			  AND COALESCE(agent_route_presence, '') = ?
			  AND COALESCE(agent_flow_scope_key, '') = ?
			  AND COALESCE(agent_flow_instance_id, '') = ?
			  AND fire_event = ?
			  AND COALESCE(entity_id, '') = COALESCE(?, '')
			  AND COALESCE(flow_instance, '') = COALESCE(?, '')
			  AND COALESCE(json_extract(fire_payload, '$.__schedule_task_id'), '') = ?
			  AND task_type IN ('timer', 'scheduled_task', 'deadline', 'global_recurring')
			  AND status = 'active'
		`, time.Now().UTC(), sqliteNullUUID(sc.RunID), sc.AgentID, sc.OwnerKind, identityFields.NameOwner,
			identityFields.NameSource, identityFields.RoutePresence, identityFields.FlowScopeKey, identityFields.FlowInstanceID,
			sc.EventType, sqliteNullUUID(sc.EntityID), sqliteNullString(sc.FlowInstance), strings.TrimSpace(sc.TaskID))
		return err
	}); err != nil {
		return fmt.Errorf("mark sqlite timer fired exact: %w", err)
	}
	return nil
}

func (s *ScheduleSQLiteOwner) CompleteScheduleFireExact(ctx context.Context, sc runtimepipeline.Schedule) error {
	return s.MarkScheduleFiredExact(ctx, sc)
}

func (s *ScheduleSQLiteOwner) ClaimSchedule(ctx context.Context, sc runtimepipeline.Schedule) (bool, error) {
	sc = scheduleWithContextRunID(ctx, sc)
	sc.NormalizeRunID()
	sc.NormalizeEntityID()
	sc.NormalizeFlowInstance()
	if err := sc.NormalizeOwner(); err != nil {
		return false, err
	}
	var err error
	sc, err = sc.AdmitEventIdentity()
	if err != nil {
		return false, err
	}
	identityFields, err := scheduleAgentIdentityFields(sc)
	if err != nil {
		return false, err
	}
	var active bool
	exec := sqliteScheduleExecutor(s.backend)
	if strings.TrimSpace(sc.RunID) != "" {
		if err := storerunstate.RequireSQLiteActiveQuery(ctx, exec, sc.RunID); err != nil {
			if errors.Is(err, runtimerunlifecycle.ErrRunNotActive) {
				return false, nil
			}
			return false, err
		}
	}
	err = exec.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM timers t
			LEFT JOIN runs run ON run.run_id = t.run_id
			WHERE COALESCE(t.run_id, '') = COALESCE(?, '')
			  AND t.owner_agent = ?
			  AND t.owner_kind = ?
			  AND COALESCE(t.agent_name_owner, '') = ?
			  AND COALESCE(t.agent_name_source, '') = ?
			  AND COALESCE(t.agent_route_presence, '') = ?
			  AND COALESCE(t.agent_flow_scope_key, '') = ?
			  AND COALESCE(t.agent_flow_instance_id, '') = ?
			  AND t.fire_event = ?
			  AND COALESCE(t.entity_id, '') = COALESCE(?, '')
			  AND COALESCE(t.flow_instance, '') = COALESCE(?, '')
			  AND COALESCE(json_extract(t.fire_payload, '$.__schedule_task_id'), '') = ?
			  AND t.task_type IN ('timer', 'scheduled_task', 'deadline', 'global_recurring')
			  AND t.status = 'active'
			  AND (t.run_id IS NULL OR run.status IN (`+storerunstate.ActiveStateSQLValues+`))
		)
	`, sqliteNullUUID(sc.RunID), sc.AgentID, sc.OwnerKind, identityFields.NameOwner, identityFields.NameSource,
		identityFields.RoutePresence, identityFields.FlowScopeKey, identityFields.FlowInstanceID,
		sc.EventType, sqliteNullUUID(sc.EntityID), sqliteNullString(sc.FlowInstance), strings.TrimSpace(sc.TaskID)).Scan(&active)
	if err != nil {
		return false, fmt.Errorf("claim sqlite schedule ownership: %w", err)
	}
	return active, nil
}

func (s *ScheduleSQLiteOwner) ReleaseSchedule(context.Context, runtimepipeline.Schedule) error {
	return nil
}

func (s *ScheduleSQLiteOwner) ReleaseScheduleClaims(context.Context) error {
	return nil
}

func scheduleTaskIDFromPayload(payload []byte) string {
	if len(payload) == 0 {
		return ""
	}
	var decoded map[string]any
	if err := json.Unmarshal(payload, &decoded); err != nil || decoded == nil {
		return ""
	}
	raw, ok := decoded["__schedule_task_id"]
	if !ok || raw == nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprint(raw))
}

func sqliteNullUUID(raw string) any {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	return raw
}

func sqliteNullString(raw string) any {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	return raw
}

func jsonRawMessageValue(raw any) []byte {
	switch value := raw.(type) {
	case nil:
		return nil
	case []byte:
		return append([]byte(nil), value...)
	case string:
		return []byte(value)
	default:
		return []byte(fmt.Sprint(value))
	}
}

func sqliteNullTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value.UTC()
}
