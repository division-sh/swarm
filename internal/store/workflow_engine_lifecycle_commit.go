package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	privaterunforkrevision "github.com/division-sh/swarm/internal/store/internal/runforkrevision"
	"github.com/google/uuid"
)

func commitWorkflowEngineLifecycle(
	ctx context.Context,
	tx *sql.Tx,
	story runtimeauthoractivity.Mutation,
	postgres bool,
	plan runtimepipeline.WorkflowLifecycleMutationPlan,
) (runtimepipeline.CommittedWorkflowLifecycleMutation, error) {
	result := runtimepipeline.CommittedWorkflowLifecycleMutation{}
	for index, mutation := range plan.Timers {
		ref, changed, err := commitWorkflowEngineTimerMutation(ctx, tx, postgres, mutation)
		if err != nil {
			return runtimepipeline.CommittedWorkflowLifecycleMutation{}, fmt.Errorf("commit workflow engine timer mutation %d: %w", index, err)
		}
		switch mutation.Kind {
		case runtimepipeline.WorkflowTimerMutationInsert:
			result.Wakeups = append(result.Wakeups, ref)
		case runtimepipeline.WorkflowTimerMutationCancel:
			if changed {
				result.Cancellations = append(result.Cancellations, ref)
			}
		}
	}
	for index, mutation := range plan.Schedules {
		if err := commitWorkflowEngineScheduleMutation(ctx, tx, postgres, mutation); err != nil {
			return runtimepipeline.CommittedWorkflowLifecycleMutation{}, fmt.Errorf("commit workflow engine schedule mutation %d: %w", index, err)
		}
		switch mutation.Kind {
		case runtimepipeline.WorkflowScheduleMutationUpsert:
			result.ScheduleUpserts = append(result.ScheduleUpserts, mutation.Schedule)
		case runtimepipeline.WorkflowScheduleMutationCancel:
			result.ScheduleCancellations = append(result.ScheduleCancellations, mutation.Schedule)
		}
	}
	for index, mutation := range plan.GateCards {
		if err := commitWorkflowEngineGateCardMutation(ctx, tx, story, postgres, mutation); err != nil {
			return runtimepipeline.CommittedWorkflowLifecycleMutation{}, fmt.Errorf("commit workflow engine gate card mutation %d: %w", index, err)
		}
	}
	return result, result.Validate()
}

func commitWorkflowEngineScheduleMutation(ctx context.Context, tx *sql.Tx, postgres bool, mutation runtimepipeline.WorkflowScheduleMutation) error {
	sc := mutation.Schedule
	if strings.TrimSpace(sc.Mode) == "" {
		sc.Mode = "once"
	}
	sc.NormalizeRunID()
	sc.NormalizeEntityID()
	sc.NormalizeFlowInstance()
	if sc.Context.Empty() {
		sc.Context = events.DeliveryContextFromContext(ctx)
	}
	sc.NormalizeDeliveryContext()
	if err := sc.NormalizeOwner(); err != nil {
		return err
	}
	identity, err := scheduleAgentIdentityFields(sc)
	if err != nil {
		return err
	}
	cancel := func() error {
		if postgres {
			_, err := tx.ExecContext(ctx, fmt.Sprintf(`
				UPDATE timers SET status = 'cancelled'
				WHERE run_id IS NOT DISTINCT FROM NULLIF($1,'')::uuid
				  AND owner_agent = $2 AND owner_kind = $7
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
				sc.OwnerKind, identity.NameOwner, identity.NameSource, identity.RoutePresence, identity.FlowScopeKey, identity.FlowInstanceID)
			return err
		}
		_, err := tx.ExecContext(ctx, `
			UPDATE timers SET status = 'cancelled'
			WHERE COALESCE(run_id, '') = COALESCE(?, '')
			  AND owner_agent = ? AND owner_kind = ?
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
		`, sqliteNullUUID(sc.RunID), sc.AgentID, sc.OwnerKind, identity.NameOwner, identity.NameSource,
			identity.RoutePresence, identity.FlowScopeKey, identity.FlowInstanceID, sc.EventType,
			sqliteNullUUID(sc.EntityID), sqliteNullString(sc.FlowInstance), strings.TrimSpace(sc.TaskID))
		return err
	}
	if err := cancel(); err != nil {
		return err
	}
	if mutation.Kind == runtimepipeline.WorkflowScheduleMutationCancel {
		return nil
	}
	if mutation.Kind != runtimepipeline.WorkflowScheduleMutationUpsert {
		return fmt.Errorf("workflow schedule mutation kind %q is unsupported", mutation.Kind)
	}
	fireAt := sc.At.UTC()
	if fireAt.IsZero() {
		return fmt.Errorf("workflow schedule requires an exact fire time")
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
	if postgres {
		_, err = tx.ExecContext(ctx, `
			INSERT INTO timers (
				run_id, timer_name, entity_id, flow_instance, fire_event, fire_payload, routing_source,
				fire_at, recurring, recurrence_cron, recurrence_interval,
				owner_node, owner_agent, owner_kind, agent_name_owner, agent_name_source,
				agent_route_presence, agent_flow_scope_key, agent_flow_instance_id,
				reply_context_id, task_type, status
			) VALUES (
				NULLIF($1,'')::uuid, $2, NULLIF($3,'')::uuid, NULLIF($4,''), $5, $6::jsonb, $7::jsonb,
				$8, $9, NULLIF($10,''), NULL,
				NULL, $11, $12, NULLIF($13, ''), NULLIF($14, ''),
				NULLIF($15, ''), NULLIF($16, ''), NULLIF($17, ''),
				NULLIF($18, ''), $19, 'active'
			)
		`, sc.RunID, timerName, sc.EntityID, sc.FlowInstance, sc.EventType, string(persistedSchedulePayload(sc)), string(routingSource), fireAt,
			recurring, sc.Cron, sc.AgentID, sc.OwnerKind, identity.NameOwner, identity.NameSource, identity.RoutePresence,
			identity.FlowScopeKey, identity.FlowInstanceID, sc.Context.ReplyContextID(), taskType)
	} else {
		_, err = tx.ExecContext(ctx, `
			INSERT INTO timers (
				timer_id, run_id, timer_name, entity_id, flow_instance, fire_event, fire_payload, routing_source,
				fire_at, recurring, recurrence_cron, owner_agent, owner_kind, agent_name_owner,
				agent_name_source, agent_route_presence, agent_flow_scope_key, agent_flow_instance_id,
				reply_context_id, task_type, status, created_at
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULLIF(?, ''), NULLIF(?, ''),
			          NULLIF(?, ''), NULLIF(?, ''), NULLIF(?, ''), NULLIF(?, ''), ?, 'active', ?)
		`, uuid.NewString(), sqliteNullUUID(sc.RunID), timerName, sqliteNullUUID(sc.EntityID), sqliteNullString(sc.FlowInstance),
			sc.EventType, string(persistedSchedulePayload(sc)), string(routingSource), fireAt, recurring, sqliteNullString(sc.Cron), sc.AgentID, sc.OwnerKind,
			identity.NameOwner, identity.NameSource, identity.RoutePresence, identity.FlowScopeKey, identity.FlowInstanceID,
			sc.Context.ReplyContextID(), taskType, time.Now().UTC())
	}
	if err != nil {
		return fmt.Errorf("insert workflow schedule: %w", err)
	}
	if postgres && strings.TrimSpace(sc.RunID) != "" {
		if _, err := privaterunforkrevision.Capture(ctx, tx, sc.RunID, privaterunforkrevision.FamilyTimers); err != nil {
			return err
		}
	}
	return nil
}

func commitWorkflowEngineGateCardMutation(ctx context.Context, tx *sql.Tx, story runtimeauthoractivity.Mutation, postgres bool, mutation runtimepipeline.WorkflowGateCardMutation) error {
	switch mutation.Kind {
	case runtimepipeline.WorkflowGateCardMutationCreate:
		return insertDecisionCardWithStory(ctx, story, tx, mutation.Card, postgres)
	case runtimepipeline.WorkflowGateCardMutationSupersede:
		persisted, err := loadDecisionCardByActivation(ctx, tx, mutation.Card.RunID, mutation.EntityID, mutation.ActivationID, postgres)
		if err != nil {
			return err
		}
		if !sameWorkflowEngineGateCard(persisted, mutation.Card) {
			return fmt.Errorf("workflow gate card changed before supersession")
		}
		changed, err := supersedeDecisionCardsForStageWithStory(ctx, story, tx, mutation.Card.RunID, mutation.EntityID, mutation.ActivationID, mutation.Reason, mutation.OccurredAt, postgres)
		if err != nil {
			return err
		}
		if !changed {
			return fmt.Errorf("workflow gate card supersession changed no card")
		}
		return nil
	default:
		return fmt.Errorf("workflow gate card mutation kind %q is unsupported", mutation.Kind)
	}
}

func sameWorkflowEngineGateCard(left, right decisioncard.Card) bool {
	return left.CardID == right.CardID && left.RunID == right.RunID && left.Status == right.Status &&
		left.CardContentHash == right.CardContentHash && left.EffectContentHash == right.EffectContentHash &&
		left.DecisionSchemaHash == right.DecisionSchemaHash && left.BundleHash == right.BundleHash &&
		left.Anchor.Kind() == right.Anchor.Kind() && left.Anchor.SemanticValue().Equal(right.Anchor.SemanticValue())
}
