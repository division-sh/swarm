package tools

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/failures"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
)

func (e *Executor) execSchedule(ctx context.Context, actor models.AgentConfig, input any) (any, error) {
	if e.genericSchedules == nil {
		return nil, failures.NewDetail("dependency_unavailable", "tool-executor", "schedule.create", map[string]any{"dependency": "generic_schedule_lifecycle"})
	}
	var in struct {
		ScheduleKey string `json:"schedule_key"`
		AgentID     string `json:"agent_id"`
		EventType   string `json:"event_type"`
		Mode        string `json:"mode"`
		Cron        string `json:"cron"`
		At          string `json:"at"`
		Delay       string `json:"delay"`
		Every       string `json:"every"`
		EntityID    string `json:"entity_id"`
		TaskID      string `json:"task_id"`
		Payload     any    `json:"payload"`
	}
	if err := decodeToolInput(input, &in); err != nil {
		return nil, err
	}
	if strings.TrimSpace(in.ScheduleKey) == "" {
		return nil, errors.New("schedule_key is required")
	}
	if strings.TrimSpace(in.AgentID) == "" {
		in.AgentID = actor.ID
	}
	if in.AgentID != actor.ID {
		return nil, failures.New(failures.ClassAuthorizationDenied, "agent_schedule_forbidden", "tool-executor", "schedule.authorize", map[string]any{"action": "schedule_create", "actor_id": actor.ID, "target_agent_id": in.AgentID})
	}
	entityID := strings.TrimSpace(in.EntityID)
	in.EntityID = entityID
	if entityID == "" {
		entityID = actor.EffectiveEntityID()
	}
	actorEntityID := actor.EffectiveEntityID()
	if entityID != "" && actorEntityID != "" && entityID != actorEntityID {
		return nil, failures.New(failures.ClassAuthorizationDenied, "cross_entity_schedule_forbidden", "tool-executor", "schedule.authorize", map[string]any{"action": "schedule_create", "actor_id": actor.ID, "entity_id": entityID})
	}

	payloadInput := in.Payload
	if payloadInput == nil {
		payloadInput = map[string]any{}
	}
	payload, err := canonicaljson.FromGo(payloadInput)
	if err != nil {
		return nil, fmt.Errorf("admit schedule payload: %w", err)
	}
	executionSource, err := runtimepinrouting.AdmitAgentExecutionRoutingSource(e.workflowSource, actor, entityID)
	if err != nil {
		return nil, fmt.Errorf("admit schedule owner source: %w", err)
	}
	routingSource := executionSource
	if executionSource.Kind() != events.RoutingSourceRoot {
		routingSource, err = events.NewFlowOwnedControlRoutingSource(executionSource.Route())
		if err != nil {
			return nil, fmt.Errorf("admit schedule control source: %w", err)
		}
	}
	flowID := ""
	if routingSource.Kind() == events.RoutingSourceFlowOwnedControl {
		flowID = routingSource.Route().FlowID
	}
	admittedEvent, err := runtimepinrouting.AdmitRuntimeControlSourceEvent(e.workflowSource, flowID, events.EventType(strings.TrimSpace(in.EventType)), routingSource)
	if err != nil {
		return nil, fmt.Errorf("admit schedule event identity: %w", err)
	}

	var due runtimegenericschedule.DueBasis
	switch strings.TrimSpace(in.Mode) {
	case "absolute":
		if strings.TrimSpace(in.Delay) != "" || strings.TrimSpace(in.Cron) != "" || strings.TrimSpace(in.Every) != "" {
			return nil, errors.New("absolute schedule requires only at")
		}
		at, err := time.Parse(time.RFC3339, strings.TrimSpace(in.At))
		if err != nil {
			return nil, fmt.Errorf("absolute schedule requires RFC3339 at: %w", err)
		}
		due = runtimegenericschedule.AbsoluteDue(at)
	case "delay":
		if strings.TrimSpace(in.At) != "" || strings.TrimSpace(in.Cron) != "" || strings.TrimSpace(in.Every) != "" {
			return nil, errors.New("delay schedule requires only delay")
		}
		delay, err := time.ParseDuration(strings.TrimSpace(in.Delay))
		if err != nil {
			return nil, fmt.Errorf("invalid schedule delay: %w", err)
		}
		due = runtimegenericschedule.DelayDue(delay)
	case "cron":
		if strings.TrimSpace(in.At) != "" || strings.TrimSpace(in.Delay) != "" || strings.TrimSpace(in.Every) != "" {
			return nil, errors.New("cron schedule requires only cron")
		}
		cron := strings.TrimSpace(in.Cron)
		if strings.HasPrefix(cron, "@every") {
			return nil, errors.New("cron schedule cannot use @every; use mode every")
		}
		due = runtimegenericschedule.CronDue(cron)
	case "every":
		if strings.TrimSpace(in.At) != "" || strings.TrimSpace(in.Delay) != "" || strings.TrimSpace(in.Cron) != "" {
			return nil, errors.New("every schedule requires only every")
		}
		interval, err := time.ParseDuration(strings.TrimSpace(in.Every))
		if err != nil {
			return nil, fmt.Errorf("invalid schedule every interval: %w", err)
		}
		due = runtimegenericschedule.EveryDue(interval)
	default:
		return nil, fmt.Errorf("unsupported schedule mode %q; use absolute, delay, cron, or every", in.Mode)
	}
	executionMode, ok := runtimeeffects.ExecutionModeFromContext(ctx)
	if !ok || !executionMode.Valid() {
		return nil, errors.New("schedule requires exact causal execution mode")
	}
	command := runtimegenericschedule.AdmissionCommand{
		ScheduleKey:   in.ScheduleKey,
		RunID:         runtimecorrelation.RunIDFromContext(ctx),
		OwnerID:       in.AgentID,
		OwnerKind:     runtimegenericschedule.OwnerAgent,
		AgentIdentity: actor.Identity,
		EventType:     string(admittedEvent),
		EntityID:      entityID,
		FlowInstance:  actor.CanonicalFlowPath(),
		TaskID:        in.TaskID,
		Payload:       payload,
		RoutingSource: routingSource,
		ExecutionMode: executionMode,
		Due:           due,
	}
	result, err := e.genericSchedules.Admit(ctx, command)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"status":        string(result.Activation.Status),
		"outcome":       string(result.Outcome),
		"activation_id": result.Activation.ID,
		"schedule_key":  result.Activation.Command.ScheduleKey,
		"due_at":        result.Activation.CurrentDueAt,
	}, nil
}
