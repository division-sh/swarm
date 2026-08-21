package tools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeauthority "github.com/division-sh/swarm/internal/runtime/authority"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/failures"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/google/uuid"
)

func (e *Executor) execAgentMessage(ctx context.Context, actor models.AgentConfig, input any) (any, error) {
	if e.bus == nil {
		return nil, failures.NewDetail("dependency_unavailable", "tool-executor", "agent_message.publish", map[string]any{"dependency": "event_bus"})
	}
	var in struct {
		TargetAgentID  string   `json:"target_agent_id"`
		TargetAgentIDs []string `json:"target_agent_ids"`
		ToAgentID      string   `json:"to_agent_id"`
		ToAgentIDs     []string `json:"to_agent_ids"`
		FlowInstance   string   `json:"flow_instance"`
		EventType      string   `json:"event_type"`
		SourceAgent    string   `json:"source_agent"`
		EntityID       string   `json:"entity_id"`
		TaskID         string   `json:"task_id"`
		Message        string   `json:"message"`
		Payload        any      `json:"payload"`
	}
	if err := decodeToolInput(input, &in); err != nil {
		return nil, err
	}
	if strings.TrimSpace(in.EventType) == "" {
		in.EventType = "agent.message"
	}
	if strings.TrimSpace(in.SourceAgent) == "" {
		in.SourceAgent = actor.ID
	}

	targets := make([]string, 0, 4)
	for _, v := range []string{in.TargetAgentID, in.ToAgentID} {
		if tv := strings.TrimSpace(v); tv != "" {
			targets = append(targets, tv)
		}
	}
	for _, v := range append(in.TargetAgentIDs, in.ToAgentIDs...) {
		if tv := strings.TrimSpace(v); tv != "" {
			targets = append(targets, tv)
		}
	}
	targets = uniqueNonEmptyStrings(targets)
	if len(targets) == 0 {
		return nil, errors.New("target_agent_id is required")
	}

	manager := e.getManager()
	if manager == nil {
		return nil, errors.New("agent manager is not configured")
	}
	targetEntity := strings.TrimSpace(in.EntityID)
	in.EntityID = targetEntity
	targetRoutes := make([]events.DeliveryRoute, 0, len(targets))
	for _, targetID := range targets {
		targetCfg, err := manager.ResolveAgentConfig(targetID, in.FlowInstance)
		if err != nil {
			return nil, fmt.Errorf("resolve target agent %s: %w", targetID, err)
		}
		targetIdentity, err := targetCfg.ConcreteIdentity()
		if err != nil {
			return nil, fmt.Errorf("resolve concrete target agent %s identity: %w", targetID, err)
		}
		targetCfgEntityID := targetCfg.EffectiveEntityID()
		if targetEntity == "" {
			targetEntity = targetCfgEntityID
		}
		if err := authorizeAgentMessage(e.authority, actor, targetCfg, manager); err != nil {
			return nil, fmt.Errorf("agent_message target %s: %w", targetID, err)
		}
		targetRoutes = append(targetRoutes, events.DeliveryRoute{
			Recipient:     events.MustAgentDeliveryRecipient(targetIdentity.AgentID()),
			AgentIdentity: targetIdentity,
		})
	}

	wirePayload, err := json.Marshal(map[string]any{
		"from_agent_id": actor.ID,
		"from_role":     actor.Role,
		"to_agent_ids":  targets,
		"message":       strings.TrimSpace(in.Message),
		"data":          in.Payload,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal payload: %w", err)
	}
	if len(wirePayload) == 0 || string(wirePayload) == "null" {
		wirePayload = []byte("{}")
	}
	executionMode, ok := runtimeeffects.ExecutionModeFromContext(ctx)
	if !ok {
		return nil, fmt.Errorf("agent_message requires typed causal execution mode")
	}
	inbound, ok := runtimecorrelation.InboundEventFromContext(ctx)
	if !ok {
		return nil, fmt.Errorf("agent_message requires typed inbound event lineage")
	}
	lineage := events.LineageFromEvent(inbound)
	lineage.TaskID = in.TaskID
	lineage.ExecutionMode = executionMode
	sourceEntity := actor.EffectiveEntityID()
	routingSource, err := runtimepinrouting.AdmitAgentExecutionRoutingSource(e.workflowSource, actor, sourceEntity)
	if err != nil {
		return nil, err
	}
	envelope := events.EnvelopeForSourceRoute(events.EventEnvelope{EntityID: targetEntity}, routingSource.Route())
	evt, err := events.NewChildEvent(events.ChildEventInput{Facts: events.EventFacts{
		ID: uuid.NewString(), Type: events.EventType(in.EventType), Producer: events.ProducerClaim{Type: events.EventProducerAgent, ID: actor.ID},
		TaskID: in.TaskID, Payload: wirePayload, Envelope: envelope, RoutingSource: routingSource,
		CreatedAt: time.Now(), ExecutionMode: executionMode,
	}, Lineage: lineage})
	if err != nil {
		return nil, err
	}
	if err := e.bus.PublishDirectRoutes(ctx, evt, targetRoutes); err != nil {
		return nil, err
	}
	return map[string]any{"event_id": evt.ID(), "status": "sent", "targets": targets}, nil
}

func authorizeAgentMessage(provider runtimeauthority.Provider, actor, target models.AgentConfig, manager Manager) error {
	if strings.TrimSpace(actor.ID) == "" || strings.TrimSpace(target.ID) == "" {
		return failures.NewDetail("invalid_tool_input", "tool-executor", "agent_message.authorize", map[string]any{"field": "agent_id"})
	}
	same, err := runtimeauthority.SameAgent(actor, target)
	if err != nil {
		return failures.NewDetail("invalid_tool_input", "tool-executor", "agent_message.authorize", map[string]any{"field": "agent_identity", "reason": err.Error()})
	}
	if same {
		return nil
	}
	if hasRoleMessageAuthority(provider, actor, target) {
		return nil
	}
	return failures.New(failures.ClassAuthorizationDenied, "agent_message_forbidden", "tool-executor", "agent_message.authorize", map[string]any{"action": "agent_message", "actor_id": actor.ID, "target_agent_id": target.ID})
}

func hasRoleMessageAuthority(provider runtimeauthority.Provider, actor, target models.AgentConfig) bool {
	return runtimeauthority.ProviderOrNoop(provider).HasMessageAuthority(actor, target)
}

func uniqueNonEmptyStrings(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, raw := range in {
		v := strings.TrimSpace(raw)
		if v == "" {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
}

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
