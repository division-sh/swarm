package tools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeauthority "github.com/division-sh/swarm/internal/runtime/authority"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
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
	actorIdentity, err := actor.ConcreteIdentity()
	if err != nil {
		return nil, err
	}
	routingSource, err := runtimepinrouting.AdmitAgentExecutionRoutingSource(e.workflowSource, actorIdentity, sourceEntity)
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
	executionSource, err := runtimepinrouting.AdmitAgentExecutionRoutingSource(e.workflowSource, actor.Identity, entityID)
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

func (e *Executor) execConfigureRouting(ctx context.Context, actor models.AgentConfig, input any) (any, error) {
	_, _, _ = ctx, actor, input
	return nil, errors.New("configure_routing is not yet implemented")
}

func (e *Executor) execAgentHire(ctx context.Context, actor models.AgentConfig, input any) (any, error) {
	if err := authorizeAgentMutationExecutionMode(ctx, actor, "agent_hire"); err != nil {
		return nil, err
	}
	manager := e.getManager()
	if manager == nil {
		return nil, errors.New("agent manager is not configured")
	}
	in, err := decodeAgentMutationInput("agent_hire", input)
	if err != nil {
		return nil, err
	}
	if in.Config.ID == "" {
		return nil, errors.New("config.id is required")
	}
	in.EntityID = coalesce(in.EntityID, actor.EffectiveEntityID())
	if in.Config.EntityID == "" {
		in.Config.EntityID = in.EntityID
	}
	in.Config.NormalizeEntityID()
	if in.Config.FlowID == "" {
		in.Config.FlowID = strings.TrimSpace(actor.FlowID)
	}
	if in.Config.FlowPath == "" {
		in.Config.FlowPath = actor.CanonicalFlowPath()
	}
	if strings.TrimSpace(string(in.Config.Memory.Source)) == "" {
		in.Config.Memory, _ = agentmemory.NewPlan(false, agentmemory.SourcePlatformDefault)
	}
	if in.Config.Memory.Enabled && in.Config.CanonicalFlowPath() == "" {
		return nil, fmt.Errorf("memory: true requires a flow-instance owner")
	}
	if err := authorizeManage(e.authority, actor, in.Config, manager); err != nil {
		return nil, err
	}
	if err := authorizeDelegableAgentConfig(actor, models.AgentConfig{}, in.Config, e.authority, e.emitRegistry); err != nil {
		return nil, err
	}
	if err := preflightManagedAgentParent(manager, actor, in.Config); err != nil {
		return nil, fmt.Errorf("validate hired agent authority: %w", err)
	}
	if err := manager.SpawnAgentForEntity(in.Config.EffectiveEntityID(), in.Config); err != nil {
		return nil, err
	}
	cfg, err := manager.ResolveAgentConfig(in.Config.ID, in.Config.CanonicalFlowPath())
	if err != nil {
		return nil, fmt.Errorf("resolve hired agent %s: %w", in.Config.ID, err)
	}
	if err := syncManagedAgentAuthority(e.authority, manager, actor, cfg); err != nil {
		syncErr := fmt.Errorf("record hired agent authority: %w", err)
		if _, teardownErr := manager.TeardownAgentTarget(cfg.ID, cfg.CanonicalFlowPath(), &cfg); teardownErr != nil {
			return nil, errors.Join(syncErr, fmt.Errorf("rollback hired agent after authority failure: %w", teardownErr))
		}
		return nil, syncErr
	}
	return map[string]any{"status": "hired", "agent_id": in.Config.ID}, nil
}

func (e *Executor) execAgentFire(ctx context.Context, actor models.AgentConfig, input any) (any, error) {
	if err := authorizeAgentMutationExecutionMode(ctx, actor, "agent_fire"); err != nil {
		return nil, err
	}
	manager := e.getManager()
	if manager == nil {
		return nil, errors.New("agent manager is not configured")
	}
	var in struct {
		AgentID      string `json:"agent_id"`
		FlowInstance string `json:"flow_instance"`
	}
	if err := decodeToolInput(input, &in); err != nil {
		return nil, err
	}
	if strings.TrimSpace(in.AgentID) == "" {
		return nil, errors.New("agent_id is required")
	}
	targetCfg, err := manager.ResolveAgentConfig(in.AgentID, in.FlowInstance)
	if err != nil {
		return nil, fmt.Errorf("resolve target agent %s: %w", in.AgentID, err)
	}
	if err := authorizeManage(e.authority, actor, targetCfg, manager); err != nil {
		return nil, err
	}
	child, err := targetCfg.ConcreteIdentity()
	if err != nil {
		return nil, fmt.Errorf("resolve fired agent identity: %w", err)
	}
	if _, err := currentManagedAgentAuthorityPlan(e.authority, child); err != nil {
		return nil, fmt.Errorf("snapshot fired agent authority: %w", err)
	}
	if _, err := manager.TeardownAgentTarget(in.AgentID, in.FlowInstance, &targetCfg); err != nil {
		return nil, err
	}
	if err := runtimeauthority.RemoveManagedAgent(e.authority, child); err != nil {
		return nil, fmt.Errorf("project committed fired agent authority: %w", err)
	}
	return map[string]any{"status": "fired", "agent_id": in.AgentID}, nil
}

func (e *Executor) execAgentReconfigure(ctx context.Context, actor models.AgentConfig, input any) (any, error) {
	if err := authorizeAgentMutationExecutionMode(ctx, actor, "agent_reconfigure"); err != nil {
		return nil, err
	}
	manager := e.getManager()
	if manager == nil {
		return nil, errors.New("agent manager is not configured")
	}
	in, err := decodeAgentMutationInput("agent_reconfigure", input)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(in.AgentID) == "" {
		return nil, errors.New("agent_id is required")
	}
	targetCfg, err := manager.ResolveAgentConfig(in.AgentID, in.FlowInstance)
	if err != nil {
		return nil, fmt.Errorf("resolve target agent %s: %w", in.AgentID, err)
	}
	if err := authorizeManage(e.authority, actor, targetCfg, manager); err != nil {
		return nil, err
	}
	updatedCfg := models.MergeAgentConfig(targetCfg, in.Config)
	if updatedCfg.Memory.Enabled && updatedCfg.CanonicalFlowPath() == "" {
		return nil, fmt.Errorf("memory: true requires a flow-instance owner")
	}
	if err := authorizeDelegableAgentConfig(actor, targetCfg, updatedCfg, e.authority, e.emitRegistry); err != nil {
		return nil, err
	}
	authorityPlan, err := managedAgentAuthorityPlanForCandidate(manager, actor, updatedCfg)
	if err != nil {
		return nil, fmt.Errorf("validate reconfigured agent authority: %w", err)
	}
	if authorityPlan.hasParent {
		if err := runtimeauthority.ValidateManagedAgent(e.authority, authorityPlan.child, authorityPlan.parent); err != nil {
			return nil, fmt.Errorf("validate reconfigured agent authority: %w", err)
		}
	}
	result, err := manager.ReconfigureAgentTarget(in.AgentID, in.FlowInstance, in.Config, &targetCfg)
	if err != nil {
		return nil, err
	}
	if result.CurrentConfig.ID == "" {
		return nil, errors.New("reconfigured agent mutation returned no committed configuration")
	}
	committedPlan, err := managedAgentAuthorityPlanForCandidate(manager, actor, result.CurrentConfig)
	if err != nil {
		return nil, fmt.Errorf("validate committed agent authority: %w", err)
	}
	if committedPlan != authorityPlan {
		return nil, errors.New("committed agent authority differs from the prevalidated transition")
	}
	if err := applyManagedAgentAuthority(e.authority, committedPlan); err != nil {
		return nil, fmt.Errorf("project committed agent authority: %w", err)
	}
	return map[string]any{"status": "reconfigured", "agent_id": in.AgentID}, nil
}

func authorizeAgentMutationExecutionMode(ctx context.Context, actor models.AgentConfig, action string) error {
	action = strings.TrimSpace(action)
	causalMode, hasCausalMode := runtimeeffects.ExecutionModeFromContext(ctx)
	if actor.ExecutionMode != runtimeeffects.ExecutionModeMock && (!hasCausalMode || causalMode != runtimeeffects.ExecutionModeMock) {
		return nil
	}
	return failures.New(
		failures.ClassAuthorizationDenied,
		"mock_"+action+"_forbidden",
		"tool-executor",
		action+".authorize_execution_mode",
		map[string]any{
			"action":                action,
			"actor_id":              actor.ID,
			"actor_execution_mode":  actor.ExecutionMode,
			"causal_execution_mode": causalMode,
		},
	)
}

type managedAgentAuthorityPlan struct {
	child     agentidentity.Identity
	parent    agentidentity.Identity
	hasParent bool
}

func applyManagedAgentAuthority(provider runtimeauthority.Provider, plan managedAgentAuthorityPlan) error {
	if !plan.hasParent {
		return runtimeauthority.RemoveManagedAgent(provider, plan.child)
	}
	return runtimeauthority.UpsertManagedAgent(provider, plan.child, plan.parent)
}

func currentManagedAgentAuthorityPlan(
	provider runtimeauthority.Provider,
	child agentidentity.Identity,
) (managedAgentAuthorityPlan, error) {
	child = child.Normalize()
	if err := child.Validate(); err != nil {
		return managedAgentAuthorityPlan{}, fmt.Errorf("managed agent identity: %w", err)
	}
	parent, found, err := runtimeauthority.ManagedAgentParent(provider, child)
	if err != nil {
		return managedAgentAuthorityPlan{}, err
	}
	return managedAgentAuthorityPlan{child: child, parent: parent, hasParent: found}, nil
}

func syncManagedAgentAuthority(provider runtimeauthority.Provider, manager Manager, actor, target models.AgentConfig) error {
	plan, err := managedAgentAuthorityPlanForCandidate(manager, actor, target)
	if err != nil {
		return err
	}
	return applyManagedAgentAuthority(provider, plan)
}

func managedAgentAuthorityPlanForCandidate(manager Manager, actor, target models.AgentConfig) (managedAgentAuthorityPlan, error) {
	targetIdentity, err := target.ConcreteIdentity()
	if err != nil {
		return managedAgentAuthorityPlan{}, fmt.Errorf("resolve concrete managed agent %s identity: %w", target.ID, err)
	}
	plan := managedAgentAuthorityPlan{child: targetIdentity}
	parentRef := strings.TrimSpace(target.ParentAgent)
	if parentRef == "" {
		parentRef = strings.TrimSpace(target.ManagerFallback)
	}
	if parentRef == "" {
		return plan, nil
	}
	if targetIdentity.MatchesAgentID(parentRef) {
		return managedAgentAuthorityPlan{}, fmt.Errorf("managed agent identity cannot be its own parent")
	}

	parent := actor
	if !managedParentReferenceMatches(parentRef, parent) || !sameConcreteFlowRoute(parent, target) {
		parent, err = manager.ResolveAgentConfig(parentRef, target.CanonicalFlowPath())
		if err != nil {
			return managedAgentAuthorityPlan{}, fmt.Errorf("resolve managed parent %s: %w", parentRef, err)
		}
	}
	parentIdentity, err := parent.ConcreteIdentity()
	if err != nil {
		return managedAgentAuthorityPlan{}, fmt.Errorf("resolve concrete managed parent %s identity: %w", parentRef, err)
	}
	same, err := agentidentity.Equal(targetIdentity, parentIdentity)
	if err != nil {
		return managedAgentAuthorityPlan{}, fmt.Errorf("compare managed agent and parent identity: %w", err)
	}
	if same {
		return managedAgentAuthorityPlan{}, fmt.Errorf("managed agent identity cannot be its own parent")
	}
	if parentIdentity.Route != targetIdentity.Route {
		return managedAgentAuthorityPlan{}, fmt.Errorf("managed parent %s is not in target flow route %s", parentRef, target.CanonicalFlowPath())
	}
	plan.parent = parentIdentity
	plan.hasParent = true
	return plan, nil
}

func preflightManagedAgentParent(manager Manager, actor, target models.AgentConfig) error {
	parentRef := strings.TrimSpace(target.ParentAgent)
	if parentRef == "" {
		parentRef = strings.TrimSpace(target.ManagerFallback)
	}
	if parentRef == "" {
		return nil
	}
	if parentRef == strings.TrimSpace(target.ID) {
		return fmt.Errorf("managed agent identity cannot be its own parent")
	}
	parent := actor
	if !managedParentReferenceMatches(parentRef, parent) || parent.CanonicalFlowPath() != target.CanonicalFlowPath() {
		var err error
		parent, err = manager.ResolveAgentConfig(parentRef, target.CanonicalFlowPath())
		if err != nil {
			return fmt.Errorf("resolve managed parent %s: %w", parentRef, err)
		}
	}
	parentIdentity, err := parent.ConcreteIdentity()
	if err != nil {
		return fmt.Errorf("resolve concrete managed parent %s identity: %w", parentRef, err)
	}
	if parentIdentity.FlowInstance() != target.CanonicalFlowPath() {
		return fmt.Errorf("managed parent %s is not in target flow route %s", parentRef, target.CanonicalFlowPath())
	}
	return nil
}

func managedParentReferenceMatches(parentRef string, parent models.AgentConfig) bool {
	return strings.TrimSpace(parentRef) != "" && strings.TrimSpace(parentRef) == strings.TrimSpace(parent.ID)
}

func sameConcreteFlowRoute(left, right models.AgentConfig) bool {
	leftIdentity, leftErr := left.ConcreteIdentity()
	rightIdentity, rightErr := right.ConcreteIdentity()
	return leftErr == nil && rightErr == nil && leftIdentity.Route == rightIdentity.Route
}

type agentMutationInput struct {
	AgentID      string
	FlowInstance string
	EntityID     string
	Config       models.AgentConfig
}

func decodeAgentMutationInput(toolName string, input any) (agentMutationInput, error) {
	normalized := canonicalRuntimeToolInput(toolName, input)
	var payload map[string]any
	if err := decodeToolInput(normalized, &payload); err != nil {
		return agentMutationInput{}, err
	}
	if payload == nil {
		payload = map[string]any{}
	}
	if err := rejectAgentMemoryForgery(payload, "input", true); err != nil {
		return agentMutationInput{}, err
	}

	configMap, err := agentMutationConfigMap(payload["config"])
	if err != nil {
		return agentMutationInput{}, err
	}
	topMemory, topPresent, err := optionalBool(payload, "memory")
	if err != nil {
		return agentMutationInput{}, err
	}
	configMemory, configPresent, err := optionalBool(configMap, "memory")
	if err != nil {
		return agentMutationInput{}, err
	}
	switch {
	case topPresent && configPresent && topMemory != configMemory:
		return agentMutationInput{}, fmt.Errorf("memory mismatch between input.memory and input.config.memory")
	case configPresent:
		topMemory, topPresent = configMemory, true
	}
	delete(payload, "memory")
	delete(configMap, "memory")
	payload["config"] = configMap

	var decoded struct {
		AgentID      string             `json:"agent_id"`
		FlowInstance string             `json:"flow_instance"`
		EntityID     string             `json:"entity_id"`
		Config       models.AgentConfig `json:"config"`
	}
	if err := decodeToolInput(payload, &decoded); err != nil {
		return agentMutationInput{}, err
	}
	if topPresent {
		decoded.Config.Memory, _ = agentmemory.NewPlan(topMemory, agentmemory.SourceAuthored)
	}
	return agentMutationInput{
		AgentID:      decoded.AgentID,
		FlowInstance: decoded.FlowInstance,
		EntityID:     decoded.EntityID,
		Config:       decoded.Config,
	}, nil
}

func agentMutationConfigMap(raw any) (map[string]any, error) {
	if raw == nil {
		return map[string]any{}, nil
	}
	var config map[string]any
	if err := decodeToolInput(raw, &config); err != nil {
		return nil, fmt.Errorf("decode config: %w", err)
	}
	if config == nil {
		return map[string]any{}, nil
	}
	return config, nil
}

func optionalBool(values map[string]any, field string) (bool, bool, error) {
	raw, ok := values[field]
	if !ok {
		return false, false, nil
	}
	value, ok := raw.(bool)
	if !ok {
		return false, true, fmt.Errorf("%s must be a boolean", field)
	}
	return value, true, nil
}

func rejectAgentMemoryForgery(value any, path string, allowMemory bool) error {
	switch typed := value.(type) {
	case map[string]any:
		for key, item := range typed {
			field := strings.TrimSpace(key)
			if field == "" {
				continue
			}
			fieldPath := path + "." + field
			switch field {
			case "mode", "conversation_mode", "session_scope", "session_scope_authority":
				return fmt.Errorf("%s is retired; use memory", fieldPath)
			case "flow_instance":
				if path == "input" {
					continue
				}
				return fmt.Errorf("%s is runtime-owned and cannot be supplied by agent mutation callers", fieldPath)
			case "run_id", "flow_id", "flow_path", "scope", "scope_key", "authority", "memory_plan":
				return fmt.Errorf("%s is runtime-owned and cannot be supplied by agent mutation callers", fieldPath)
			case "memory":
				if !allowMemory {
					return fmt.Errorf("%s is only supported at input.memory or input.config.memory", fieldPath)
				}
			}
			childAllowMemory := false
			if path == "input" && field == "config" {
				childAllowMemory = true
			}
			if err := rejectAgentMemoryForgery(item, fieldPath, childAllowMemory); err != nil {
				return err
			}
		}
	case []any:
		for i, item := range typed {
			if err := rejectAgentMemoryForgery(item, fmt.Sprintf("%s[%d]", path, i), false); err != nil {
				return err
			}
		}
	}
	return nil
}
