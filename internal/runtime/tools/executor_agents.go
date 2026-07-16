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
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/failures"
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
	for _, targetID := range targets {
		targetCfg, ok := manager.GetAgentConfig(targetID)
		if !ok {
			return nil, fmt.Errorf("target agent not found: %s", targetID)
		}
		targetCfgEntityID := targetCfg.EffectiveEntityID()
		if targetEntity == "" {
			targetEntity = targetCfgEntityID
		}
		if err := authorizeAgentMessage(e.authority, actor, targetCfg, manager); err != nil {
			return nil, fmt.Errorf("agent_message target %s: %w", targetID, err)
		}
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
	evt := events.NewChildEventWithLineage(
		uuid.NewString(),
		events.EventType(in.EventType),
		events.AgentProducer(in.SourceAgent),
		in.TaskID,
		wirePayload,
		0,
		events.EventLineage{
			RunID:         runtimecorrelation.RunIDFromContext(ctx),
			ExecutionMode: executionMode,
		},
		events.EventEnvelope{EntityID: targetEntity},
		time.Now(),
	)
	if err := e.bus.PublishDirect(ctx, evt, targets); err != nil {
		return nil, err
	}
	return map[string]any{"event_id": evt.ID(), "status": "sent", "targets": targets}, nil
}

func authorizeAgentMessage(provider runtimeauthority.Provider, actor, target models.AgentConfig, manager Manager) error {
	if strings.TrimSpace(actor.ID) == "" || strings.TrimSpace(target.ID) == "" {
		return failures.NewDetail("invalid_tool_input", "tool-executor", "agent_message.authorize", map[string]any{"field": "agent_id"})
	}
	if strings.TrimSpace(actor.ID) == strings.TrimSpace(target.ID) {
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
	if e.scheduler == nil {
		return nil, failures.NewDetail("dependency_unavailable", "tool-executor", "schedule.create", map[string]any{"dependency": "scheduler"})
	}
	var in struct {
		AgentID   string `json:"agent_id"`
		EventType string `json:"event_type"`
		Mode      string `json:"mode"`
		Cron      string `json:"cron"`
		At        string `json:"at"`
		EntityID  string `json:"entity_id"`
		TaskID    string `json:"task_id"`
		Payload   any    `json:"payload"`
	}
	if err := decodeToolInput(input, &in); err != nil {
		return nil, err
	}
	if strings.TrimSpace(in.Mode) == "" {
		in.Mode = "once"
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

	var at time.Time
	if strings.TrimSpace(in.At) != "" {
		parsed, err := time.Parse(time.RFC3339, in.At)
		if err != nil {
			return nil, fmt.Errorf("invalid at value: %w", err)
		}
		at = parsed
	}

	payload, err := json.Marshal(in.Payload)
	if err != nil {
		return nil, fmt.Errorf("marshal payload: %w", err)
	}
	if len(payload) == 0 || string(payload) == "null" {
		payload = []byte("{}")
	}

	schedule := Schedule{
		RunID:        runtimecorrelation.RunIDFromContext(ctx),
		AgentID:      in.AgentID,
		EventType:    in.EventType,
		Mode:         in.Mode,
		Cron:         in.Cron,
		At:           at,
		EntityID:     entityID,
		FlowInstance: actor.CanonicalFlowPath(),
		TaskID:       in.TaskID,
		Payload:      payload,
	}
	if err := e.scheduler.Register(schedule); err != nil {
		return nil, err
	}
	if e.scheduleStore != nil {
		if err := e.scheduleStore.UpsertSchedule(ctx, schedule); err != nil {
			return nil, err
		}
	}

	return map[string]any{"status": "scheduled"}, nil
}

func (e *Executor) execConfigureRouting(ctx context.Context, actor models.AgentConfig, input any) (any, error) {
	_, _, _ = ctx, actor, input
	return nil, errors.New("configure_routing is not yet implemented")
}

func (e *Executor) execAgentHire(ctx context.Context, actor models.AgentConfig, input any) (any, error) {
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
	if err := e.ValidateNativeToolAdmission(ctx, in.Config); err != nil {
		return nil, err
	}
	if err := manager.SpawnAgentForEntity(in.Config.EffectiveEntityID(), in.Config); err != nil {
		return nil, err
	}
	if cfg, ok := manager.GetAgentConfig(in.Config.ID); ok {
		runtimeauthority.UpsertManagedAgent(e.authority, cfg)
	} else {
		runtimeauthority.UpsertManagedAgent(e.authority, in.Config)
	}
	return map[string]any{"status": "hired", "agent_id": in.Config.ID}, nil
}

func (e *Executor) execAgentFire(actor models.AgentConfig, input any) (any, error) {
	manager := e.getManager()
	if manager == nil {
		return nil, errors.New("agent manager is not configured")
	}
	var in struct {
		AgentID string `json:"agent_id"`
	}
	if err := decodeToolInput(input, &in); err != nil {
		return nil, err
	}
	if strings.TrimSpace(in.AgentID) == "" {
		return nil, errors.New("agent_id is required")
	}
	targetCfg, ok := manager.GetAgentConfig(in.AgentID)
	if !ok {
		return nil, fmt.Errorf("target agent not found: %s", in.AgentID)
	}
	if err := authorizeManage(e.authority, actor, targetCfg, manager); err != nil {
		return nil, err
	}
	if err := manager.TeardownAgent(in.AgentID); err != nil {
		return nil, err
	}
	runtimeauthority.RemoveManagedAgent(e.authority, in.AgentID)
	return map[string]any{"status": "fired", "agent_id": in.AgentID}, nil
}

func (e *Executor) execAgentReconfigure(ctx context.Context, actor models.AgentConfig, input any) (any, error) {
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
	targetCfg, ok := manager.GetAgentConfig(in.AgentID)
	if !ok {
		return nil, fmt.Errorf("target agent not found: %s", in.AgentID)
	}
	if err := authorizeManage(e.authority, actor, targetCfg, manager); err != nil {
		return nil, err
	}
	updatedCfg := mergeDelegablePrivilegeConfig(targetCfg, in.Config)
	if strings.TrimSpace(string(in.Config.Memory.Source)) != "" {
		updatedCfg.Memory = in.Config.Memory
	}
	if updatedCfg.Memory.Enabled && updatedCfg.CanonicalFlowPath() == "" {
		return nil, fmt.Errorf("memory: true requires a flow-instance owner")
	}
	if err := authorizeDelegableAgentConfig(actor, targetCfg, updatedCfg, e.authority, e.emitRegistry); err != nil {
		return nil, err
	}
	if err := e.ValidateNativeToolAdmission(ctx, updatedCfg); err != nil {
		return nil, err
	}
	if err := manager.ReconfigureAgent(in.AgentID, in.Config); err != nil {
		return nil, err
	}
	if cfg, ok := manager.GetAgentConfig(in.AgentID); ok {
		runtimeauthority.UpsertManagedAgent(e.authority, cfg)
	} else {
		in.Config.ID = in.AgentID
		runtimeauthority.UpsertManagedAgent(e.authority, in.Config)
	}
	return map[string]any{"status": "reconfigured", "agent_id": in.AgentID}, nil
}

type agentMutationInput struct {
	AgentID  string
	EntityID string
	Config   models.AgentConfig
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
		AgentID  string             `json:"agent_id"`
		EntityID string             `json:"entity_id"`
		Config   models.AgentConfig `json:"config"`
	}
	if err := decodeToolInput(payload, &decoded); err != nil {
		return agentMutationInput{}, err
	}
	if topPresent {
		decoded.Config.Memory, _ = agentmemory.NewPlan(topMemory, agentmemory.SourceAuthored)
	}
	return agentMutationInput{
		AgentID:  decoded.AgentID,
		EntityID: decoded.EntityID,
		Config:   decoded.Config,
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
			case "run_id", "flow_id", "flow_path", "flow_instance", "scope", "scope_key", "authority", "memory_plan":
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
