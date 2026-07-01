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
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimesessions "github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/google/uuid"
)

func (e *Executor) execAgentMessage(ctx context.Context, actor models.AgentConfig, input any) (any, error) {
	if e.bus == nil {
		return nil, errors.New("event bus is not configured")
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
	evt := events.NewChildEventWithLineage(
		uuid.NewString(),
		events.EventType(in.EventType),
		in.SourceAgent,
		in.TaskID,
		wirePayload,
		0,
		events.EventLineage{
			RunID: runtimecorrelation.RunIDFromContext(ctx),
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
		return errors.New("agent ids are required for message authorization")
	}
	if strings.TrimSpace(actor.ID) == strings.TrimSpace(target.ID) {
		return nil
	}
	if hasRoleMessageAuthority(provider, actor, target) {
		return nil
	}
	return fmt.Errorf("role %s cannot message role %s", actor.Role, target.Role)
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
		return nil, errors.New("scheduler is not configured")
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
		return nil, errors.New("agents can only schedule for themselves")
	}
	entityID := strings.TrimSpace(in.EntityID)
	in.EntityID = entityID
	if entityID == "" {
		entityID = actor.EffectiveEntityID()
	}
	actorEntityID := actor.EffectiveEntityID()
	if entityID != "" && actorEntityID != "" && entityID != actorEntityID {
		return nil, errors.New("cross-entity schedule is not allowed")
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

func (e *Executor) execAgentHire(actor models.AgentConfig, input any) (any, error) {
	manager := e.getManager()
	if manager == nil {
		return nil, errors.New("agent manager is not configured")
	}
	in, err := decodeAgentMutationInput("agent_hire", input, true)
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
	if in.Config.Mode == "" {
		in.Config.Mode = coalesce(actor.Mode, "entity")
	}
	if _, err := runtimesessions.ValidateAgentSessionScopeConfig(in.Config); err != nil {
		return nil, fmt.Errorf("invalid agent session scope: %w", err)
	}
	if err := authorizeManage(e.authority, actor, in.Config, manager); err != nil {
		return nil, err
	}
	if err := authorizeDelegableAgentConfig(actor, models.AgentConfig{}, in.Config, e.authority, e.emitRegistry); err != nil {
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

func (e *Executor) execAgentReconfigure(actor models.AgentConfig, input any) (any, error) {
	manager := e.getManager()
	if manager == nil {
		return nil, errors.New("agent manager is not configured")
	}
	in, err := decodeAgentMutationInput("agent_reconfigure", input, false)
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
	sessionCfg := mergeAgentSessionScopeConfig(targetCfg, in.Config)
	if _, err := runtimesessions.ValidateAgentSessionScopeConfig(sessionCfg); err != nil {
		return nil, fmt.Errorf("invalid agent session scope: %w", err)
	}
	updatedCfg := mergeDelegablePrivilegeConfig(targetCfg, in.Config)
	if err := authorizeDelegableAgentConfig(actor, targetCfg, updatedCfg, e.authority, e.emitRegistry); err != nil {
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

func decodeAgentMutationInput(toolName string, input any, requireMode bool) (agentMutationInput, error) {
	normalized := canonicalRuntimeToolInput(toolName, input)
	var payload map[string]any
	if err := decodeToolInput(normalized, &payload); err != nil {
		return agentMutationInput{}, err
	}
	if payload == nil {
		payload = map[string]any{}
	}
	if err := rejectAgentMemoryModeForgery(payload, "input", true); err != nil {
		return agentMutationInput{}, err
	}

	configMap, err := agentMutationConfigMap(payload["config"])
	if err != nil {
		return agentMutationInput{}, err
	}
	topMode := strings.TrimSpace(asString(payload["mode"]))
	configMode := strings.TrimSpace(asString(configMap["mode"]))
	switch {
	case topMode != "" && configMode != "" && topMode != configMode:
		return agentMutationInput{}, fmt.Errorf("mode mismatch between input.mode and input.config.mode")
	case configMode != "":
		topMode = configMode
	}
	if requireMode && topMode == "" {
		return agentMutationInput{}, fmt.Errorf("config.mode is required")
	}
	delete(configMap, "mode")
	payload["config"] = configMap

	var decoded struct {
		AgentID  string             `json:"agent_id"`
		EntityID string             `json:"entity_id"`
		Config   models.AgentConfig `json:"config"`
	}
	if err := decodeToolInput(payload, &decoded); err != nil {
		return agentMutationInput{}, err
	}
	if topMode != "" {
		mode, scope, err := runtimesessions.ResolveAuthoredAgentMemoryMode(topMode)
		if err != nil {
			return agentMutationInput{}, fmt.Errorf("invalid mode: %w", err)
		}
		decoded.Config.ConversationMode = mode.String()
		decoded.Config.SessionScope = scope.String()
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

func rejectAgentMemoryModeForgery(value any, path string, allowMode bool) error {
	switch typed := value.(type) {
	case map[string]any:
		for key, item := range typed {
			field := strings.TrimSpace(key)
			if field == "" {
				continue
			}
			fieldPath := path + "." + field
			switch field {
			case "conversation_mode":
				return fmt.Errorf("%s is retired; use mode", fieldPath)
			case "session_scope":
				return fmt.Errorf("%s is runtime-derived from mode", fieldPath)
			case "session_scope_authority":
				return fmt.Errorf("%s is platform-internal runtime state", fieldPath)
			case "mode":
				if !allowMode {
					return fmt.Errorf("%s is only supported as the agent memory mode field", fieldPath)
				}
			}
			childAllowMode := false
			if path == "input" && field == "config" {
				childAllowMode = true
			}
			if err := rejectAgentMemoryModeForgery(item, fieldPath, childAllowMode); err != nil {
				return err
			}
		}
	case []any:
		for i, item := range typed {
			if err := rejectAgentMemoryModeForgery(item, fmt.Sprintf("%s[%d]", path, i), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func mergeAgentSessionScopeConfig(base, patch models.AgentConfig) models.AgentConfig {
	out := base
	if strings.TrimSpace(patch.ConversationMode) != "" {
		out.ConversationMode = strings.TrimSpace(patch.ConversationMode)
		if out.ConversationMode == runtimesessions.RuntimeModeTask.String() {
			out.SessionScope = ""
		}
	}
	if strings.TrimSpace(patch.SessionScope) != "" {
		out.SessionScope = strings.TrimSpace(patch.SessionScope)
	}
	if strings.TrimSpace(patch.FlowPath) != "" {
		out.FlowPath = strings.TrimSpace(patch.FlowPath)
	}
	if strings.TrimSpace(patch.EntityID) != "" {
		out.EntityID = strings.TrimSpace(patch.EntityID)
	}
	out.NormalizeRuntimeDescriptor()
	return out
}
