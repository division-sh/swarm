package tools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"swarm/internal/events"
	runtimeauthority "swarm/internal/runtime/authority"
	models "swarm/internal/runtime/core/actors"
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
	actorEntityID := actor.EffectiveEntityID()
	for _, targetID := range targets {
		targetCfg, ok := manager.GetAgentConfig(targetID)
		if !ok {
			return nil, fmt.Errorf("target agent not found: %s", targetID)
		}
		targetCfgEntityID := targetCfg.EffectiveEntityID()
		if actorEntityID != "" && targetCfgEntityID != actorEntityID {
			return nil, errors.New("cross-entity agent_message is not allowed")
		}
		if targetEntity == "" {
			targetEntity = targetCfgEntityID
		}
		if targetArg := strings.TrimSpace(in.EntityID); targetArg != "" && targetCfgEntityID != targetArg {
			return nil, errors.New("entity_id does not match target agent entity")
		}
		if err := authorizeAgentMessage(actor, targetCfg, manager); err != nil {
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
	evt := (events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType(in.EventType),
		SourceAgent: in.SourceAgent,
		TaskID:      in.TaskID,
		Payload:     wirePayload,
		CreatedAt:   time.Now(),
	}).WithEntityID(targetEntity)
	if err := e.bus.PublishDirect(ctx, evt, targets); err != nil {
		return nil, err
	}
	return map[string]any{"event_id": evt.ID, "status": "sent", "targets": targets}, nil
}

func authorizeAgentMessage(actor, target models.AgentConfig, manager Manager) error {
	if strings.TrimSpace(actor.ID) == "" || strings.TrimSpace(target.ID) == "" {
		return errors.New("agent ids are required for message authorization")
	}
	if strings.TrimSpace(actor.ID) == strings.TrimSpace(target.ID) {
		return nil
	}
	if hasRoleMessageAuthority(actor, target) {
		return nil
	}
	return fmt.Errorf("role %s cannot message role %s", actor.Role, target.Role)
}

func hasRoleMessageAuthority(actor, target models.AgentConfig) bool {
	return runtimeauthority.Active().HasMessageAuthority(actor, target)
}

func isManagerAncestor(manager Manager, managerID, targetID string) bool {
	managerID = strings.TrimSpace(managerID)
	targetID = strings.TrimSpace(targetID)
	if manager == nil || managerID == "" || targetID == "" || managerID == targetID {
		return false
	}
	currentID := targetID
	visited := map[string]struct{}{currentID: {}}
	for {
		cfg, ok := manager.GetAgentConfig(currentID)
		if !ok {
			return false
		}
		parent := strings.TrimSpace(cfg.ParentAgent)
		if parent == "" {
			return false
		}
		if parent == managerID {
			return true
		}
		if _, seen := visited[parent]; seen {
			return false
		}
		visited[parent] = struct{}{}
		currentID = parent
	}
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
		AgentID:   in.AgentID,
		EventType: in.EventType,
		Mode:      in.Mode,
		Cron:      in.Cron,
		At:        at,
		EntityID:  entityID,
		TaskID:    in.TaskID,
		Payload:   payload,
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
	return nil, errors.New("configure_routing is not part of the MAS runtime; routes derive from contracts")
}

func (e *Executor) execAgentHire(actor models.AgentConfig, input any) (any, error) {
	manager := e.getManager()
	if manager == nil {
		return nil, errors.New("agent manager is not configured")
	}
	var in struct {
		EntityID string             `json:"entity_id"`
		Config   models.AgentConfig `json:"config"`
	}
	if err := decodeToolInput(input, &in); err != nil {
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
	if err := authorizeManage(actor, in.Config, manager); err != nil {
		return nil, err
	}
	if err := manager.SpawnAgentForEntity(in.Config.EffectiveEntityID(), in.Config); err != nil {
		return nil, err
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
	if err := authorizeManage(actor, targetCfg, manager); err != nil {
		return nil, err
	}
	if err := manager.TeardownAgent(in.AgentID); err != nil {
		return nil, err
	}
	return map[string]any{"status": "fired", "agent_id": in.AgentID}, nil
}

func (e *Executor) execAgentReconfigure(actor models.AgentConfig, input any) (any, error) {
	manager := e.getManager()
	if manager == nil {
		return nil, errors.New("agent manager is not configured")
	}
	var in struct {
		AgentID string             `json:"agent_id"`
		Config  models.AgentConfig `json:"config"`
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
	if err := authorizeManage(actor, targetCfg, manager); err != nil {
		return nil, err
	}
	if err := manager.ReconfigureAgent(in.AgentID, in.Config); err != nil {
		return nil, err
	}
	return map[string]any{"status": "reconfigured", "agent_id": in.AgentID}, nil
}
