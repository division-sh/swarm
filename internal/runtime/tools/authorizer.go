package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"empireai/internal/events"
	models "empireai/internal/runtime/core/actors"
	"github.com/google/uuid"
)

type ToolAuthorizer struct {
	bus EventPublisher
}

type toolOwnershipClass string

const (
	toolOwnershipPlatformBuiltin    toolOwnershipClass = "platform_builtin"
	toolOwnershipWorkflowRegistered toolOwnershipClass = "workflow_registered"
)

type toolAuthorizationClass string

const (
	toolAuthorizationUniversal    toolAuthorizationClass = "universal"
	toolAuthorizationPermission   toolAuthorizationClass = "permission"
	toolAuthorizationEmitAllowed  toolAuthorizationClass = "emit_allowed"
	toolAuthorizationActorConfig  toolAuthorizationClass = "actor_config"
	toolAuthorizationDefaultAllow toolAuthorizationClass = "default_allow"
	toolAuthorizationDenied       toolAuthorizationClass = "denied"
)

type toolAuthorizationDecision struct {
	ownership   toolOwnershipClass
	class       toolAuthorizationClass
	allowed     bool
	constrained bool
}

func NewToolAuthorizer(bus EventPublisher) *ToolAuthorizer {
	return &ToolAuthorizer{bus: bus}
}

func (a *ToolAuthorizer) Authorize(ctx context.Context, actor models.AgentConfig, toolName string) error {
	decision := classifyToolAuthorization(actor, toolName)
	if decision.allowed {
		return nil
	}
	err := fmt.Errorf("%w: tool %s is not allowed for agent %s", ErrToolNotAllowed, toolName, actor.ID)
	if a.bus != nil {
		entityID := actor.EffectiveEntityID()
		payload, marshalErr := json.Marshal(map[string]any{
			"reason":       "tool_not_allowed",
			"agent_id":     actor.ID,
			"agent_role":   actor.Role,
			"tool_name":    toolName,
			"entity_id":    entityID,
			"runtime_tool": true,
		})
		if marshalErr == nil {
			if pubErr := a.bus.Publish(ctx, (events.Event{
				ID:          uuid.NewString(),
				Type:        events.EventType("spec.contradiction_detected"),
				SourceAgent: "runtime",
				Payload:     payload,
				CreatedAt:   time.Now(),
			}).WithEntityID(entityID)); pubErr != nil {
				runtimeWarn(
					"tool-executor",
					"failed to publish spec.contradiction_detected actor=%s tool=%s: %v",
					strings.TrimSpace(actor.ID),
					strings.TrimSpace(toolName),
					pubErr,
				)
			}
		}
	}
	return err
}

func classifyToolAuthorization(actor models.AgentConfig, toolName string) toolAuthorizationDecision {
	decision := toolAuthorizationDecision{
		ownership: toolOwnershipForName(toolName),
		class:     toolAuthorizationDenied,
	}
	if IsUniversal(toolName) {
		decision.class = toolAuthorizationUniversal
		decision.allowed = true
		return decision
	}
	if requiredPerm, ok := toolPermissionRequirements[strings.TrimSpace(toolName)]; ok {
		decision.class = toolAuthorizationPermission
		if agentHasPermission(actor, requiredPerm) {
			decision.allowed = true
		}
		return decision
	}
	if IsEmitToolAllowedForRole(actor.Role, toolName) || IsEmitToolAllowedForConfig(actor.Config, toolName) {
		decision.class = toolAuthorizationEmitAllowed
		decision.allowed = true
		return decision
	}
	allowed, constrained := extractAllowedToolsFromConfig(actor)
	if !constrained {
		decision.class = toolAuthorizationDefaultAllow
		decision.allowed = true
		return decision
	}
	decision.constrained = true
	if _, ok := allowed[toolName]; ok {
		decision.class = toolAuthorizationActorConfig
		decision.allowed = true
		return decision
	}
	return decision
}

func toolOwnershipForName(toolName string) toolOwnershipClass {
	if IsUniversal(toolName) {
		return toolOwnershipPlatformBuiltin
	}
	return toolOwnershipWorkflowRegistered
}

func extractAllowedToolsFromConfig(actor models.AgentConfig) (map[string]struct{}, bool) {
	allowed := make(map[string]struct{})
	if len(actor.Config) == 0 || !json.Valid(actor.Config) {
		return allowed, false
	}
	var parsed map[string]any
	if err := json.Unmarshal(actor.Config, &parsed); err != nil {
		return allowed, false
	}
	found := false
	for _, key := range []string{"tools", "allowed_tools"} {
		raw, ok := parsed[key]
		if !ok {
			continue
		}
		arr, ok := raw.([]any)
		if !ok {
			continue
		}
		for _, item := range arr {
			name := strings.TrimSpace(asString(item))
			if name == "" {
				continue
			}
			found = true
			allowed[name] = struct{}{}
		}
	}
	return allowed, found
}
