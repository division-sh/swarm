package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	runtimeauthority "swarm/internal/runtime/authority"
	models "swarm/internal/runtime/core/actors"
)

type ToolAuthorizer struct {
	bus      EventPublisher
	classify func(models.AgentConfig, string) toolAuthorizationDecision
}

type toolOwnershipClass string

const (
	toolOwnershipPlatformBuiltin    toolOwnershipClass = "platform_builtin"
	toolOwnershipWorkflowRegistered toolOwnershipClass = "workflow_registered"
)

type toolAuthorizationClass string

const (
	toolAuthorizationUniversal   toolAuthorizationClass = "universal"
	toolAuthorizationPermission  toolAuthorizationClass = "permission"
	toolAuthorizationEmitAllowed toolAuthorizationClass = "emit_allowed"
	toolAuthorizationNativeTool  toolAuthorizationClass = "native_tool"
	toolAuthorizationRoleScoped  toolAuthorizationClass = "role_scoped_entity_tool"
	toolAuthorizationActorConfig toolAuthorizationClass = "actor_config"
	toolAuthorizationDenied      toolAuthorizationClass = "denied"
)

func NewToolAuthorizer(bus EventPublisher, classify func(models.AgentConfig, string) toolAuthorizationDecision) *ToolAuthorizer {
	if classify == nil {
		classify = func(actor models.AgentConfig, toolName string) toolAuthorizationDecision {
			return classifyToolAuthorization(actor, toolName, runtimeauthority.NoopProvider(), nil)
		}
	}
	return &ToolAuthorizer{bus: bus, classify: classify}
}

func (a *ToolAuthorizer) Authorize(ctx context.Context, actor models.AgentConfig, toolName string) error {
	_ = ctx
	decision := a.classify(actor, toolName)
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
			processWarn(
				"tool-executor",
				"tool authorization denied actor=%s tool=%s entity=%s detail=%s",
				strings.TrimSpace(actor.ID),
				strings.TrimSpace(toolName),
				entityID,
				strings.TrimSpace(string(payload)),
			)
		}
	}
	return err
}

func toolOwnershipForName(toolName string) toolOwnershipClass {
	if IsUniversal(toolName) {
		return toolOwnershipPlatformBuiltin
	}
	return toolOwnershipWorkflowRegistered
}
