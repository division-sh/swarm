package tools

import (
	"strings"

	runtimeauthority "swarm/internal/runtime/authority"
	models "swarm/internal/runtime/core/actors"
	"swarm/internal/runtime/core/toolcapabilities"
	"swarm/internal/runtime/core/toolidentity"
	"swarm/internal/runtime/flowdata"
)

type toolAuthorizationDecision struct {
	ownership   toolOwnershipClass
	class       toolAuthorizationClass
	allowed     bool
	constrained bool
}

func classifyToolAuthorization(actor models.AgentConfig, toolName string, provider runtimeauthority.Provider, emitRegistry *EmitRegistry) toolAuthorizationDecision {
	toolName = normalizeNativeToolName(toolName)
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
	if toolEmitAllowed(actor, toolName, provider, emitRegistry) {
		decision.class = toolAuthorizationEmitAllowed
		decision.allowed = true
		return decision
	}
	if _, ok := nativeFallbackRegisteredTool(actor, toolName); ok {
		decision.class = toolAuthorizationNativeTool
		decision.allowed = true
		return decision
	}
	allowed, constrained := extractAllowedTools(actor)
	decision.constrained = constrained
	if _, ok := allowed[toolName]; ok {
		decision.class = toolAuthorizationActorConfig
		decision.allowed = true
		return decision
	}
	return decision
}

func toolEmitAllowed(actor models.AgentConfig, toolName string, provider runtimeauthority.Provider, emitRegistry *EmitRegistry) bool {
	if emitRegistry == nil {
		emitRegistry = NewEmitRegistry(nil, provider)
	}
	return emitRegistry.IsEmitToolAllowedForRole(actor.Role, toolName) || emitRegistry.IsEmitToolAllowedForActor(actor, toolName)
}

func toolKindPolicy(toolName string) toolcapabilities.ToolKind {
	if toolidentity.IsEmitToolName(toolName) {
		return toolcapabilities.KindEmit
	}
	return toolcapabilities.KindStandard
}

func toolContextRequirementPolicy(toolName string) toolcapabilities.ContextRequirement {
	switch normalizeNativeToolName(toolName) {
	case "get_entity", "query_entities", "search_entities", "query_metrics", "read_file", "web_search", flowdata.ToolName:
		return toolcapabilities.ContextRequirementActorContext
	default:
		return toolcapabilities.ContextRequirementTurnContext
	}
}

func toolStartupProbeModePolicy(toolName string) toolcapabilities.StartupProbeMode {
	return toolcapabilities.StartupProbeModeVisibilityOnly
}

func extractAllowedTools(actor models.AgentConfig) (map[string]struct{}, bool) {
	allowed := make(map[string]struct{})
	if len(actor.Tools) == 0 {
		return allowed, false
	}
	found := false
	for _, item := range actor.Tools {
		name := normalizeNativeToolName(item)
		if name == "" {
			continue
		}
		found = true
		allowed[name] = struct{}{}
	}
	return allowed, found
}
