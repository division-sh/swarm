package tools

import (
	"encoding/json"
	"strings"

	models "swarm/internal/runtime/core/actors"
	"swarm/internal/runtime/core/toolcapabilities"
	"swarm/internal/runtime/core/toolidentity"
)

type toolAuthorizationDecision struct {
	ownership   toolOwnershipClass
	class       toolAuthorizationClass
	allowed     bool
	constrained bool
}

func classifyToolAuthorization(actor models.AgentConfig, toolName string) toolAuthorizationDecision {
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
	if toolEmitAllowed(actor, toolName) {
		decision.class = toolAuthorizationEmitAllowed
		decision.allowed = true
		return decision
	}
	if _, ok := nativeFallbackRegisteredTool(actor, toolName); ok {
		decision.class = toolAuthorizationNativeTool
		decision.allowed = true
		return decision
	}
	allowed, constrained := extractAllowedToolsFromConfig(actor)
	decision.constrained = constrained
	if _, ok := allowed[toolName]; ok {
		decision.class = toolAuthorizationActorConfig
		decision.allowed = true
		return decision
	}
	return decision
}

func toolEmitAllowed(actor models.AgentConfig, toolName string) bool {
	return IsEmitToolAllowedForRole(actor.Role, toolName) || IsEmitToolAllowedForConfig(actor.Config, toolName)
}

func toolKindPolicy(toolName string) toolcapabilities.ToolKind {
	if toolidentity.IsEmitToolName(toolName) {
		return toolcapabilities.KindEmit
	}
	return toolcapabilities.KindStandard
}

func toolContextRequirementPolicy(toolName string) toolcapabilities.ContextRequirement {
	switch normalizeNativeToolName(toolName) {
	case "get_entity", "query_entities", "search_entities", "query_metrics", "read_file", "web_search":
		return toolcapabilities.ContextRequirementActorContext
	default:
		return toolcapabilities.ContextRequirementTurnContext
	}
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
	raw, ok := parsed["tools"]
	if !ok {
		return allowed, false
	}
	arr, ok := raw.([]any)
	if !ok {
		return allowed, false
	}
	for _, item := range arr {
		name := normalizeNativeToolName(asString(item))
		if name == "" {
			continue
		}
		found = true
		allowed[name] = struct{}{}
	}
	return allowed, found
}
