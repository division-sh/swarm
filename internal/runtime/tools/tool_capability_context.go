package tools

import (
	"strings"

	runtimeauthority "github.com/division-sh/swarm/internal/runtime/authority"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/toolcapabilities"
)

func capabilityForTool(actor models.AgentConfig, toolName string, requestAllowed map[string]struct{}, provider runtimeauthority.Provider, emitRegistry *EmitRegistry) toolcapabilities.Capability {
	decision := classifyToolAuthorization(actor, toolName, provider, emitRegistry)
	name := normalizeNativeToolName(toolName)
	cap := toolcapabilities.Capability{
		Name:               name,
		Kind:               toolKindPolicy(name),
		Visible:            decision.allowed,
		Callable:           decision.allowed,
		ContextRequirement: toolContextRequirementPolicy(name),
		StartupProbeMode:   toolStartupProbeModePolicy(name),
		AuthorizationClass: string(decision.class),
	}
	if len(requestAllowed) > 0 {
		if _, ok := requestAllowed[name]; !ok {
			cap.Visible = false
			cap.Callable = false
			cap.DenialReason = "request_not_allowed"
			return cap
		}
	}
	if !decision.allowed {
		cap.DenialReason = "tool_not_allowed"
	}
	return cap
}

func capabilitySetForActor(actor models.AgentConfig, names []string, requestAllowed map[string]struct{}) toolcapabilities.Set {
	return capabilitySetForActorWithDeps(actor, names, requestAllowed, runtimeauthority.NoopProvider(), nil)
}

func capabilitySetForActorWithDeps(actor models.AgentConfig, names []string, requestAllowed map[string]struct{}, provider runtimeauthority.Provider, emitRegistry *EmitRegistry) toolcapabilities.Set {
	caps := make([]toolcapabilities.Capability, 0, len(names))
	seen := map[string]struct{}{}
	for _, raw := range names {
		name := normalizeNativeToolName(strings.TrimSpace(raw))
		if name == "" {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		caps = append(caps, capabilityForTool(actor, name, requestAllowed, provider, emitRegistry))
	}
	return toolcapabilities.NewSet(caps)
}
