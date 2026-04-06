package tools

import (
	"fmt"
	"slices"
	"strings"

	runtimeauthority "swarm/internal/runtime/authority"
	models "swarm/internal/runtime/core/actors"
)

func authorizeDelegableAgentConfig(actor, current, proposed models.AgentConfig, provider runtimeauthority.Provider, emitRegistry *EmitRegistry) error {
	actor.NormalizeRuntimeDescriptor()
	current.NormalizeRuntimeDescriptor()
	proposed.NormalizeRuntimeDescriptor()

	for _, perm := range addedCanonicalValues(current.Permissions, proposed.Permissions, identityValue) {
		if agentHasPermission(actor, perm) {
			continue
		}
		return fmt.Errorf("delegated permission %q exceeds caller authority", perm)
	}

	for _, capability := range addedNativeToolCapabilities(current.NativeTools, proposed.NativeTools) {
		if actor.NativeTools.Enabled(capability) {
			continue
		}
		return fmt.Errorf("delegated native_tools.%s exceeds caller authority", capability)
	}

	for _, toolName := range addedCanonicalValues(current.Tools, proposed.Tools, normalizeNativeToolName) {
		if classifyToolAuthorization(actor, toolName, provider, emitRegistry).allowed {
			continue
		}
		return fmt.Errorf("delegated tool %q exceeds caller authority", toolName)
	}

	actorEmitEvents := effectiveDelegableEmitEvents(actor, provider)
	currentEmitEvents := effectiveDelegableEmitEvents(current, provider)
	proposedEmitEvents := effectiveDelegableEmitEvents(proposed, provider)
	for _, eventType := range addedCanonicalValues(currentEmitEvents, proposedEmitEvents, localEmitEventType) {
		if containsEquivalentEmitEvent(actorEmitEvents, eventType) {
			continue
		}
		return fmt.Errorf("delegated emit authority %q exceeds caller authority", eventType)
	}

	return nil
}

func mergeDelegablePrivilegeConfig(base, patch models.AgentConfig) models.AgentConfig {
	out := base
	if strings.TrimSpace(patch.Role) != "" {
		out.Role = strings.TrimSpace(patch.Role)
	}
	if len(patch.EmitEvents) > 0 {
		out.EmitEvents = append([]string(nil), patch.EmitEvents...)
	}
	if len(patch.Tools) > 0 {
		out.Tools = append([]string(nil), patch.Tools...)
	}
	if len(patch.Permissions) > 0 {
		out.Permissions = append([]string(nil), patch.Permissions...)
	}
	if patch.NativeTools.Any() {
		out.NativeTools = patch.NativeTools
	}
	out.NormalizeRuntimeDescriptor()
	return out
}

func effectiveDelegableEmitEvents(cfg models.AgentConfig, provider runtimeauthority.Provider) []string {
	if configured := UniqueNonEmpty(cfg.EmitEvents); len(configured) > 0 {
		return configured
	}
	return UniqueNonEmpty(runtimeauthority.ProviderOrNoop(provider).ProducerEventsForRole(cfg.Role))
}

func containsEquivalentEmitEvent(events []string, candidate string) bool {
	candidate = localEmitEventType(candidate)
	if candidate == "" {
		return false
	}
	for _, eventType := range events {
		if localEmitEventType(eventType) == candidate {
			return true
		}
	}
	return false
}

func addedNativeToolCapabilities(current, proposed models.NativeToolConfig) []string {
	out := make([]string, 0, 3)
	for _, capability := range []string{"bash", "web_search", "file_io"} {
		if !current.Enabled(capability) && proposed.Enabled(capability) {
			out = append(out, capability)
		}
	}
	return out
}

func addedCanonicalValues(current, proposed []string, canonicalize func(string) string) []string {
	currentSet := make(map[string]struct{}, len(current))
	for _, value := range current {
		value = canonicalize(value)
		if value != "" {
			currentSet[value] = struct{}{}
		}
	}
	added := make([]string, 0, len(proposed))
	for _, value := range proposed {
		value = canonicalize(value)
		if value == "" {
			continue
		}
		if _, ok := currentSet[value]; ok {
			continue
		}
		if slices.Contains(added, value) {
			continue
		}
		added = append(added, value)
	}
	return added
}

func identityValue(value string) string {
	return strings.TrimSpace(value)
}
