package effectpersistence

import (
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
)

// This classifies only the original frozen provider surface. It does not
// authorize recovery, an origin retry, or a network invocation.
func completionResumeSurfaceAllowsHTTP(adapter string, request runtimeeffects.CompletionRequest, surface managedcapabilities.Surface) (bool, error) {
	var backend string
	switch adapter {
	case "anthropic_api":
		backend = llmselection.BackendAnthropic
	case "openai_compatible":
		backend = llmselection.BackendOpenAICompatible
	case "openai_responses":
		backend = llmselection.BackendOpenAIResponses
	default:
		return false, nil
	}
	if err := request.Validate(); err != nil {
		return false, fmt.Errorf("provider resume request identity: %w", err)
	}
	if request.BackendProfile != backend {
		return false, fmt.Errorf("provider resume adapter contradicts its frozen request")
	}
	profile, err := llmselection.ResolveActiveBackend(backend)
	if err != nil {
		return false, err
	}
	if err := surface.Validate(); err != nil {
		return false, fmt.Errorf("provider resume frozen surface: %w", err)
	}
	if surface.Authority.Kind != managedcapabilities.AuthorityProviderTurn || surface.Provider != profile.Provider ||
		surface.Transport != profile.Transport || surface.RuntimeMode != profile.RuntimeMode || surface.HasMismatch() {
		return false, fmt.Errorf("provider resume surface contradicts its frozen request")
	}
	if surface.Authority.ExecutionKind != managedcapabilities.ExecutionNormalAgent {
		return false, nil
	}
	for _, tool := range surface.Tools {
		if tool.Capability.AuthorizationClass == "provider_native" || len(tool.Bindings) == 0 {
			return false, nil
		}
		for _, binding := range tool.Bindings {
			if binding.Kind != managedcapabilities.BindingAPIDefinition {
				return false, nil
			}
		}
	}
	return true, nil
}
