package llm

import (
	"strings"

	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
)

func usageMetadataForProvider(profile llmselection.Profile, providerModel llmselection.ResolvedModel) map[string]any {
	meta := map[string]any{
		"backend_profile": profile.ID,
		"provider":        profile.Provider,
		"transport":       profile.Transport,
		"model_alias":     strings.TrimSpace(providerModel.ModelAlias),
		"resolved_model":  strings.TrimSpace(providerModel.ConcreteModel),
	}
	return meta
}
