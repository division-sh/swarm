package llm

import (
	"context"
	"strings"

	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
)

func usageMetadataForContext(ctx context.Context, profile llmselection.Profile, fallbackConcreteModel string) map[string]any {
	concrete := strings.TrimSpace(fallbackConcreteModel)
	meta := map[string]any{
		"backend_profile": profile.ID,
		"provider":        profile.Provider,
		"transport":       profile.Transport,
	}
	if actor, ok := runtimeactors.ActorFromContext(ctx); ok {
		meta["model_alias"] = strings.TrimSpace(actor.Model)
		if strings.TrimSpace(actor.LLMBackend) != "" {
			meta["backend_profile"] = strings.TrimSpace(actor.LLMBackend)
		}
		if strings.TrimSpace(actor.ResolvedLLMProvider) != "" {
			meta["provider"] = strings.TrimSpace(actor.ResolvedLLMProvider)
		}
		if strings.TrimSpace(actor.ResolvedLLMTransport) != "" {
			meta["transport"] = strings.TrimSpace(actor.ResolvedLLMTransport)
		}
		if strings.TrimSpace(actor.ResolvedModel) != "" {
			concrete = strings.TrimSpace(actor.ResolvedModel)
		}
	}
	meta["resolved_model"] = concrete
	return meta
}
