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
	}
	meta["resolved_model"] = concrete
	return meta
}
