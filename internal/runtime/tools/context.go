package tools

import (
	"context"

	"empireai/internal/models"
)

type actorContextKey struct{}

func WithActor(ctx context.Context, actor models.AgentConfig) context.Context {
	return context.WithValue(ctx, actorContextKey{}, actor)
}

func ActorFromContext(ctx context.Context) (models.AgentConfig, bool) {
	v := ctx.Value(actorContextKey{})
	if v == nil {
		return models.AgentConfig{}, false
	}
	cfg, ok := v.(models.AgentConfig)
	return cfg, ok
}
