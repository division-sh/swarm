package actorctx

import (
	"context"

	"empireai/internal/models"
)

type key struct{}

func WithActor(ctx context.Context, actor models.AgentConfig) context.Context {
	return context.WithValue(ctx, key{}, actor)
}

func ActorFromContext(ctx context.Context) (models.AgentConfig, bool) {
	v := ctx.Value(key{})
	if v == nil {
		return models.AgentConfig{}, false
	}
	cfg, ok := v.(models.AgentConfig)
	return cfg, ok
}
